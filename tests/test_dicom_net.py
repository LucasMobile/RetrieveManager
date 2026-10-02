import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

from pydicom.dataset import Dataset
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from app.dicom_net import (
    FindResult,
    PacsNode,
    echo,
    find_prior_series,
    find_study,
    find_study_series,
    move,
    move_series,
    move_study,
    prior_series_query,
)
from app.models import DicomInstance, ModalityRule, Order, OrderEvent, Unit
from app.parse import study_response
from app.pipeline.monitor import check_monitoring
from app.pipeline.move import _run_move, claim_due_moves, run_claimed_move
from app.receiver import Receiver
from tests.fake_pacs import FakePacs, free_port
from tests.support import make_unit, postgres_test_engine
from tests.test_receiver import ct_image

STUDY = "1.2.826.0.1.3680043.10.1"
PRIOR = "1.2.826.0.1.3680043.10.2"


def study_images(study_uid=STUDY, count=3, series_uid=None, **values):
    series_uid = series_uid or f"{study_uid}.1"
    return [
        ct_image(
            study_uid=study_uid,
            SeriesInstanceUID=series_uid,
            AccessionNumber=values.get("accession", "ACC-1"),
            PatientBirthDate="19691027",
            BodyPartExamined="ABDOMEN",
            StudyDate=values.get("study_date", "20260920"),
        )
        for _ in range(count)
    ]


class PacsReceiverFixture:
    """A pynetdicom PACS moving into the real receiver and its database."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.receive_dir = root / "receive"
        self.engine = postgres_test_engine(self)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.receiver_port = free_port()
        self.unit = make_unit(
            calling_aet="RETRIEVE",
            store_port=self.receiver_port,
            receive_dir=str(self.receive_dir),
            error_dir=str(root / "error"),
        )
        with self.Session() as db:
            db.add(self.unit)
            db.commit()
        self.receiver = Receiver(self.Session, host="127.0.0.1")
        self.receiver.start()
        self.receiver.reconcile([self.unit])
        images = study_images() + study_images(
            PRIOR, count=2, accession="OLD-1", study_date="20250110"
        )
        self.pacs = FakePacs(
            images,
            destinations={"RETRIEVE": ("127.0.0.1", self.receiver_port)},
        ).start()
        self.node = PacsNode("127.0.0.1", self.pacs.port, "PACS", "RETRIEVE")
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        self.pacs.stop()
        self.receiver.stop()
        self.engine.dispose()
        self.tmp.cleanup()

    def stored(self) -> int:
        with self.Session() as db:
            return db.scalar(select(func.count()).select_from(DicomInstance))


class DicomNetworkTest(PacsReceiverFixture, unittest.TestCase):
    """Real associations against a pynetdicom PACS and the real receiver."""

    def test_echo(self):
        self.assertTrue(echo(self.node, timeout=5).ok)
        closed = echo(PacsNode("127.0.0.1", free_port(), "PACS", "RETRIEVE"), timeout=3)
        self.assertFalse(closed.ok)
        self.assertIn("conexão", closed.error)

    def test_find_study_returns_identity_as_datasets(self):
        result = find_study(self.node, "ACC-1", "19691027", timeout=10)

        self.assertTrue(result.ok)
        self.assertEqual(result.status, 0x0000)
        (response,) = [study_response(ds) for ds in result.responses]
        self.assertEqual(response.study_uid, STUDY)
        self.assertEqual(response.patient_id, "30211738")
        self.assertEqual(response.birth_date, "19691027")
        self.assertEqual(response.modalities, "CT")
        self.assertEqual(response.instance_count, "3")
        self.assertIn("1 resposta(s)", result.summary)

    def test_find_without_match_is_ok_and_empty(self):
        result = find_study(self.node, "NAO-EXISTE", "19691027", timeout=10)
        self.assertTrue(result.ok)
        self.assertEqual(result.responses, ())

    def test_find_rejected_association_is_reported(self):
        wrong_pacs_aet = PacsNode("127.0.0.1", self.pacs.port, "OUTRO", "RETRIEVE")
        result = find_study(wrong_pacs_aet, "ACC-1", "19691027", timeout=10)
        self.assertFalse(result.ok)
        self.assertIn("recusada", result.error)

    def test_series_and_prior_queries(self):
        series = find_study_series(self.node, STUDY, timeout=10)
        self.assertTrue(series.ok)
        self.assertEqual(len(series.responses), 1)
        self.assertEqual(series.responses[0].Modality, "CT")

        priors = find_prior_series(
            self.node,
            timeout=10,
            body_part="ABDOMEN",
            modality="CT",
            patient_id="30211738",
            birth_date="19691027",
            date_range="20230101-20260919",
        )
        self.assertTrue(priors.ok)
        self.assertEqual({ds.StudyInstanceUID for ds in priors.responses}, {PRIOR})

    def test_prior_query_patient_id_is_exact_unless_wildcard(self):
        exact = prior_series_query(
            body_part="",
            modality="CT",
            patient_id="123",
            birth_date="",
            date_range="",
        )
        wildcard = prior_series_query(
            body_part="",
            modality="CT",
            patient_id="123",
            birth_date="",
            date_range="",
            patient_id_wildcard=True,
        )
        self.assertEqual(exact.PatientID, "123")
        self.assertEqual(wildcard.PatientID, "123*")

    def test_move_study_delivers_images_to_the_receiver(self):
        result = move_study(self.node, STUDY, timeout=30)

        self.assertTrue(result.ok, result.summary)
        self.assertEqual(result.status, 0x0000)
        self.assertEqual((result.completed, result.failed), (3, 0))
        self.assertEqual(self.pacs.moves, ["RETRIEVE"])
        self.assertEqual(self.stored(), 3)

    def test_move_series_retrieves_only_that_series(self):
        result = move_series(self.node, PRIOR, f"{PRIOR}.1", timeout=30)
        self.assertTrue(result.ok, result.summary)
        self.assertEqual(result.completed, 2)
        self.assertEqual(self.stored(), 2)

    def test_partial_failure_is_not_a_success(self):
        # The receiver refuses an object without StudyInstanceUID (0xC000).
        broken = ct_image(study_uid=None, SeriesInstanceUID=f"{STUDY}.1")
        self.pacs.images.append(broken)
        broken.StudyInstanceUID = STUDY  # matched by the PACS ...
        original_select = self.pacs._select

        def select_and_strip(query):
            matches = original_select(query)
            stripped = []
            for image in matches:
                if image is broken:
                    image = image.copy()
                    del image.StudyInstanceUID  # ... but sent without it
                stripped.append(image)
            return stripped

        self.pacs._select = select_and_strip
        result = move_study(self.node, STUDY, timeout=30)

        self.assertFalse(result.ok)
        self.assertEqual((result.completed, result.failed), (3, 1))
        self.assertIn("parte das imagens", result.error)

    def test_unknown_move_destination(self):
        other = PacsNode("127.0.0.1", self.pacs.port, "PACS", "DESCONHECIDO")
        result = move_study(other, STUDY, timeout=30)
        self.assertFalse(result.ok)
        self.assertEqual(result.status, 0xA801)
        self.assertIn("não conhece o AE de destino", result.error)

    def test_silent_pacs_times_out(self):
        self.pacs.move_delay = 3
        result = move_study(self.node, STUDY, timeout=1)
        self.assertFalse(result.ok)
        self.assertTrue(result.error)
        self.assertEqual(self.stored(), 0)


def move_status(code, **counts):
    status = Dataset()
    status.Status = code
    for keyword, value in counts.items():
        setattr(status, f"NumberOf{keyword}Suboperations", value)
    return status


class MonitoringTest(PacsReceiverFixture, unittest.TestCase):
    """After the 1st retrieve, each check fetches only what the PACS gained."""

    def setUp(self):
        super().setUp()
        now = datetime.now()
        with self.Session() as db:
            unit = db.get(Unit, self.unit.id)
            unit.pacs_port = self.pacs.port
            order = Order(
                unit_id=unit.id,
                acc="ACC-1",
                birth_date="19691027",
                study_uid=STUDY,
                modality="CT",
                status="monitoring",
                prior_status="disabled",
                monitor_interval_minutes=5,
                monitor_next_at=now,
                monitor_until=now + timedelta(hours=6),
            )
            db.add(order)
            db.commit()
            self.order_id = order.id
        self.assertTrue(move_study(self.node, STUDY, timeout=30).ok)
        self.moves_before = len(self.pacs.moves)

    def run_check(self):
        """One monitoring check plus the update move it may have queued."""
        with self.Session() as db:
            unit = db.get(Unit, self.unit.id)
            self.assertEqual(check_monitoring(db, unit), 1)
            for resource_id, kind in claim_due_moves(db, unit):
                run_claimed_move(db, resource_id, kind)
        with self.Session() as db:
            order = db.get(Order, self.order_id)
            events = list(
                db.scalars(
                    select(OrderEvent.message)
                    .where(OrderEvent.order_id == self.order_id)
                    .order_by(OrderEvent.id)
                )
            )
            return order, events

    def test_nothing_new_keeps_monitoring_and_logs_the_check(self):
        order, events = self.run_check()
        self.assertEqual(order.status, "monitoring")
        self.assertEqual(order.monitor_checks, 1)
        self.assertEqual(len(self.pacs.moves), self.moves_before)
        self.assertAlmostEqual(
            (order.monitor_next_at - datetime.now()).total_seconds() / 60, 5, delta=0.2
        )
        self.assertTrue(events[0].startswith("Verificação 1: nenhuma imagem nova"))
        self.assertIn("PACS: 3 imagem(ns) em 1 série(s); recebidas: 3.", events[0])

    def test_only_series_with_new_images_are_moved(self):
        self.pacs.images.extend(study_images(count=2, series_uid=f"{STUDY}.2"))
        order, events = self.run_check()
        self.assertEqual(order.status, "monitoring")
        self.assertEqual(len(self.pacs.moves), self.moves_before + 1)
        self.assertEqual(self.stored(), 5)
        self.assertEqual(order.monitor_new_images, 2)
        self.assertIn("Imagens novas em 1 série(s)", events[0])
        self.assertTrue(events[1].startswith("C-MOVE de novas imagens concluído"))

        # The next check sees everything received.
        with self.Session() as db:
            db.get(Order, self.order_id).monitor_next_at = datetime.now()
            db.commit()
        order, events = self.run_check()
        self.assertEqual(len(self.pacs.moves), self.moves_before + 1)
        self.assertTrue(events[-1].startswith("Verificação 2: nenhuma imagem nova"))

    def test_images_failed_after_reception_are_not_requested_again(self):
        with self.Session() as db:
            for state, row in zip(
                ("error", "missing"),
                db.scalars(select(DicomInstance).limit(2)),
                strict=True,
            ):
                row.state = state
            db.commit()
        order, _events = self.run_check()
        self.assertEqual(order.status, "monitoring")
        self.assertEqual(len(self.pacs.moves), self.moves_before)

    def test_without_series_counts_the_whole_study_is_moved(self):
        response = Dataset()
        response.SeriesInstanceUID = f"{STUDY}.1"
        with mock.patch(
            "app.pipeline.monitor.find_study_series",
            return_value=FindResult(True, (response,), 0x0000),
        ):
            order, events = self.run_check()
        self.assertEqual(order.status, "monitoring")
        self.assertEqual(len(self.pacs.moves), self.moves_before + 1)
        self.assertIn("estudo completo solicitado", events[0])

    def test_failed_find_is_retried_without_error(self):
        with mock.patch(
            "app.pipeline.monitor.find_study_series",
            return_value=FindResult(False, error="tempo esgotado"),
        ):
            order, events = self.run_check()
        self.assertEqual(order.status, "monitoring")
        self.assertEqual(order.last_error, "")
        self.assertTrue(events[0].startswith("Verificação 1: C-FIND falhou"))

    def test_check_at_window_end_closes_the_order(self):
        with self.Session() as db:
            db.get(Order, self.order_id).monitor_until = datetime.now()
            db.commit()
        order, events = self.run_check()
        self.assertEqual(order.status, "done")
        self.assertIsNotNone(order.done_at)
        self.assertIsNone(order.monitor_next_at)
        self.assertTrue(events[-1].startswith("Verificação 1: nenhuma imagem nova"))
        self.assertIn(
            "Monitoramento de novas imagens encerrado: 1 verificação(ões)", events[-1]
        )

    def test_stopped_order_is_left_alone_by_an_inflight_check(self):
        def stop_meanwhile(*_args, **_kwargs):
            with self.Session() as other:
                other.get(Order, self.order_id).status = "cancelled"
                other.commit()
            return FindResult(True, (), 0x0000)

        with mock.patch(
            "app.pipeline.monitor.find_study_series", side_effect=stop_meanwhile
        ):
            order, events = self.run_check()
        self.assertEqual(order.status, "cancelled")
        self.assertEqual(order.monitor_checks, 0)
        self.assertEqual(events, [])

    def test_first_move_opens_the_window_only_for_monitored_modalities(self):
        with self.Session() as db:
            db.add(ModalityRule(modality="CT", wait_minutes=10, monitor_enabled=True))
            order = db.get(Order, self.order_id)
            order.status = "retrieving"
            order.monitor_until = None
            db.commit()
            _run_move(db, db.get(Unit, order.unit_id), order)
            self.assertEqual(order.status, "monitoring")
            self.assertAlmostEqual(
                (order.monitor_until - datetime.now()).total_seconds() / 3600,
                6,
                delta=0.01,
            )

            order.status = "retrieving"
            order.modality = "DX"
            db.commit()
            _run_move(db, db.get(Unit, order.unit_id), order)
            self.assertEqual(order.status, "done")


class MoveCountsTest(unittest.TestCase):
    def test_final_response_settles_remaining(self):
        # Some PACS report Remaining only on the first pending response.
        assoc = mock.Mock(is_established=True)
        assoc.send_c_move.return_value = iter(
            [
                (move_status(0xFF00, Remaining=392, Completed=0), None),
                (move_status(0xB000, Completed=388, Failed=4), None),
            ]
        )
        node = PacsNode("127.0.0.1", 104, "PACS", "RETRIEVE")
        with mock.patch("app.dicom_net._associate", return_value=(assoc, "")):
            result = move(node, Dataset(), destination_aet="RETRIEVE", timeout=5)

        self.assertFalse(result.ok)
        self.assertEqual((result.completed, result.failed), (388, 4))
        self.assertEqual(result.remaining, 0)
        self.assertNotIn("pendentes", result.summary)


if __name__ == "__main__":
    unittest.main()
