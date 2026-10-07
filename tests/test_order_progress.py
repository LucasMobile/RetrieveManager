import re
from uuid import uuid4

from app.models import ImageTransfer, Order
from tests.support import make_unit
from tests.web_support import AdminWebTestCase

_STEP = re.compile(r'<div(?: class="([^"]*)")?[^>]*><span>(0\d) · ')


class OrderProgressStepsTest(AdminWebTestCase):
    """Steps done before and running again are highlighted as current."""

    def steps(self, status, **transfers):
        with self.Session() as db:
            unit = make_unit(name=f"Unidade {uuid4().hex[:6]}")
            db.add(unit)
            db.flush()
            order = Order(
                unit_id=unit.id,
                acc="202601055",
                birth_date="20110713",
                status=status,
                study_uid="1.2.3",
            )
            db.add(order)
            db.flush()
            for transfer_status, amount in transfers.items():
                db.add_all(
                    ImageTransfer(
                        unit_id=unit.id,
                        order_id=order.id,
                        filename=f"{transfer_status}-{index}.dcm",
                        correlation_id=str(uuid4()),
                        status=transfer_status,
                    )
                    for index in range(amount)
                )
            db.commit()
            order_id = order.id
        page = self.client.get(f"/orders/{order_id}").text
        progress = page[page.index('class="flow-progress"') :]
        return {
            number: set((classes or "").split())
            for classes, number in _STEP.findall(progress)[:4]
        }

    def test_monitoring_after_everything_was_sent_highlights_only_retrieve(self):
        steps = self.steps("monitoring", uploaded=357)

        self.assertEqual(steps["03"], {"is-done", "is-current"})
        self.assertEqual(steps["04"], {"is-done"})

    def test_images_waiting_to_be_sent_again_highlight_the_send_step(self):
        steps = self.steps("wait_update", uploaded=357, compressed=10, upload_error=2)

        self.assertEqual(steps["03"], {"is-done", "is-current"})
        self.assertEqual(steps["04"], {"is-done", "is-current"})

    def test_finished_order_has_no_highlight(self):
        steps = self.steps("done", uploaded=10)

        self.assertEqual(steps["03"], {"is-done"})
        self.assertEqual(steps["04"], {"is-done"})

    def test_first_send_in_progress_is_not_done_yet(self):
        steps = self.steps("retrieving", compressed=5)

        self.assertEqual(steps["03"], set())
        self.assertEqual(steps["04"], set())
