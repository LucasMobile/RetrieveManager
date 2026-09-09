from __future__ import annotations

import logging
import subprocess

from app.dicom_tools import ToolMissing, start_storescp
from app.models import Unit
from app.observability import log_event

log = logging.getLogger("worker")


class StoreSupervisor:
    def __init__(self) -> None:
        self._procs: dict[int, subprocess.Popen] = {}
        self._sig: dict[int, tuple[str, int, str]] = {}

    def reconcile(self, units: list[Unit]) -> dict[int, str]:
        wanted = {u.id: u for u in units if u.enabled}
        status: dict[int, str] = {}

        for uid, proc in list(self._procs.items()):
            unit = wanted.get(uid)
            sig = self._sig.get(uid)
            dead = proc.poll() is not None
            mismatch = unit is None or sig != (
                unit.dest_aet,
                unit.store_port,
                unit.receive_dir,
            )
            if dead or mismatch:
                self._stop(uid)
                if dead and unit is not None:
                    status[uid] = "reiniciando"

        for uid, unit in wanted.items():
            if uid in self._procs and self._procs[uid].poll() is None:
                status[uid] = "no ar"
                continue
            try:
                proc = start_storescp(unit.dest_aet, unit.store_port, unit.receive_dir)
            except ToolMissing as exc:
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.store.start",
                    resource=f"unit:{uid}",
                    status="failure",
                    error=exc,
                    unit_id=uid,
                )
                status[uid] = "storescp ausente"
                continue
            except OSError as exc:
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.store.start",
                    resource=f"unit:{uid}",
                    status="failure",
                    error=exc,
                    unit_id=uid,
                )
                status[uid] = "falha ao subir"
                continue
            self._procs[uid] = proc
            self._sig[uid] = (unit.dest_aet, unit.store_port, unit.receive_dir)
            status[uid] = "no ar"
            log_event(
                log,
                logging.INFO,
                "dicom.store.start",
                resource=f"unit:{uid}",
                status="success",
                unit_id=uid,
                process_id=proc.pid,
            )
        return status

    def snapshot(self) -> dict[int, str]:
        out = {}
        for uid, proc in list(self._procs.items()):
            out[uid] = "no ar" if proc.poll() is None else "parado"
        return out

    def stop_all(self) -> None:
        for uid in list(self._procs):
            self._stop(uid)

    def _stop(self, uid: int) -> None:
        proc = self._procs.pop(uid, None)
        self._sig.pop(uid, None)
        if proc is None or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
