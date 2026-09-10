from __future__ import annotations

import logging
import subprocess
from collections import deque
from threading import Thread

from app.config import store_bind_port
from app.dicom_tools import ToolMissing, redact_dicom_output, start_storescp
from app.models import Unit
from app.observability import log_event

log = logging.getLogger("worker")

_STARTUP_TIMEOUT_SECONDS = 0.25
_OUTPUT_TAIL_LINES = 20


class StoreSCPStartError(RuntimeError):
    pass


class StoreSupervisor:
    def __init__(self) -> None:
        self._procs: dict[int, subprocess.Popen[str]] = {}
        self._sig: dict[int, tuple[str, int, str]] = {}
        self._output: dict[int, deque[str]] = {}
        self._readers: dict[int, Thread] = {}

    def reconcile(self, units: list[Unit]) -> dict[int, str]:
        wanted = {u.id: u for u in units if u.enabled}
        status: dict[int, str] = {}

        for uid, proc in list(self._procs.items()):
            unit = wanted.get(uid)
            sig = self._sig.get(uid)
            dead = proc.poll() is not None
            mismatch = unit is None or sig != (
                unit.calling_aet,
                unit.store_port,
                unit.receive_dir,
            )
            if dead or mismatch:
                if dead and unit is not None:
                    self._log_unexpected_exit(uid, proc)
                self._stop(uid)
                if dead and unit is not None:
                    status[uid] = "reiniciando"

        for uid, unit in wanted.items():
            if uid in self._procs and self._procs[uid].poll() is None:
                status[uid] = "no ar"
                continue
            try:
                bind_port = store_bind_port(unit.store_port)
                proc = start_storescp(unit.calling_aet, bind_port, unit.receive_dir)
                self._procs[uid] = proc
                self._sig[uid] = (unit.calling_aet, unit.store_port, unit.receive_dir)
                self._start_output_reader(uid, proc)
                try:
                    return_code = proc.wait(timeout=_STARTUP_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired:
                    pass
                else:
                    self._join_reader(uid)
                    detail = self._output_tail(uid)
                    self._discard(uid)
                    raise StoreSCPStartError(
                        f"storescp encerrou na inicialização (código {return_code})"
                        + (f": {detail}" if detail else "")
                    )
            except ToolMissing as exc:
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.store.start",
                    resource=f"unit:{uid}",
                    status="failure",
                    error=exc,
                    unit_id=uid,
                    error_detail=str(exc),
                )
                status[uid] = "storescp ausente"
                continue
            except (OSError, StoreSCPStartError) as exc:
                self._discard(uid)
                log_event(
                    log,
                    logging.ERROR,
                    "dicom.store.start",
                    resource=f"unit:{uid}",
                    status="failure",
                    error=exc,
                    unit_id=uid,
                    store_port=unit.store_port,
                    bind_port=store_bind_port(unit.store_port),
                    error_detail=str(exc),
                )
                status[uid] = "falha ao subir"
                continue
            status[uid] = "no ar"
            log_event(
                log,
                logging.INFO,
                "dicom.store.start",
                resource=f"unit:{uid}",
                status="success",
                unit_id=uid,
                process_id=proc.pid,
                store_port=unit.store_port,
                bind_port=store_bind_port(unit.store_port),
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
        if proc is None:
            self._discard(uid)
            return
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        self._join_reader(uid)
        self._discard(uid)

    def _start_output_reader(self, uid: int, proc: subprocess.Popen[str]) -> None:
        output = deque(maxlen=_OUTPUT_TAIL_LINES)
        self._output[uid] = output

        def drain() -> None:
            if proc.stdout is None:
                return
            for line in proc.stdout:
                safe_line = redact_dicom_output(line).strip()
                if safe_line:
                    output.append(safe_line)

        reader = Thread(target=drain, name=f"storescp-output-{uid}", daemon=True)
        self._readers[uid] = reader
        reader.start()

    def _log_unexpected_exit(self, uid: int, proc: subprocess.Popen[str]) -> None:
        self._join_reader(uid)
        log_event(
            log,
            logging.ERROR,
            "dicom.store.exit",
            resource=f"unit:{uid}",
            status="failure",
            unit_id=uid,
            return_code=proc.returncode,
            error_detail=self._output_tail(uid) or "sem saída do storescp",
        )

    def _output_tail(self, uid: int) -> str:
        return " | ".join(self._output.get(uid, ()))[:2000]

    def _join_reader(self, uid: int) -> None:
        reader = self._readers.get(uid)
        if reader is not None:
            reader.join(timeout=1)

    def _discard(self, uid: int) -> None:
        self._procs.pop(uid, None)
        self._sig.pop(uid, None)
        self._readers.pop(uid, None)
        self._output.pop(uid, None)
