"""Minimal Query/Retrieve SCP (pynetdicom) used as a PACS in network tests.

It answers Study Root C-FIND by exact (or trailing ``*``) matching on the
query keys and performs C-MOVE by sending real C-STORE sub-operations to the
requested destination, so the receiver can be exercised end to end.
"""

from __future__ import annotations

import socket
import time
from collections.abc import Iterable

from pydicom.dataset import Dataset
from pynetdicom import AE, StoragePresentationContexts, evt
from pynetdicom.sop_class import (
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelMove,
    Verification,
)

SERIES_KEYS = {"SeriesInstanceUID", "Modality", "BodyPartExamined"}


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _matches(stored: Dataset, query: Dataset) -> bool:
    for element in query:
        keyword = element.keyword
        if keyword in ("QueryRetrieveLevel", "SpecificCharacterSet"):
            continue
        wanted = str(element.value or "")
        if not wanted:
            continue  # universal matching / return key
        value = str(stored.get(keyword, "") or "")
        if "-" in wanted and keyword.endswith("Date"):
            start, end = wanted.split("-", 1)
            if not (start or "0") <= value <= (end or "99999999"):
                return False
        elif wanted.endswith("*"):
            if not value.startswith(wanted[:-1]):
                return False
        elif value != wanted:
            return False
    return True


class FakePacs:
    def __init__(
        self,
        images: Iterable[Dataset],
        *,
        ae_title: str = "PACS",
        destinations: dict[str, tuple[str, int]] | None = None,
        move_delay: float = 0.0,
    ) -> None:
        self.images = list(images)
        self.ae_title = ae_title
        self.destinations = destinations or {}
        self.move_delay = move_delay
        self.port = free_port()
        self.moves: list[str] = []
        self._server = None

    def start(self) -> FakePacs:
        ae = AE(ae_title=self.ae_title)
        ae.add_supported_context(StudyRootQueryRetrieveInformationModelFind)
        ae.add_supported_context(StudyRootQueryRetrieveInformationModelMove)
        ae.add_supported_context(Verification)
        # Contexts proposed when it acts as SCU for the C-STORE sub-operations.
        for context in StoragePresentationContexts[:100]:
            ae.add_requested_context(context.abstract_syntax)
        ae.require_called_aet = True
        self._server = ae.start_server(
            ("127.0.0.1", self.port),
            block=False,
            evt_handlers=[
                (evt.EVT_C_FIND, self._on_find),
                (evt.EVT_C_MOVE, self._on_move),
            ],
        )
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()

    def _select(self, query: Dataset) -> list[Dataset]:
        return [image for image in self.images if _matches(image, query)]

    def _on_find(self, event):
        query = event.identifier
        level = str(query.get("QueryRetrieveLevel", ""))
        seen: set[str] = set()
        for image in self._select(query):
            key = (
                image.SeriesInstanceUID if level == "SERIES" else image.StudyInstanceUID
            )
            if key in seen:
                continue
            seen.add(key)
            response = Dataset()
            response.QueryRetrieveLevel = level
            for element in query:
                if element.keyword == "QueryRetrieveLevel":
                    continue
                if element.keyword == "NumberOfStudyRelatedInstances":
                    count = sum(
                        1
                        for other in self.images
                        if other.StudyInstanceUID == image.StudyInstanceUID
                    )
                    response.NumberOfStudyRelatedInstances = str(count)
                elif element.keyword == "NumberOfSeriesRelatedInstances":
                    count = sum(
                        1
                        for other in self.images
                        if other.SeriesInstanceUID == image.SeriesInstanceUID
                    )
                    response.NumberOfSeriesRelatedInstances = str(count)
                elif element.keyword == "ModalitiesInStudy":
                    response.ModalitiesInStudy = image.get("Modality", "")
                else:
                    setattr(response, element.keyword, image.get(element.keyword, ""))
            yield 0xFF00, response

    def _on_move(self, event):
        destination = event.move_destination
        if isinstance(destination, bytes):
            destination = destination.decode()
        destination = destination.strip()
        self.moves.append(destination)
        if destination not in self.destinations:
            yield None, None
            return
        yield self.destinations[destination]
        if self.move_delay:
            time.sleep(self.move_delay)
        matches = self._select(event.identifier)
        yield len(matches)
        for image in matches:
            yield 0xFF00, image
