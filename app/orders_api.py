from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiohttp

from app.config import ORDERS_API_MAX_RETRIES, ORDERS_API_TIMEOUT_SECONDS

RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class OrdersApiError(RuntimeError):
    pass


class InvalidApiOrder(ValueError):
    pass


@dataclass(frozen=True)
class ApiOrder:
    source_id: str
    patient_id: str
    accession_number: str
    patient_birthdate: str
    exam_date: str


@dataclass(frozen=True)
class AckResult:
    accession_number: str
    success: bool
    error: str = ""


def _required_text(item: dict[str, Any], key: str) -> str:
    value = item.get(key)
    text = str(value).replace("\xa0", " ").strip() if value is not None else ""
    if not text:
        raise InvalidApiOrder(f"campo obrigatório ausente: {key}")
    return text


def _dicom_date(value: str, field: str) -> str:
    try:
        return datetime.strptime(value, "%m/%d/%Y %H:%M:%S").strftime("%Y%m%d")
    except ValueError as exc:
        raise InvalidApiOrder(
            f"{field} inválido; esperado MM/DD/YYYY HH:MM:SS"
        ) from exc


def parse_api_order(item: dict[str, Any]) -> ApiOrder:
    if not isinstance(item, dict):
        raise InvalidApiOrder("pedido não é um objeto JSON")
    birthdate = _required_text(item, "patientBirthdate")
    exam_date = _required_text(item, "examDate")
    return ApiOrder(
        source_id=str(item.get("_id") or "").strip(),
        patient_id=_required_text(item, "patientId"),
        accession_number=_required_text(item, "accessionNumber"),
        patient_birthdate=_dicom_date(birthdate, "patientBirthdate"),
        exam_date=_dicom_date(exam_date, "examDate"),
    )


async def _request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    token: str,
    **kwargs: Any,
) -> tuple[int, str]:
    last_error = "falha desconhecida"
    for attempt in range(1, ORDERS_API_MAX_RETRIES + 1):
        try:
            async with session.request(
                method,
                url,
                headers={"Content-Type": "application/json", "token": token},
                **kwargs,
            ) as response:
                body = await response.text()
                if response.status < 300:
                    return response.status, body
                # O corpo pode conter dados clínicos; nunca o inclua no erro/log.
                last_error = f"HTTP {response.status}"
                if response.status not in RETRYABLE_STATUSES:
                    break
        except (aiohttp.ClientError, TimeoutError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt < ORDERS_API_MAX_RETRIES:
            await asyncio.sleep(1.5 * (2 ** (attempt - 1)))
    raise OrdersApiError(last_error)


async def fetch_orders(url: str, token: str) -> list[dict[str, Any]]:
    timeout = aiohttp.ClientTimeout(total=ORDERS_API_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        status, body = await _request(session, "GET", url, token)
    if status == 204 or not body.strip():
        return []
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise OrdersApiError("GET retornou JSON inválido") from exc
    if not isinstance(payload, list):
        raise OrdersApiError("GET deve retornar um array JSON")
    return payload


async def acknowledge_orders(
    url: str, token: str, accessions: list[str]
) -> list[AckResult]:
    if not accessions:
        return []
    endpoint = f"{url.rstrip('/')}/"
    timeout = aiohttp.ClientTimeout(total=ORDERS_API_TIMEOUT_SECONDS)
    results: list[AckResult] = []
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for accession in accessions:
            try:
                await _request(
                    session,
                    "PUT",
                    endpoint,
                    token,
                    params={"accessionNumber": accession},
                    json={"mirthReaded": True},
                )
                results.append(AckResult(accession, True))
            except OrdersApiError as exc:
                results.append(AckResult(accession, False, str(exc)))
    return results
