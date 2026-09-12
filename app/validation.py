from __future__ import annotations

import ipaddress
import re
from pathlib import Path
from urllib.parse import urlparse

from app.config import ALLOWED_DATA_ROOTS, CLOUD_ALLOWED_HOSTS, IS_PRODUCTION

_AET = re.compile(r"^[A-Za-z0-9 _-]{1,16}$")
_HOSTNAME = re.compile(
    r"^(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$"
)
_MODALITY = re.compile(r"^(?:\*|[A-Z0-9]{1,8})$")
_JPEG_FLAGS = frozenset({"+e1", "+eb", "+ee"})


def bounded_int(
    value: str | int | None,
    field: str,
    *,
    minimum: int,
    maximum: int,
    default: int,
) -> int:
    try:
        number = int(value if value not in (None, "") else default)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}: informe um número inteiro") from exc
    if not minimum <= number <= maximum:
        raise ValueError(f"{field}: use um valor entre {minimum} e {maximum}")
    return number


def validate_aet(value: str, field: str) -> str:
    aet = value.strip()
    if not _AET.fullmatch(aet):
        raise ValueError(f"{field}: AET inválido (máximo de 16 caracteres)")
    return aet


def validate_host(value: str) -> str:
    host = value.strip()
    try:
        ipaddress.ip_address(host)
    except ValueError as exc:
        if not _HOSTNAME.fullmatch(host):
            raise ValueError("IP/host do PACS inválido") from exc
    return host


def validate_data_path(value: str, field: str) -> str:
    raw = value.strip()
    if not raw or not Path(raw).is_absolute():
        raise ValueError(f"{field}: informe um caminho absoluto")
    path = Path(raw).resolve(strict=False)
    if not any(
        path == root or path.is_relative_to(root) for root in ALLOWED_DATA_ROOTS
    ):
        roots = ", ".join(str(root) for root in ALLOWED_DATA_ROOTS)
        raise ValueError(f"{field}: caminho fora das raízes permitidas ({roots})")
    return str(path)


def validate_unit_form(form: dict[str, str]) -> dict[str, str | int | bool]:
    name = form.get("name", "").strip()
    if not 1 <= len(name) <= 120:
        raise ValueError("Nome da unidade deve ter entre 1 e 120 caracteres")
    if form.get("enabled", "1") not in {"0", "1"}:
        raise ValueError("Estado da unidade inválido")
    if form.get("retrieve_prior_enabled", "0") not in {"0", "1"}:
        raise ValueError("Configuração de exames anteriores inválida")
    return {
        "name": name,
        "enabled": form.get("enabled", "1") == "1",
        **validate_pacs_connection(form),
        "store_port": bounded_int(
            form.get("store_port"),
            "Porta do store",
            minimum=1,
            maximum=65535,
            default=444,
        ),
        "orders_api_url": validate_orders_api_url(form.get("orders_api_url", "")),
        "orders_api_token": form.get("orders_api_token", "").strip(),
        "orders_api_station_id": form.get("orders_api_station_id", "").strip(),
        "retrieve_prior_enabled": form.get("retrieve_prior_enabled", "0") == "1",
        "move_timeout_prior": bounded_int(
            form.get("move_timeout_prior"),
            "Timeout do retrieve de exames anteriores",
            minimum=60,
            maximum=14400,
            default=1800,
        ),
        "receive_dir": validate_data_path(
            form.get("receive_dir", ""), "Pasta de recebimento"
        ),
        "send_dir": validate_data_path(form.get("send_dir", ""), "Pasta de envio"),
        "error_dir": validate_data_path(form.get("error_dir", ""), "Pasta de erro"),
        "token": form.get("token", "").strip(),
        "move_timeout_first": bounded_int(
            form.get("move_timeout_first"),
            "Timeout do 1º C-MOVE",
            minimum=10,
            maximum=7200,
            default=600,
        ),
        "move_timeout_second": bounded_int(
            form.get("move_timeout_second"),
            "Timeout do 2º C-MOVE",
            minimum=10,
            maximum=7200,
            default=900,
        ),
        "max_parallel_moves": bounded_int(
            form.get("max_parallel_moves"),
            "C-MOVE em paralelo",
            minimum=1,
            maximum=16,
            default=1,
        ),
        "find_interval_seconds": bounded_int(
            form.get("find_interval_seconds"),
            "Intervalo C-FIND",
            minimum=5,
            maximum=3600,
            default=30,
        ),
        "compact_workers": bounded_int(
            form.get("compact_workers"),
            "Processos de compactação",
            minimum=1,
            maximum=32,
            default=8,
        ),
        "send_workers": bounded_int(
            form.get("send_workers"),
            "Envios em paralelo",
            minimum=1,
            maximum=64,
            default=16,
        ),
    }


def validate_orders_api_url(value: str) -> str:
    url = value.strip().rstrip("/")
    if not 1 <= len(url) <= 500:
        raise ValueError("URL da API Pedido PLERES deve ter entre 1 e 500 caracteres")
    parsed = urlparse(url)
    allowed_schemes = {"https"} if IS_PRODUCTION else {"http", "https"}
    if parsed.scheme not in allowed_schemes or not parsed.hostname:
        scheme = "HTTPS" if IS_PRODUCTION else "HTTP ou HTTPS"
        raise ValueError(f"URL da API Pedido PLERES inválida; use {scheme}")
    if parsed.username or parsed.password:
        raise ValueError("URL da API Pedido PLERES não pode conter credenciais")
    if parsed.query or parsed.fragment:
        raise ValueError("URL da API Pedido PLERES não deve conter query ou fragmento")
    return url


def validate_pacs_connection(form: dict[str, str]) -> dict[str, str | int]:
    """Validate only the fields required for a DICOM association."""
    return {
        "pacs_aet": validate_aet(form.get("pacs_aet", ""), "AET do PACS"),
        "pacs_ip": validate_host(form.get("pacs_ip", "")),
        "pacs_port": bounded_int(
            form.get("pacs_port"),
            "Porta do PACS",
            minimum=1,
            maximum=65535,
            default=2104,
        ),
        "calling_aet": validate_aet(form.get("calling_aet", ""), "Calling AET"),
    }


def validate_cloud_url(value: str) -> str:
    url = value.strip()
    if not 1 <= len(url) <= 500:
        raise ValueError("URL da nuvem deve ter entre 1 e 500 caracteres")
    parsed = urlparse(url)
    allowed_schemes = {"https"} if IS_PRODUCTION else {"http", "https"}
    if parsed.scheme not in allowed_schemes or not parsed.hostname:
        scheme = "HTTPS" if IS_PRODUCTION else "HTTP ou HTTPS"
        raise ValueError(f"URL da nuvem inválida; use {scheme}")
    if parsed.username or parsed.password:
        raise ValueError("URL da nuvem não pode conter credenciais")
    if parsed.hostname.lower() not in CLOUD_ALLOWED_HOSTS:
        allowed = ", ".join(sorted(CLOUD_ALLOWED_HOSTS))
        raise ValueError(f"Host da nuvem não permitido ({allowed})")
    return url


def validate_modality(value: str) -> str:
    modality = value.strip().upper()
    if not _MODALITY.fullmatch(modality):
        raise ValueError("Modalidade inválida")
    return modality


def validate_jpeg_flag(value: str) -> str:
    flag = value.strip()
    if flag not in _JPEG_FLAGS:
        raise ValueError("Perfil JPEG inválido")
    return flag
