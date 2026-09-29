from __future__ import annotations

import ipaddress
import re
from pathlib import Path
from urllib.parse import urlparse

from app.config import (
    ALLOWED_DATA_ROOTS,
    CLOUD_ALLOWED_HOSTS,
    DEFAULT_CLOUD_URL,
    IS_PRODUCTION,
)

_AET = re.compile(r"^[A-Za-z0-9 _-]{1,16}$")
_HOSTNAME = re.compile(
    r"^(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$"
)
_MODALITY = re.compile(r"^(?:\*|[A-Z0-9]{1,8})$")
_JPEG_FLAGS = frozenset({"lossless", "lossy"})


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


def validate_store_allowed_aets(value: str) -> str:
    """Normalize the Store SCP sender allowlist; an empty list accepts anyone."""
    accepted: list[str] = []
    seen: set[str] = set()
    for item in re.split(r"[,;\n]", value or ""):
        if not item.strip():
            continue
        aet = validate_aet(item, "AE Titles autorizados a enviar")
        if aet.upper() not in seen:
            seen.add(aet.upper())
            accepted.append(aet)
    if len(accepted) > 32:
        raise ValueError("AE Titles autorizados a enviar: informe no máximo 32")
    return ",".join(accepted)


StoreNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network


def validate_store_allowed_ips(value: str) -> str:
    """Normalize the Store SCP IP allowlist; an empty list accepts any address.

    Items are single addresses or CIDR ranges (192.168.3.0/24).
    """
    accepted: list[str] = []
    for item in re.split(r"[,;\s]+", value or ""):
        if not item:
            continue
        try:
            network = ipaddress.ip_network(item, strict=False)
        except ValueError as exc:
            raise ValueError(
                f"IPs autorizados a enviar: {item[:45]} não é um IP nem uma faixa "
                "válida (ex.: 192.168.3.103 ou 192.168.3.0/24)"
            ) from exc
        text = (
            str(network.network_address) if network.num_addresses == 1 else str(network)
        )
        if text not in accepted:
            accepted.append(text)
    if len(accepted) > 64:
        raise ValueError("IPs autorizados a enviar: informe no máximo 64")
    return ",".join(accepted)


def store_allowed_networks(value: str | None) -> tuple[StoreNetwork, ...]:
    return tuple(
        ipaddress.ip_network(item.strip(), strict=False)
        for item in (value or "").split(",")
        if item.strip()
    )


def peer_ip_allowed(peer_ip: str, networks: tuple[StoreNetwork, ...]) -> bool:
    """An empty allowlist accepts everyone; unparsable peers are refused."""
    if not networks:
        return True
    try:
        address = ipaddress.ip_address(peer_ip.split("%", 1)[0])
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped
    return any(address in network for network in networks)


def store_allowed_senders(value: str | None) -> frozenset[str]:
    """Return the stored allowlist in its case-insensitive comparison form."""
    return frozenset(
        item.strip().upper() for item in (value or "").split(",") if item.strip()
    )


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


def validate_orders_api_company_id(value: str | None) -> str:
    company_id = (value or "").strip()
    if not company_id:
        raise ValueError("Empresa ID é obrigatório.")
    if not re.fullmatch(r"[0-9]{1,64}", company_id) or int(company_id) <= 0:
        raise ValueError(
            "Empresa ID deve ser um número inteiro positivo de até 64 dígitos."
        )
    return str(int(company_id))


def validate_unit_form(
    form: dict[str, str], *, creating: bool = False
) -> dict[str, str | int | bool]:
    name = form.get("name", "").strip()
    if not 1 <= len(name) <= 120:
        raise ValueError("Nome da unidade deve ter entre 1 e 120 caracteres")
    if form.get("enabled", "1") not in {"0", "1"}:
        raise ValueError("Estado da unidade inválido")
    if form.get("retrieve_prior_enabled", "0") not in {"0", "1"}:
        raise ValueError("Configuração de exames anteriores inválida")
    if form.get("pacs_patient_id_wildcard", "0") not in {"0", "1"}:
        raise ValueError("Configuração do curinga no Patient ID inválida")
    orders_api_token = form.get("orders_api_token", "").strip()
    station_id = form.get("orders_api_station_id", "").strip()
    token = form.get("token", "").strip()
    if creating and not token:
        raise ValueError("Token da unidade é obrigatório.")
    if len(token) > 64:
        raise ValueError("Token da unidade excede 64 caracteres.")
    if creating and not orders_api_token:
        raise ValueError("Token de Integração é obrigatório.")
    if len(orders_api_token) > 2048:
        raise ValueError("Token de Integração excede 2048 caracteres.")
    if len(station_id) > 64:
        raise ValueError("ID Posto excede 64 caracteres.")
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
        "pacs_patient_id_wildcard": form.get("pacs_patient_id_wildcard", "0") == "1",
        "store_allowed_ips": validate_store_allowed_ips(
            form.get("store_allowed_ips", "")
        ),
        "store_allowed_aets": validate_store_allowed_aets(
            form.get("store_allowed_aets", "")
        ),
        "orders_api_url": validate_orders_api_url(form.get("orders_api_url", "")),
        "orders_api_token": orders_api_token,
        "orders_api_station_id": station_id,
        "orders_api_company_id": validate_orders_api_company_id(
            form.get("orders_api_company_id", "")
        ),
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
        "token": token,
        "cloud_url": validate_cloud_url(form.get("cloud_url", DEFAULT_CLOUD_URL)),
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
    return _validate_http_url(
        value,
        label="URL da API Pedido PLERES",
        strip_trailing_slash=True,
        forbid_query_or_fragment=True,
    )


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
    return _validate_http_url(
        value,
        label="URL da nuvem",
        allowed_hosts=CLOUD_ALLOWED_HOSTS,
    )


def _validate_http_url(
    value: str,
    *,
    label: str,
    allowed_hosts: frozenset[str] | set[str] | None = None,
    strip_trailing_slash: bool = False,
    forbid_query_or_fragment: bool = False,
) -> str:
    url = value.strip()
    if strip_trailing_slash:
        url = url.rstrip("/")
    if not 1 <= len(url) <= 500:
        raise ValueError(f"{label} deve ter entre 1 e 500 caracteres")
    parsed = urlparse(url)
    allowed_schemes = {"https"} if IS_PRODUCTION else {"http", "https"}
    if parsed.scheme not in allowed_schemes or not parsed.hostname:
        scheme = "HTTPS" if IS_PRODUCTION else "HTTP ou HTTPS"
        raise ValueError(f"{label} inválida; use {scheme}")
    if parsed.username or parsed.password:
        raise ValueError(f"{label} não pode conter credenciais")
    if forbid_query_or_fragment and (parsed.query or parsed.fragment):
        raise ValueError(f"{label} não deve conter query ou fragmento")
    if allowed_hosts is not None and parsed.hostname.lower() not in allowed_hosts:
        allowed = ", ".join(sorted(allowed_hosts))
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
        raise ValueError("Perfil de compactação inválido")
    return flag
