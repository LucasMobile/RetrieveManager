import os
import socket


def _check_hosts() -> list[str]:
    raw = os.getenv("STORE_CHECK_HOSTS", "127.0.0.1,worker,localhost")
    return [h.strip() for h in raw.split(",") if h.strip()]


def port_listening(port: int, host: str = "") -> bool:
    if not port:
        return False
    hosts = [host] if host else _check_hosts()
    for target in hosts:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.4)
                if sock.connect_ex((target, int(port))) == 0:
                    return True
        except OSError:
            continue
    return False
