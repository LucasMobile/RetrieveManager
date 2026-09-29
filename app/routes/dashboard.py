"""Overview page and its cached unit/folder statistics."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Lock
from time import monotonic
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import case, func, or_, select
from sqlalchemy.orm import Session

from app.config import (
    DASHBOARD_FILE_COUNT_CACHE_SECONDS,
)
from app.db import get_db
from app.models import (
    Order,
    Unit,
    User,
)
from app.netutil import port_listening
from app.pager import paginate
from app.pipeline import folder_counts
from app.web import DASH_PAGE, ctx, require_user, templates

router = APIRouter()


_dashboard_stats_cache: dict[str, Any] = {"expires_at": 0.0}


_dashboard_stats_lock = Lock()


_dashboard_folder_cache: dict[
    tuple[int, str, str, str], tuple[float, dict[str, int]]
] = {}


_dashboard_folder_lock = Lock()


def _unit_runtime(unit: Unit) -> tuple[dict[str, int], bool]:
    cache_key = (
        unit.id,
        unit.receive_dir,
        unit.send_dir,
        unit.error_dir,
    )
    now = monotonic()
    with _dashboard_folder_lock:
        cached = _dashboard_folder_cache.get(cache_key)
        folders = dict(cached[1]) if cached and now < cached[0] else None
    if folders is None:
        folders = folder_counts(unit)
        with _dashboard_folder_lock:
            _dashboard_folder_cache[cache_key] = (
                now + max(1, DASHBOARD_FILE_COUNT_CACHE_SECONDS),
                dict(folders),
            )
    return (
        folders,
        port_listening(unit.store_port) if unit.enabled else False,
    )


def _dashboard_units(
    db: Session, page: int
) -> tuple[list[Unit], dict[str, int | bool], dict[str, int]]:
    total = (
        db.scalar(
            select(func.count()).select_from(Unit).where(Unit.deleted_at.is_(None))
        )
        or 0
    )
    pager = paginate(total, page, DASH_PAGE)
    units = list(
        db.scalars(
            select(Unit)
            .where(Unit.deleted_at.is_(None))
            .order_by(Unit.name)
            .offset(pager["offset"])
            .limit(pager["size"])
        )
    )
    with _dashboard_stats_lock:
        cache_valid = _dashboard_stats_cache.get("engine_id") == id(
            db.get_bind()
        ) and monotonic() < float(_dashboard_stats_cache.get("expires_at", 0.0))
        if cache_valid:
            stats = _dashboard_stats_cache["stats"]
            summary = _dashboard_stats_cache["summary"]
        else:
            stats: dict[int, tuple[int, int, int, int]] = {}
            day = datetime.now() - timedelta(hours=24)
            rows = db.execute(
                select(
                    Order.unit_id,
                    func.sum(case((Order.status == "watching", 1), else_=0)),
                    func.sum(
                        case(
                            (
                                Order.status.in_(("wait_retrieve", "wait_second"))
                                | Order.prior_status.in_(("queued", "retry_wait")),
                                1,
                            ),
                            else_=0,
                        )
                    ),
                    func.sum(
                        case(
                            (
                                Order.status.in_(("retrieving", "retrieving_second"))
                                | (Order.prior_status == "retrieving"),
                                1,
                            ),
                            else_=0,
                        )
                    ),
                    func.sum(
                        case(
                            (
                                (
                                    (Order.status == "error")
                                    | (Order.prior_status == "error")
                                )
                                & (Order.updated_at >= day),
                                1,
                            ),
                            else_=0,
                        )
                    ),
                )
                .where(
                    Order.archived_at.is_(None),
                    or_(
                        Order.status.in_(
                            (
                                "watching",
                                "wait_retrieve",
                                "wait_second",
                                "retrieving",
                                "retrieving_second",
                                "error",
                            )
                        ),
                        Order.prior_status.in_(
                            ("queued", "retry_wait", "retrieving", "error")
                        ),
                    ),
                )
                .group_by(Order.unit_id)
            )
            for row in rows:
                stats[int(row[0])] = tuple(int(value or 0) for value in row[1:5])
            summary = {
                "enabled": int(
                    db.scalar(
                        select(func.count())
                        .select_from(Unit)
                        .where(
                            Unit.enabled.is_(True),
                            Unit.deleted_at.is_(None),
                        )
                    )
                    or 0
                ),
                "watching": sum(row[0] for row in stats.values()),
                "queue": sum(row[1] for row in stats.values()),
                "running": sum(row[2] for row in stats.values()),
                "error": sum(row[3] for row in stats.values()),
            }
            _dashboard_stats_cache.update(
                expires_at=monotonic() + 5,
                engine_id=id(db.get_bind()),
                stats=stats,
                summary=summary,
            )
    if units:
        with ThreadPoolExecutor(max_workers=len(units)) as executor:
            runtime = list(executor.map(_unit_runtime, units))
        for unit, (folders, store_up) in zip(units, runtime, strict=True):
            unit.folders = folders  # type: ignore[attr-defined]
            unit.store_up = store_up  # type: ignore[attr-defined]
    views = []
    for unit in units:
        watching, queue, running, errors = stats.get(unit.id, (0, 0, 0, 0))
        unit.counts = {  # type: ignore[attr-defined]
            "watching": watching,
            "queue": queue,
            "running": running,
            "error": errors,
        }
        views.append(unit)
    return views, pager, summary


@router.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    page: int = 1,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    units, pager, summary = _dashboard_units(db, page)
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context=ctx(
            request, db, "dash", units=units, pager=pager, summary=summary, qs=""
        ),
    )


@router.get("/dashboard/partial", response_class=HTMLResponse)
def dashboard_partial(
    request: Request,
    page: int = 1,
    db: Session = Depends(get_db),
    user: User = Depends(require_user),
):
    units, pager, summary = _dashboard_units(db, page)
    return templates.TemplateResponse(
        request=request,
        name="dashboard_partial.html",
        context=ctx(
            request, db, "dash", units=units, pager=pager, summary=summary, qs=""
        ),
    )
