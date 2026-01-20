from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from pipeline_runtime.performance.errors import WindowChunkingError


@dataclass(frozen=True)
class TimeWindow:
    start_ts: datetime
    end_ts_excl: datetime


def _require_utc(dt: datetime, name: str) -> datetime:
    if dt.tzinfo is None:
        raise WindowChunkingError(f"{name} must be timezone-aware (UTC)")
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc


def _floor_to_unit(dt: datetime, unit: str) -> datetime:
    dt = dt.astimezone(timezone.utc)
    if unit == "days":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if unit == "hours":
        return dt.replace(minute=0, second=0, microsecond=0)
    if unit == "minutes":
        return dt.replace(second=0, microsecond=0)
    raise WindowChunkingError(f"Unsupported unit: {unit}")


def _delta(unit: str, size: int) -> timedelta:
    if size <= 0:
        raise WindowChunkingError(f"size must be > 0, got {size}")
    if unit == "days":
        return timedelta(days=size)
    if unit == "hours":
        return timedelta(hours=size)
    if unit == "minutes":
        return timedelta(minutes=size)
    raise WindowChunkingError(f"Unsupported unit: {unit}")


def split_window(
    start_ts: datetime,
    end_ts_excl: datetime,
    unit: str,
    size: int,
    align_to_unit_boundary: bool,
) -> List[TimeWindow]:
    s = _require_utc(start_ts, "start_ts")
    e = _require_utc(end_ts_excl, "end_ts_excl")

    if e <= s:
        raise WindowChunkingError(f"end_ts_excl must be > start_ts: start={s.isoformat()} end={e.isoformat()}")

    step = _delta(unit, size)

    if align_to_unit_boundary:
        s_aligned = _floor_to_unit(s, unit)
        if s_aligned < s:
            s = s_aligned

    out: List[TimeWindow] = []
    cur = s
    while cur < e:
        nxt = cur + step
        if nxt > e:
            nxt = e
        out.append(TimeWindow(start_ts=cur, end_ts_excl=nxt))
        cur = nxt

    return out