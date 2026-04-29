"""Row counts from dlt pipeline normalize step."""

from __future__ import annotations

from typing import Any


def normalize_row_counts(pipeline: Any) -> dict[str, int]:
    """Return resource/table name -> row count from last normalize step (excludes _dlt internals)."""
    out: dict[str, int] = {}
    try:
        trace = getattr(pipeline, "last_trace", None)
        if trace is None:
            return out
        ni = getattr(trace, "last_normalize_info", None)
        if ni is None:
            return out
        rc = getattr(ni, "row_counts", None)
        if not isinstance(rc, dict):
            return out
        for k, v in rc.items():
            ks = str(k)
            if ks.startswith("_dlt"):
                continue
            try:
                out[ks] = int(v)
            except (TypeError, ValueError):
                continue
    except Exception:
        return out
    return out


def total_rows(normalize_counts: dict[str, int]) -> int:
    return sum(normalize_counts.values())
