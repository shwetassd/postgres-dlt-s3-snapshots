"""Pandas helpers for chunked SQL extract → Parquet (stable Arrow string types)."""

from __future__ import annotations

import pandas as pd

# Columns that must not flip int↔string or timestamp↔string across chunks (PyArrow schema merge).
_STABLE_STRING_COLS = frozenset(
    {
        "sender_user_id",
        "receiver_user_id",
        "receiver_company_id",
        "email_send_at",
        "event_received_at",
        "request_received_at",
    }
)


def _cell_to_stable_string(val) -> str:
    """Timestamps → ISO; numbers/NA → str or empty (Arrow-friendly large_string only)."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    if hasattr(val, "isoformat") and not isinstance(val, str):
        try:
            return val.isoformat()
        except (OSError, ValueError, TypeError):
            return str(val)
    return str(val)


def normalize_sql_chunk_dtypes(chunk_df: pd.DataFrame) -> pd.DataFrame:
    """Stabilize Arrow types across SQL chunks (string vs large_string; object vs string)."""
    chunk_df = chunk_df.convert_dtypes()

    for col in list(chunk_df.columns):
        if col in _STABLE_STRING_COLS:
            chunk_df[col] = (
                chunk_df[col].map(_cell_to_stable_string).astype(pd.StringDtype())
            )
            continue
        s = chunk_df[col]
        if isinstance(s.dtype, pd.StringDtype):
            continue
        if pd.api.types.is_object_dtype(s.dtype):
            try:
                chunk_df[col] = s.astype(pd.StringDtype())
            except (TypeError, ValueError):
                # JSON/text from PG occasionally mixes None, str, or decoded values — force Arrow-safe strings.
                chunk_df[col] = s.map(_cell_to_stable_string).astype(pd.StringDtype())
    return chunk_df
