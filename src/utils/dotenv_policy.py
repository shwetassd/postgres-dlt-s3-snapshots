"""Load `.env` only when not disabled (containers / AWS Batch should use injected env only)."""

from __future__ import annotations

import os


def load_dotenv_optional() -> None:
    if os.environ.get("SKIP_DOTENV", "").strip().lower() in ("1", "true", "yes"):
        return
    from dotenv import load_dotenv

    load_dotenv(override=False)
