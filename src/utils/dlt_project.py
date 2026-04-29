"""Ensure dlt reads project config from cwd (.dlt/config.toml)."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

log = logging.getLogger(__name__)

TEMPLATE = Path("config/dlt/config.toml")
ACTIVE = Path(".dlt/config.toml")


def ensure_dlt_config_from_repo() -> None:
    """Copy tracked template into .dlt/config.toml so Docker/Batch picks it up (.dockerignore skips .dlt/)."""
    if not TEMPLATE.is_file():
        log.debug("No dlt config template at %s — using dlt defaults", TEMPLATE)
        return
    ACTIVE.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(TEMPLATE, ACTIVE)
    log.debug("Installed dlt config: %s -> %s", TEMPLATE, ACTIVE)
