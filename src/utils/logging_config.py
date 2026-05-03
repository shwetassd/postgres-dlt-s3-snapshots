"""Application logging and noisy third-party log/warning suppression."""

from __future__ import annotations

import logging
import os
import sys
import warnings


class _SuppressDltArrowSchemaHintWarnings(logging.Filter):
    """dlt logs WARNING on schema hint mismatches; not actionable for our filesystem loads."""

    _HINT_SUBSTRINGS = (
        "merging arrow schema",
        "column hints were different",
        "arrow schema with dlt schema",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno != logging.WARNING:
            return True
        msg = record.getMessage().lower()
        if any(s in msg for s in self._HINT_SUBSTRINGS):
            return False
        return True


def _attach_noise_filters(handler: logging.Handler) -> None:
    if getattr(handler, "_postgres_dlt_noise_filters", False):
        return
    handler.addFilter(_SuppressDltArrowSchemaHintWarnings())
    handler._postgres_dlt_noise_filters = True


def flush_logging_handlers() -> None:
    """Flush handlers so RUN SUMMARY reaches stderr/CloudWatch before SNS or process exit."""
    for handler in logging.root.handlers:
        try:
            handler.flush()
        except Exception:
            pass
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass


def configure_logging() -> logging.Logger:
    """Configure root logging once; LOG_LEVEL env overrides ``runtime.log_level`` in settings.yaml."""
    level_name = os.getenv("LOG_LEVEL", "").strip()
    if not level_name:
        try:
            from src.config_loader.loader import load_settings

            rt = load_settings().get("runtime") or {}
            level_name = str(rt.get("log_level") or "INFO").strip()
        except Exception:
            level_name = "INFO"
    level_name = level_name.upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    app_log = logging.getLogger("postgres_dlt_s3")
    app_log.setLevel(level)

    if root.handlers:
        root.setLevel(level)
        for h in root.handlers:
            _attach_noise_filters(h)
        _quiet_third_party_loggers()
        return app_log

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    _attach_noise_filters(handler)
    root.addHandler(handler)
    root.setLevel(level)

    _quiet_third_party_loggers()

    return app_log


def refresh_dlt_log_levels() -> None:
    """Call before pipeline.run(); dlt sometimes attaches WARNING-level loggers lazily during extract."""
    for h in logging.getLogger().handlers:
        _attach_noise_filters(h)
    _quiet_dlt_loggers()


def _quiet_dlt_loggers() -> None:
    """dlt submodules (e.g. extractors) often set their own level; silence WARNING noise."""
    logging.getLogger("dlt").setLevel(logging.ERROR)
    for sub in (
        "extractors",
        "normalize",
        "pipeline",
        "common",
        "extract",
        "load",
        "destination",
        "workers",
        "sources",
    ):
        logging.getLogger(f"dlt.{sub}").setLevel(logging.ERROR)
    mgr = getattr(logging.root.manager, "loggerDict", {})
    for name in list(mgr.keys()):
        if isinstance(name, str) and name.startswith("dlt."):
            logging.getLogger(name).setLevel(logging.ERROR)


def _quiet_third_party_loggers() -> None:
    """Reduce clutter from libraries (still log ERROR)."""
    noisy = (
        ("urllib3", logging.WARNING),
        ("urllib3.connectionpool", logging.WARNING),
        ("botocore", logging.WARNING),
        ("botocore.credentials", logging.ERROR),
        ("boto3", logging.WARNING),
        ("s3transfer", logging.WARNING),
        ("sqlalchemy.engine", logging.WARNING),
        ("sqlalchemy.pool", logging.WARNING),
        ("pyarrow", logging.WARNING),
        ("fsspec", logging.WARNING),
        ("s3fs", logging.WARNING),
    )
    for name, lvl in noisy:
        logging.getLogger(name).setLevel(lvl)
    _quiet_dlt_loggers()


def silence_common_warnings() -> None:
    """Filter warnings that are noisy but not actionable in batch logs."""
    warnings.filterwarnings(
        "ignore",
        message=r".*pandas only supports SQLAlchemy connectable.*",
        category=UserWarning,
    )
    warnings.filterwarnings(
        "ignore",
        message=r".*Could not infer format.*",
        category=UserWarning,
    )
