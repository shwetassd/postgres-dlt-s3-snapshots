"""Resolve operational settings: environment overrides YAML (`config/settings.yaml` → ``runtime``)."""

from __future__ import annotations

import os
from typing import Any


def _runtime(settings: dict) -> dict:
    return settings.get("runtime") or {}


def env_first_int(
    settings: dict,
    env_var: str,
    yaml_key: str,
    default: int,
) -> int:
    raw = os.environ.get(env_var)
    if raw is not None and str(raw).strip() != "":
        return int(raw)
    rt = _runtime(settings)
    if yaml_key in rt and rt[yaml_key] is not None:
        return int(rt[yaml_key])
    return default


def env_first_int_chain(
    settings: dict,
    env_vars: tuple[str, ...],
    yaml_keys: tuple[str, ...],
    default: int,
) -> int:
    for ev in env_vars:
        raw = os.environ.get(ev)
        if raw is not None and str(raw).strip() != "":
            return int(raw)
    rt = _runtime(settings)
    for yk in yaml_keys:
        if yk in rt and rt[yk] is not None:
            return int(rt[yk])
    return default


def env_first_float(
    settings: dict,
    env_var: str,
    yaml_key: str,
    default: float,
) -> float:
    raw = os.environ.get(env_var)
    if raw is not None and str(raw).strip() != "":
        return float(raw)
    rt = _runtime(settings)
    if yaml_key in rt and rt[yaml_key] is not None:
        return float(rt[yaml_key])
    return default


def env_first_float_chain(
    settings: dict,
    env_vars: tuple[str, ...],
    yaml_keys: tuple[str, ...],
    default: float,
) -> float:
    for ev in env_vars:
        raw = os.environ.get(ev)
        if raw is not None and str(raw).strip() != "":
            return float(raw)
    rt = _runtime(settings)
    for yk in yaml_keys:
        if yk in rt and rt[yk] is not None:
            return float(rt[yk])
    return default


def env_first_bool(
    settings: dict,
    env_var: str,
    yaml_key: str,
    default: bool,
) -> bool:
    if env_var in os.environ:
        v = os.environ[env_var].strip().lower()
        if v == "":
            pass
        else:
            return v in ("1", "true", "yes")
    rt = _runtime(settings)
    if yaml_key in rt and rt[yaml_key] is not None:
        return bool(rt[yaml_key])
    return default


def env_first_str(
    settings: dict,
    env_var: str,
    yaml_key: str,
    default: str,
) -> str:
    raw = os.environ.get(env_var)
    if raw is not None and raw.strip() != "":
        return raw
    rt = _runtime(settings)
    if yaml_key in rt and rt[yaml_key] is not None:
        return str(rt[yaml_key])
    return default


def _resolved_extract_chunk_size(settings: dict) -> int:
    raw = os.environ.get("EXTRACT_CHUNK_SIZE")
    if raw is not None and str(raw).strip() != "":
        return int(raw)
    rt = _runtime(settings)
    if rt.get("extract_chunk_size") is not None:
        return int(rt["extract_chunk_size"])
    return int(settings.get("extract", {}).get("chunk_size", 100000))


def _resolved_extract_backend(settings: dict) -> str:
    raw = os.environ.get("EXTRACT_BACKEND")
    if raw is not None and raw.strip() != "":
        return raw.strip()
    rt = _runtime(settings)
    if rt.get("extract_backend") is not None:
        return str(rt["extract_backend"])
    return str(settings.get("extract", {}).get("backend", "pandas"))


def build_operational_runtime(settings: dict) -> dict[str, Any]:
    """Flat dict merged into ``runtime_cfg`` in ``main`` (non-secret job tuning).

    Edit defaults in ``config/settings.yaml`` under ``runtime:``. Environment variables
    always win when set to a non-empty value (same names as before).
    """
    return {
        "full_load_max_attempts": env_first_int(
            settings, "FULL_LOAD_MAX_ATTEMPTS", "full_load_max_attempts", 1
        ),
        "full_load_retry_delay_seconds": env_first_float(
            settings, "FULL_LOAD_RETRY_DELAY_SECONDS", "full_load_retry_delay_seconds", 20.0
        ),
        "delta_load_max_attempts": env_first_int_chain(
            settings,
            ("DELTA_LOAD_MAX_ATTEMPTS", "FULL_LOAD_MAX_ATTEMPTS"),
            ("delta_load_max_attempts", "full_load_max_attempts"),
            1,
        ),
        "delta_load_retry_delay_seconds": env_first_float_chain(
            settings,
            ("DELTA_LOAD_RETRY_DELAY_SECONDS", "FULL_LOAD_RETRY_DELAY_SECONDS"),
            ("delta_load_retry_delay_seconds", "full_load_retry_delay_seconds"),
            20.0,
        ),
        "skip_delete_existing_snapshot": env_first_bool(
            settings,
            "SKIP_DELETE_EXISTING_SNAPSHOT",
            "skip_delete_existing_snapshot",
            False,
        ),
        "full_load_fail_fast": env_first_bool(
            settings, "FULL_LOAD_FAIL_FAST", "full_load_fail_fast", False
        ),
        "delta_load_fail_fast": env_first_bool(
            settings, "DELTA_LOAD_FAIL_FAST", "delta_load_fail_fast", False
        ),
        "phase_watchdog_interval_sec": env_first_int(
            settings,
            "PHASE_WATCHDOG_INTERVAL_SEC",
            "phase_watchdog_interval_sec",
            180,
        ),
        "pipeline_clear_workdir_before_run": env_first_bool(
            settings,
            "PIPELINE_CLEAR_WORKDIR_BEFORE_RUN",
            "pipeline_clear_workdir_before_run",
            False,
        ),
        "max_workers_full": env_first_int(settings, "MAX_WORKERS_FULL", "max_workers_full", 4),
        "max_workers_delta": env_first_int(settings, "MAX_WORKERS_DELTA", "max_workers_delta", 4),
        "full_load_database_serial": env_first_bool(
            settings,
            "FULL_LOAD_DATABASE_SERIAL",
            "full_load_database_serial",
            False,
        ),
        "exit_on_table_failures": env_first_bool(
            settings, "EXIT_ON_TABLE_FAILURES", "exit_on_table_failures", False
        ),
        "sns_publish_delay_seconds": env_first_int(
            settings, "SNS_PUBLISH_DELAY_SECONDS", "sns_publish_delay_seconds", 0
        ),
        "sns_always_publish_failure_digest": env_first_bool(
            settings,
            "SNS_ALWAYS_PUBLISH_FAILURE_DIGEST",
            "sns_always_publish_failure_digest",
            False,
        ),
        # When true (default): skip SNS on Batch attempt > 1 unless SNS_ALWAYS_PUBLISH_FAILURE_DIGEST.
        "sns_skip_digest_on_batch_retry": env_first_bool(
            settings,
            "SNS_PUBLISH_FAILURE_DIGEST_ON_BATCH_RETRY_ONLY_ONCE",
            "sns_skip_digest_on_batch_retry",
            True,
        ),
        "full_load_retry_clear_pipeline_workdir": env_first_bool(
            settings,
            "FULL_LOAD_RETRY_CLEAR_PIPELINE_WORKDIR",
            "full_load_retry_clear_pipeline_workdir",
            False,
        ),
        "run_summary_s3_upload": env_first_bool(
            settings,
            "RUN_SUMMARY_S3_UPLOAD",
            "run_summary_s3_upload",
            False,
        ),
        "extract_chunk_size": _resolved_extract_chunk_size(settings),
        "extract_backend": _resolved_extract_backend(settings),
    }
