"""Compact RUN SUMMARY block for logs (one scan-friendly card)."""

from __future__ import annotations

import textwrap


_INNER = 78


def _fmt_row(inner: str) -> str:
    if len(inner) <= _INNER:
        return "| " + inner.ljust(_INNER) + " |"
    lines = textwrap.wrap(
        inner,
        width=_INNER,
        break_long_words=True,
        break_on_hyphens=False,
    )
    return "\n".join("| " + ln.ljust(_INNER) + " |" for ln in lines)


def build_run_summary_box(
    *,
    runtime_cfg: dict,
    run_load_type: str,
    full_table_count: int,
    full_success: int,
    full_failed: int,
    full_rows: int,
    delta_table_count: int = 0,
    delta_success: int = 0,
    delta_failed: int = 0,
    delta_rows: int = 0,
    max_workers_delta: int = 4,
    total_time_sec: int,
    run_database: str | None,
    run_table: str | None,
    max_workers_full: int,
    serial_full_by_db: bool,
    skipped_fail_fast: int = 0,
    reported_exit_code: int | None = None,
) -> str:
    ok = full_failed == 0 and delta_failed == 0
    max_attempts = max(1, int(runtime_cfg.get("full_load_max_attempts", 1)))
    delta_attempts = max(1, int(runtime_cfg.get("delta_load_max_attempts", 1)))
    retry_delay = str(runtime_cfg.get("full_load_retry_delay_seconds", 20))
    skip_delete = bool(runtime_cfg.get("skip_delete_existing_snapshot", False))
    ff = bool(runtime_cfg.get("full_load_fail_fast", False))
    dff = bool(runtime_cfg.get("delta_load_fail_fast", False))

    if reported_exit_code is not None:
        exit_code = str(reported_exit_code)
    else:
        exit_code = "0" if ok else "1"

    fail_parts = []
    if full_failed:
        fail_parts.append(f"full_failed={full_failed}")
    if delta_failed:
        fail_parts.append(f"delta_failed={delta_failed}")
    status = "SUCCESS" if ok else "FAILED (" + ", ".join(fail_parts) + ")"

    lines: list[str] = []
    top = "+" + "-" * (_INNER + 2) + "+"
    mid = "+" + "=" * (_INNER + 2) + "+"

    lines.append(top)
    lines.append(_fmt_row("RUN SUMMARY"))
    lines.append(mid)

    lines.append(
        _fmt_row(
            f"status={status}   exit={exit_code}   duration={total_time_sec}s   "
            f"snapshot={runtime_cfg['snapshot_date']}"
        )
    )
    lines.append(
        _fmt_row(
            f"mode={run_load_type}   tables   full={full_table_count}   delta={delta_table_count}"
        )
    )

    filt_db = run_database or "*"
    filt_tb = run_table or "*"
    lines.append(_fmt_row(f"filter   RUN_DATABASE={filt_db}   RUN_TABLE={filt_tb}"))

    bucket = runtime_cfg["bucket_url"]
    lines.append(
        _fmt_row(f"output   {bucket}  dataset={runtime_cfg['dataset_name']}  {runtime_cfg['file_format']}")
    )

    lines.append(
        _fmt_row(
            f"rows_written   full={full_rows}   delta={delta_rows}   "
            f"|   table_tasks_ok_fail   full {full_success}/{full_failed}   delta {delta_success}/{delta_failed}"
        )
    )

    serial_note = "serial_by_db=yes" if serial_full_by_db else "serial_by_db=no"
    eb = runtime_cfg["extract_backend"]
    ecs = runtime_cfg["extract_chunk_size"]
    lines.append(
        _fmt_row(
            f"workers  full={max_workers_full}  delta={max_workers_delta}  {serial_note}  "
            f"fail_fast full={'yes' if ff else 'no'} delta={'yes' if dff else 'no'}"
        )
    )
    lines.append(
        _fmt_row(
            f"retries  full={max_attempts}  delta={delta_attempts}  delay_sec={retry_delay}  "
            f"extract  chunk={ecs}  {eb}"
        )
    )
    lines.append(
        _fmt_row(
            f"full_s3  replace_prefix_before_load={'yes' if runtime_cfg['delete_existing_full_load_snapshot'] else 'no'}   "
            f"SKIP_DELETE_EXISTING_SNAPSHOT={'yes' if skip_delete else 'no'}"
        )
    )
    lines.append(_fmt_row(f"dlt      pipelines_dir={runtime_cfg['pipelines_dir']}"))

    if skipped_fail_fast > 0:
        lines.append(_fmt_row(f"note     skipped_fail_fast={skipped_fail_fast} (cancelled after peer failure)"))

    lines.append(mid)

    return "\n".join(lines)
