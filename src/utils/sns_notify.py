"""Optional SNS alert when table loads fail (configure SNS_FAILURE_TOPIC_ARN).

Duplicate digests: AWS Batch retries re-run the process; main.py skips publishing on
attempt > 1 unless SNS_ALWAYS_PUBLISH_FAILURE_DIGEST=1 (see env vars there).
Separate duplicate emails can come from multiple SNS subscriptions or CloudWatch→ERROR filters.
"""

from __future__ import annotations

import logging
import os
import re

log = logging.getLogger(__name__)

# SNS Publish API maximum message body size (binary / UTF-8).
_MAX_MESSAGE_BYTES = 256 * 1024

# Max length for one failure line in SNS (plain text).
_MAX_LINE_CHARS = 450


def _split_failure_entry(entry: str) -> tuple[str, str, str] | None:
    """Parse ``db.schema.table: ExcType: message`` (message may contain ``: ``)."""
    parts = entry.split(": ", 2)
    if len(parts) < 3:
        return None
    return parts[0], parts[1], parts[2]


def _lines_to_column_types(block: str) -> dict[str, str]:
    """Parse PyArrow-style ``col: type`` lines before ``-- schema metadata --``."""
    if "-- schema metadata --" in block:
        block = block.split("-- schema metadata --", 1)[0]
    out: dict[str, str] = {}
    for raw in block.splitlines():
        line = raw.strip()
        if not line or line.startswith("--"):
            continue
        if ": " not in line:
            continue
        name, _, typ = line.partition(": ")
        name = name.strip()
        typ = typ.strip()
        if not name:
            continue
        # Skip noise lines
        if name.lower() == "pandas":
            continue
        out[name] = typ
    return out


def _summarize_arrow_schema_mismatch(detail: str) -> str | None:
    if "Table schema does not match" not in detail:
        return None
    text = detail.replace("\r\n", "\n")
    anchor = text.find("Table schema does not match")
    rest = text[anchor:] if anchor >= 0 else text
    # Arrow compares two blocks labeled table: / file: at line start (avoid JSON false positives).
    t_mark = re.search(r"(?ms)^table:\s*\n", rest)
    f_mark = re.search(r"(?ms)^file:\s*\n", rest)
    if not t_mark or not f_mark or f_mark.start() <= t_mark.end():
        return "Arrow schema mismatch (see logs for full dump)"

    table_raw = rest[t_mark.end() : f_mark.start()]
    file_raw = rest[f_mark.end() :]

    table_cols = _lines_to_column_types(table_raw)
    file_cols = _lines_to_column_types(file_raw)
    if not table_cols and not file_cols:
        return "Arrow schema mismatch (see logs for full dump)"

    diffs: list[str] = []
    all_keys = sorted(set(table_cols) | set(file_cols))
    for k in all_keys:
        tv = table_cols.get(k)
        fv = file_cols.get(k)
        if tv == fv:
            continue
        if tv is None:
            diffs.append(f"{k} (missing in table chunk, file={_short_type(fv)})")
        elif fv is None:
            diffs.append(f"{k} (missing in file, table={_short_type(tv)})")
        else:
            diffs.append(f"{k} (table={_short_type(tv)} vs file={_short_type(fv)})")

    if not diffs:
        return "Arrow schema mismatch — column sets equal but metadata differs (see logs)"

    summary = "; ".join(diffs[:12])
    if len(diffs) > 12:
        summary += f"; … +{len(diffs) - 12} more"
    return f"Extract/schema mismatch: {summary}"


def _short_type(t: str) -> str:
    s = re.sub(r"\s+", " ", t.strip())
    if len(s) > 72:
        return s[:69] + "…"
    return s


def _summarize_disk_or_os(detail: str) -> str | None:
    if "No space left on device" in detail:
        return "Disk full (errno 28 No space left on device)"
    m = re.search(r"\[Errno\s+(\d+)\]\s*(.+?)(?:\n|$)", detail)
    if m:
        return f"OS error errno={m.group(1)} {_short_type(m.group(2))}"
    return None


def _strip_pipeline_boilerplate(detail: str) -> str:
    """Remove leading dlt wrapper text to leave the inner exception line."""
    text = detail.strip()
    # Inner "<class 'X'>\nmessage" → drop class line
    text = re.sub(r"^<class '[^']+'>\s*\n?", "", text)
    # First line often enough if short
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return ""
    # Prefer substantive lines
    for ln in lines[:8]:
        if ln.startswith("Pipeline execution failed"):
            continue
        if ln.startswith("step="):
            continue
        return ln
    return lines[0]


def _summarize_generic(detail: str) -> str:
    inner = _strip_pipeline_boilerplate(detail)
    inner = re.sub(r"\s+", " ", inner)
    if len(inner) <= _MAX_LINE_CHARS:
        return inner
    return inner[: _MAX_LINE_CHARS - 1] + "…"


def format_failure_for_sns(entry: str) -> str:
    """One concise line per failure for SNS: table, issue type, short hint."""
    parsed = _split_failure_entry(entry)
    if not parsed:
        return entry[:_MAX_LINE_CHARS]

    table_ref, exc_name, detail = parsed

    arrow = _summarize_arrow_schema_mismatch(detail)
    if arrow:
        line = f"{table_ref} — {exc_name} — {arrow}"
        return line[:_MAX_LINE_CHARS]

    disk = _summarize_disk_or_os(detail)
    if disk:
        line = f"{table_ref} — {exc_name} — {disk}"
        return line[:_MAX_LINE_CHARS]

    short = _summarize_generic(detail)
    line = f"{table_ref} — {exc_name} — {short}"
    return line[:_MAX_LINE_CHARS]


def publish_load_failures(
    failures: list[str],
    *,
    pipeline_name: str,
    snapshot_date: str,
    bucket_url: str,
    dataset_name: str,
) -> None:
    """Publish one SNS message listing failed tables (compact lines).

    Set env SNS_FAILURE_TOPIC_ARN to the topic ARN. Requires IAM sns:Publish on that topic.
    If the variable is unset or empty, this is a no-op. SNS errors are logged and do not raise.
    """
    topic_arn = (os.getenv("SNS_FAILURE_TOPIC_ARN") or "").strip()
    if not topic_arn or not failures:
        return

    try:
        import boto3
    except ImportError:
        log.warning("boto3 not available; skipping SNS notification")
        return

    compact = [format_failure_for_sns(e) for e in failures]

    lines = [
        "This digest is sent only after the job finishes all phases and prints RUN SUMMARY in CloudWatch.",
        "(If you received another alert earlier, it is likely a CloudWatch subscription on ERROR logs, not this digest.)",
        "",
        f"Pipeline      : {pipeline_name}",
        f"Snapshot date : {snapshot_date}",
        f"Dataset       : {dataset_name}",
        f"Bucket URL    : {bucket_url}",
        "",
        f"Failed tables ({len(failures)}):",
        "",
    ]
    for i, row in enumerate(compact, start=1):
        lines.append(f"{i}. {row}")

    message = "\n".join(lines)
    raw = message.encode("utf-8")
    if len(raw) > _MAX_MESSAGE_BYTES:
        message = (
            raw[: _MAX_MESSAGE_BYTES - 80].decode("utf-8", errors="ignore")
            + "\n\n… (message truncated for SNS size limit)"
        )

    subject = f"[{pipeline_name}] {len(failures)} load failure(s) · {snapshot_date}"
    if len(subject) > 100:
        subject = subject[:97] + "..."

    try:
        boto3.client("sns").publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message,
        )
        log.info("SNS failure notification published (topic configured)")
    except Exception:
        log.exception("SNS publish failed — job outcome unchanged")
