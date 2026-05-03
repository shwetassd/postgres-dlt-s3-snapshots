"""Optional JSON run summary object on S3 for Batch / automation (Parquet data paths unchanged)."""

from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any
from urllib.parse import urlparse

import boto3

log = logging.getLogger(__name__)


def _s3_bucket_and_prefix(bucket_url: str) -> tuple[str, str]:
    parsed = urlparse(bucket_url)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"run summary upload expects s3:// URL, got {bucket_url!r}")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/").rstrip("/")
    return bucket, prefix


def maybe_upload_run_summary_json(
    *,
    bucket_url: str,
    dataset_name: str,
    snapshot_date: str,
    payload: dict[str, Any],
) -> None:
    """Write one JSON object next to dataset data (does not replace pipeline Parquet layout)."""
    bucket, base_prefix = _s3_bucket_and_prefix(bucket_url)
    parts = [base_prefix, dataset_name.strip("/"), "_batch_run_summaries", snapshot_date.strip("/")]
    job_id = (
        (os.getenv("AWS_BATCH_JOB_ID") or "").strip()
        or (os.getenv("ECS_TASK_ID") or "").strip()
        or uuid.uuid4().hex
    )
    key = "/".join(p for p in parts if p) + f"/{job_id}.json"

    body = json.dumps(payload, indent=2, default=str).encode("utf-8")
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    log.info("Run summary JSON → s3://%s/%s (%s bytes)", bucket, key, len(body))
