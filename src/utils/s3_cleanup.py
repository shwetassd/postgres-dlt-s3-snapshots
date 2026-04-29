import logging
from urllib.parse import urlparse

import boto3

log = logging.getLogger(__name__)


def delete_table_snapshot_prefix(
    bucket_url: str,
    dataset_name: str,
    layout_prefix_template: str,
    table_name: str,
    snapshot_date: str,
) -> None:
    """Remove existing objects under full_load/{table}/{snapshot}/ before this job writes new Parquet.

    Called once per output table per job (before extract). It does **not** delete files produced
    earlier in the same worker — only prior runs' objects under that prefix.
    """
    parsed = urlparse(bucket_url)
    bucket = parsed.netloc
    base_prefix = parsed.path.lstrip("/")

    prefix = layout_prefix_template.format(
        table_name=table_name,
        snapshot_date=snapshot_date,
    ).strip("/")

    prefix_parts = []
    if base_prefix:
        prefix_parts.append(base_prefix.rstrip("/"))
    if dataset_name:
        prefix_parts.append(dataset_name.strip("/"))
    prefix_parts.append(prefix)

    full_prefix = "/".join(prefix_parts) + "/"
    full_uri = f"s3://{bucket}/{full_prefix}"

    log.info("Clearing existing snapshot under %s", full_uri)
    log.info(
        "Listing objects (large prefixes can take many minutes before the next log line)",
    )

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=full_prefix)

    keys_to_delete = []
    total_listed = 0
    total_deleted = 0
    page_no = 0
    for page in pages:
        page_no += 1
        contents = page.get("Contents", [])
        for obj in contents:
            keys_to_delete.append({"Key": obj["Key"]})
            total_listed += 1
            if len(keys_to_delete) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys_to_delete})
                total_deleted += len(keys_to_delete)
                keys_to_delete = []
        if page_no == 1 or page_no % 50 == 0:
            log.info(
                "S3 list progress: %s object(s) (%s list page(s))",
                total_listed,
                page_no,
            )

    if keys_to_delete:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys_to_delete})
        total_deleted += len(keys_to_delete)

    log.info("S3 delete done: listed %s object(s), deleted %s", total_listed, total_deleted)