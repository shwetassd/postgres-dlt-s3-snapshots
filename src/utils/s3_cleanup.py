from urllib.parse import urlparse
import boto3


def delete_table_snapshot_prefix(
    bucket_url: str,
    dataset_name: str,
    layout_prefix_template: str,
    table_name: str,
    snapshot_date: str,
) -> None:
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

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=full_prefix)

    keys_to_delete = []
    for page in pages:
        for obj in page.get("Contents", []):
            keys_to_delete.append({"Key": obj["Key"]})
            if len(keys_to_delete) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys_to_delete})
                keys_to_delete = []

    if keys_to_delete:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys_to_delete})