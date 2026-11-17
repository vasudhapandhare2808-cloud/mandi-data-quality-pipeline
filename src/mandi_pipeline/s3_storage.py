
"""AWS S3 storage utilities for the Mandi Data Quality Pipeline."""

import logging
from pathlib import Path

import boto3

BUCKET_NAME = "mandi-data-pipeline-vasudha"
AWS_REGION = "ap-south-1"
AWS_PROFILE = "mandi-pipeline"

ALLOWED_PREFIXES = {
    "raw",
    "bronze",
    "silver",
    "silver/accepted",
    "silver/rejected",
    "gold",
}

logger = logging.getLogger(__name__)


def get_s3_client():
    """Create an S3 client using the configured AWS CLI profile."""
    session = boto3.Session(
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION,
    )
    return session.client("s3")


def upload_file(local_path, s3_prefix):
    """Upload a local file to S3 and verify the uploaded file size."""
    local_path = Path(local_path)

    if not local_path.is_file():
        raise FileNotFoundError(f"File not found: {local_path}")

    if s3_prefix not in ALLOWED_PREFIXES:
        raise ValueError(f"Invalid S3 prefix: {s3_prefix}")

    s3_key = f"{s3_prefix}/{local_path.name}"
    client = get_s3_client()

    logger.info(
        "Uploading %s to s3://%s/%s",
        local_path,
        BUCKET_NAME,
        s3_key,
    )

    client.upload_file(
        str(local_path),
        BUCKET_NAME,
        s3_key,
    )

    response = client.head_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
    )

    uploaded_size = response["ContentLength"]
    local_size = local_path.stat().st_size

    if uploaded_size != local_size:
        raise RuntimeError(
            f"Upload size mismatch: local={local_size}, "
            f"S3={uploaded_size}"
        )

    logger.info(
        "S3 upload verified: s3://%s/%s",
        BUCKET_NAME,
        s3_key,
    )

    return f"s3://{BUCKET_NAME}/{s3_key}"
