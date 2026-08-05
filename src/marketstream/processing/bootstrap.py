import boto3
from botocore.exceptions import ClientError

from marketstream.config import get_settings


def ensure_archive_bucket() -> None:
    settings = get_settings()
    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
    )
    try:
        client.head_bucket(Bucket=settings.s3_bucket)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        client.create_bucket(Bucket=settings.s3_bucket)


if __name__ == "__main__":
    ensure_archive_bucket()
