import boto3
from functools import lru_cache
from typing import Optional


@lru_cache
def get_aws_session(region: Optional[str] = None) -> boto3.Session:
    """
    Creates and returns a SINGLE cached AWS session.

    - Thread-safe
    - Reused across the entire application
    - Credentials are picked automatically from:
      env vars / IAM role / AWS config
    """
    return boto3.Session(region_name=region)


@lru_cache
def get_aws_client(
    service_name: str,
    region: Optional[str] = None,
):
    """
    Generic AWS client factory.

    Example:
        s3 = get_aws_client("s3")
        secrets = get_aws_client("secretsmanager")
    """
    session = get_aws_session(region)
    return session.client(service_name)
