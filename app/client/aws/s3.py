import boto3
from botocore.exceptions import ClientError
from typing import List, Optional


class S3Service:
    def __init__(
        self,
        bucket_name: str,
        region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        self.bucket_name = bucket_name

        self.s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    # -------------------------
    # Upload File
    # -------------------------
    def upload_file(
        self,
        file_path: str,
        s3_key: str,
        content_type: Optional[str] = None,
    ):
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type

        self.s3.upload_file(
            Filename=file_path,
            Bucket=self.bucket_name,
            Key=s3_key,
            ExtraArgs=extra_args or None,
        )

    # -------------------------
    # Upload Bytes / Stream
    # -------------------------
    def upload_bytes(
        self,
        data: bytes,
        s3_key: str,
        content_type: Optional[str] = None,
    ):
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type

        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=data,
            **extra_args,
        )

    # -------------------------
    # Download File
    # -------------------------
    def download_file(self, s3_key: str, file_path: str):
        self.s3.download_file(
            Bucket=self.bucket_name,
            Key=s3_key,
            Filename=file_path,
        )

    # -------------------------
    # Read Object as Bytes
    # -------------------------
    def get_object_bytes(self, s3_key: str) -> bytes:
        response = self.s3.get_object(
            Bucket=self.bucket_name,
            Key=s3_key,
        )
        return response["Body"].read()

    # -------------------------
    # List Objects
    # -------------------------
    def list_objects(self, prefix: str = "") -> List[str]:
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=prefix,
        ):
            for item in page.get("Contents", []):
                keys.append(item["Key"])

        return keys

    # -------------------------
    # Delete Object
    # -------------------------
    def delete_object(self, s3_key: str):
        self.s3.delete_object(
            Bucket=self.bucket_name,
            Key=s3_key,
        )

    # -------------------------
    # Delete Multiple Objects
    # -------------------------
    def delete_objects(self, keys: List[str]):
        objects = [{"Key": key} for key in keys]

        self.s3.delete_objects(
            Bucket=self.bucket_name,
            Delete={"Objects": objects},
        )

    # -------------------------
    # Check if Object Exists
    # -------------------------
    def object_exists(self, s3_key: str) -> bool:
        try:
            self.s3.head_object(
                Bucket=self.bucket_name,
                Key=s3_key,
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    # -------------------------
    # Generate Presigned URL
    # -------------------------
    def generate_presigned_url(
        self,
        s3_key: str,
        expires_in: int = 3600,
    ) -> str:
        return self.s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={
                "Bucket": self.bucket_name,
                "Key": s3_key,
            },
            ExpiresIn=expires_in,
        )
