import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError
import os


def upload_file(file_name: str, bucket: str, object_name: Optional[str] = None) -> bool:
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["AWS_SECRET_KEY"],
    )
    try:
        response = s3_client.upload_file(
            file_name,
            bucket,
            object_name,
            ExtraArgs={"ACL": "bucket-owner-full-control"},
        )
        logging.info(
            f"successfully uploaded {file_name} to {bucket} with response {response.json()}"
        )
        return True
    except ClientError as e:
        logging.error(e)
        return False
