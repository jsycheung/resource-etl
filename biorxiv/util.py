import boto3
from botocore.errorfactory import ClientError
from datetime import datetime as dt
from datetime import timedelta as td

def get_client():
    return boto3.client("s3")

def get_prev_date(bucket_name: str, file_prefix: str, bookmark_filename: str, baseline_date: str) -> str:
    """Get the previous date from the bookmark file in s3 bucket.

    Args:
        bucket_name (str): Name of the s3 bucket.
        file_prefix (str): Prefix of the file in the s3 bucket.
        bookmark_filename (str): Name of the bookmark file.
        baseline_date (str): Baseline date to return if no bookmark file is found. In the format of YYYY-MM-DD.

    Returns:
        str: Previous date.
    """
    s3_client = get_client()
    try:
        bookmark_file = s3_client.get_object(Bucket=bucket_name, Key=f"{file_prefix}/{bookmark_filename}")
        prev_date = bookmark_file["Body"].read().decode("utf-8")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            prev_date = baseline_date
        else:
            raise e
    return prev_date

def get_next_date(prev_date: str) -> str:
    """Get the next date from the previous date.

    Args:
        prev_date (str): Previous date. In the format of YYYY-MM-DD.

    Returns:
        str: Next date. In the format of YYYY-MM-DD.
    """
    next_date = dt.strftime(dt.strptime(prev_date, "%Y-%m-%d") + td(days=1), "%Y-%m-%d")
    return next_date

def upload_bookmark(bucket_name: str, file_prefix: str, bookmark_filename: str, processed_date: str) -> None:
    """Upload the processed date to the bookmark file in s3 bucket.

    Args:
        bucket_name (str): Name of the s3 bucket.
        file_prefix (str): Prefix of the file in the s3 bucket.
        bookmark_filename (str): Name of the bookmark file.
        processed_date (str): Processed date. In the format of YYYY-MM-DD.
    """
    s3_client = get_client()
    s3_client.put_object(Bucket=bucket_name, Key=f"{file_prefix}/{bookmark_filename}", Body=processed_date.encode("utf-8"))
