from util import get_client

def upload_s3(body: bytes, bucket_name: str, key: str) -> dict:
    """Upload data to s3 bucket.

    Args:
        body (bytes): Data to upload.
        bucket_name (str): Name of the s3 bucket.
        key (str): Key of the file in the s3 bucket.

    Returns:
        dict: Response from s3.
    """
    s3_client = get_client()
    res = s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
    return res
