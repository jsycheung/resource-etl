import os
import json
from download import download_file
from upload import upload_s3
from util import get_prev_date, get_next_date, upload_bookmark
from datetime import datetime as dt


def lambda_handler(event, context):
    """Lambda function to download data from medrxiv api, process data by filtering for keyword, and upload to s3 bucket."""
    bucket_name = os.environ.get("BUCKET_NAME")
    bookmark_filename = os.environ.get("BOOKMARK_FILENAME")
    baseline_date = os.environ.get("BASELINE_DATE")
    file_prefix = os.environ.get("FILE_PREFIX")
    regex = os.environ.get("REGEX")
    while True:
        prev_date = get_prev_date(
            bucket_name, file_prefix, bookmark_filename, baseline_date
        )
        date_to_download = get_next_date(prev_date)
        # If today's date is reached, break the loop because data is not yet available for today
        if date_to_download == dt.now().strftime("%Y-%m-%d"):
            break
        download_data_list = download_file(date_to_download, regex)
        # If None or empty list is returned, there is no data for that day or no relevant results from regex, upload bookmark and continue to next day
        if download_data_list is None or download_data_list == []:
            upload_bookmark(
                bucket_name, file_prefix, bookmark_filename, date_to_download
            )
            continue
        # Case where data is processed and uploaded to s3
        else:
            upload_s3(
                json.dumps(download_data_list).encode("utf-8"),
                bucket_name,
                f"{file_prefix}/{date_to_download}.json",
            )
            print(f"{date_to_download}.json successfully processed and uploaded.")
            upload_bookmark(
                bucket_name, file_prefix, bookmark_filename, date_to_download
            )
