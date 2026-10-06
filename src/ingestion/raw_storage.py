import json
import os

from s3_client import s3

from config import (
    DATASET_NAME,
    S3_BUCKET,
    S3_RAW_PREFIX
)

from run_context import ingestion_date


def save_page(data, page_number):
    """
    Save the raw API response locally and upload it to S3.
    """

    local_directory = (
        f"data/raw/{DATASET_NAME}/"
        f"ingestion_date={ingestion_date}"
    )

    os.makedirs(
        local_directory,
        exist_ok=True
    )

    local_file_path = (
        f"{local_directory}/"
        f"page_{page_number:04d}.json"
    )

    s3_key = (
        f"{S3_RAW_PREFIX}/"
        f"ingestion_date={ingestion_date}/"
        f"page_{page_number:04d}.json"
    )

    with open(
        local_file_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            indent=2
        )

    s3.upload_file(
        local_file_path,
        S3_BUCKET,
        s3_key
    )

    print(
        f"Page {page_number} uploaded to:"
    )

    print(
        f"s3://{S3_BUCKET}/{s3_key}"
    )