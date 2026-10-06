import json

from s3_client import s3

from config import (
    S3_BUCKET,
    S3_CHECKPOINT_KEY,
    S3_INCREMENTAL_CHECKPOINT_KEY,
    LOCAL_CHECKPOINT_FILE
)


def load_checkpoint():
    """
    Load the last successful ingestion checkpoint from S3.

    If no checkpoint exists, start from the beginning.
    """

    try:
        s3.download_file(
            S3_BUCKET,
            S3_CHECKPOINT_KEY,
            LOCAL_CHECKPOINT_FILE
        )

        with open(
            LOCAL_CHECKPOINT_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            checkpoint = json.load(file)

        print("Checkpoint loaded:", checkpoint)

        return checkpoint

    except s3.exceptions.ClientError as e:

        error_code = e.response["Error"]["Code"]

        if error_code in ["404", "NoSuchKey"]:

            print("No checkpoint found.")
            print("Starting ingestion from the beginning.")

            return {
                "last_successful_page": 0,
                "last_successful_offset": 0
            }

        raise


def load_incremental_checkpoint():
    """
    Load the last successful incremental ingestion checkpoint from S3.
    """

    try:
        s3.download_file(
            S3_BUCKET,
            S3_INCREMENTAL_CHECKPOINT_KEY,
            "incremental_checkpoint.json"
        )

        with open(
            "incremental_checkpoint.json",
            "r",
            encoding="utf-8"
        ) as file:

            checkpoint = json.load(file)

        print(
            "Incremental checkpoint loaded:",
            checkpoint
        )

        return checkpoint

    except s3.exceptions.ClientError as e:

        error_code = e.response["Error"]["Code"]

        if error_code in ["404", "NoSuchKey"]:

            print("No incremental checkpoint found.")
            print(
                "Starting incremental ingestion "
                "from the initial state."
            )

            return {
                "last_successful_run_id": None
            }

        raise


def save_incremental_checkpoint(checkpoint):
    """
    Save the incremental ingestion checkpoint locally
    and upload it to S3.
    """

    local_file = "incremental_checkpoint.json"

    with open(
        local_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            checkpoint,
            file,
            indent=2
        )

    s3.upload_file(
        local_file,
        S3_BUCKET,
        S3_INCREMENTAL_CHECKPOINT_KEY
    )

    print(
        "Incremental checkpoint saved:",
        checkpoint
    )


def save_checkpoint(checkpoint):
    """
    Save the ingestion checkpoint locally
    and then upload it to S3.
    """

    with open(
        LOCAL_CHECKPOINT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            checkpoint,
            file,
            indent=2
        )

    s3.upload_file(
        LOCAL_CHECKPOINT_FILE,
        S3_BUCKET,
        S3_CHECKPOINT_KEY
    )

    print("Checkpoint saved:", checkpoint)