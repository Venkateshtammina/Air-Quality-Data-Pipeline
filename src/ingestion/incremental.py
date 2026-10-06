import json

from s3_client import s3

from config import (
    RECONCILIATION_RECORDS,
    S3_BUCKET,
    S3_RAW_PREFIX
)

from run_context import run_id

from api_client import fetch_page

def calculate_reconciliation_offset(total):
    """
    Calculate the starting offset for the reconciliation window.
    """

    return max(
        0,
        total - RECONCILIATION_RECORDS
    )


def create_incremental_s3_key():
    """
    Create the S3 path for one incremental ingestion run.
    """

    return (
        f"{S3_RAW_PREFIX}/"
        f"incremental/"
        f"run_id={run_id}/"
        f"reconciliation.json"
    )


def create_new_records_s3_key():
    """
    Create the S3 path for genuinely new records
    from one incremental ingestion run.
    """

    return (
        f"{S3_RAW_PREFIX}/"
        f"incremental/"
        f"new/"
        f"run_id={run_id}/"
        f"new_records.json"
    )


def save_new_records(new_records):
    """
    Save genuinely new records locally and upload them to S3.
    """

    if not new_records:

        print("No new records found.")
        return True

    local_file = "new_records.json"

    with open(
        local_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            new_records,
            file,
            indent=2
        )

    s3_key = create_new_records_s3_key()

    try:

        s3.upload_file(
            local_file,
            S3_BUCKET,
            s3_key
        )

    except Exception as e:

        print(
            f"Failed to upload new records to S3: {e}"
        )

        return False

    print("New records uploaded to:")
    print(f"s3://{S3_BUCKET}/{s3_key}")

    return True


def save_incremental_batch(data):
    """
    Save an incremental reconciliation batch locally
    and upload it to S3.
    """

    local_file = "reconciliation.json"

    with open(
        local_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            indent=2
        )

    s3_key = create_incremental_s3_key()

    s3.upload_file(
        local_file,
        S3_BUCKET,
        s3_key
    )

    print("Incremental batch uploaded to:")
    print(f"s3://{S3_BUCKET}/{s3_key}")


def fetch_reconciliation_window(total):
    """
    Fetch the latest records from the API
    for incremental reconciliation.
    """

    offset = calculate_reconciliation_offset(total)

    print(
        f"Reconciliation window starts at offset: {offset}"
    )

    print(
        f"Records to fetch: {RECONCILIATION_RECORDS}"
    )

    return fetch_page(
        offset,
        RECONCILIATION_RECORDS
    ), offset