import glob
import json

from config import DATASET_NAME


def create_record_key(record):
    """
    Create the natural key for an air-quality observation.
    """

    return (
        record["Station ID"],
        record["Timestamp"]
    )


def deduplicate_records(records):
    """
    Remove duplicate observations from an incoming batch.
    """

    seen_keys = set()
    unique_records = []

    for record in records:

        key = create_record_key(record)

        if key not in seen_keys:

            seen_keys.add(key)
            unique_records.append(record)

    return unique_records


def load_existing_keys():
    """
    Load natural keys from previously stored full-ingestion raw data.
    """

    raw_directory = f"data/raw/{DATASET_NAME}"

    files = glob.glob(
        f"{raw_directory}/ingestion_date=*/page_*.json"
    )

    existing_keys = set()

    for file in files:

        with open(
            file,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        records = data["result"]["records"]

        for record in records:

            key = create_record_key(record)

            existing_keys.add(key)

    return existing_keys


def find_new_records(records, existing_keys):
    """
    Return records that do not already exist
    in the processed dataset.
    """

    new_records = []

    for record in records:

        key = create_record_key(record)

        if key not in existing_keys:

            new_records.append(record)

    return new_records


def process_incremental_batch(records):
    """
    Process an incoming incremental batch
    and return genuinely new records.
    """

    unique_records = deduplicate_records(records)

    existing_keys = load_existing_keys()

    new_records = find_new_records(
        unique_records,
        existing_keys
    )

    return new_records