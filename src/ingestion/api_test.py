from api_client import (
    fetch_page,
    validate_api_response
)

from checkpoint import (
    load_checkpoint,
    load_incremental_checkpoint,
    save_incremental_checkpoint,
    save_checkpoint
)

from deduplication import (
    deduplicate_records,
    load_existing_keys,
    process_incremental_batch
)

from incremental import (
    create_incremental_s3_key,
    create_new_records_s3_key,
    fetch_reconciliation_window,
    save_incremental_batch,
    save_new_records
)

from config import (
    RECONCILIATION_RECORDS,
    PAGE_SIZE
)

from raw_storage import save_page

from run_context import run_id


def run_ingestion():

    print("=" * 60)
    print("Bengaluru Air Quality API Ingestion")
    print("=" * 60)

    checkpoint = load_checkpoint()

    incremental_checkpoint = (
        load_incremental_checkpoint()
    )

    incremental_s3_key = (
        create_incremental_s3_key()
    )

    new_records_s3_key = (
        create_new_records_s3_key()
    )

    print("New records S3 key:")
    print(new_records_s3_key)

    print("Incremental S3 key:")
    print(incremental_s3_key)

    # -----------------------------
    # Incremental reconciliation
    # -----------------------------

    metadata_result = fetch_page(0, 1)

    if metadata_result is None:

        print("Failed to fetch API metadata.")
        raise RuntimeError(
            "Failed to fetch API metadata."
        )

    if not validate_api_response(metadata_result):

        print("Invalid API response.")
        raise RuntimeError(
            "Invalid API metadata response."
        )

    total = metadata_result["result"]["total"]

    print(f"Total records: {total}")

    print()

    print("Testing invalid API response...")

    invalid_response = {
        "success": True
    }

    if not validate_api_response(
        invalid_response
    ):

        print(
            "Invalid response correctly rejected."
        )

    else:

        print(
            "ERROR: Invalid response was accepted."
        )

    result, reconciliation_offset = (
        fetch_reconciliation_window(total)
    )

    if result is None:

        print("Reconciliation fetch failed.")

        print(
            "Stopping incremental ingestion safely."
        )

        raise RuntimeError(
            "Incremental reconciliation fetch failed."
        )

    if not validate_api_response(result):

        print("Invalid API response.")

        print(
            "Stopping incremental ingestion safely."
        )

        raise RuntimeError(
            "Invalid reconciliation API response."
        )

    save_incremental_batch(result)

    records = result["result"]["records"]

    unique_records = deduplicate_records(
        records
    )

    existing_keys = load_existing_keys()

    new_records = process_incremental_batch(
        records
    )

    print()

    print(
        "Incremental reconciliation results:"
    )

    print(
        "Records received:",
        len(records)
    )

    print(
        "Unique records:",
        len(unique_records)
    )

    print(
        "Duplicate records:",
        len(records) - len(unique_records)
    )

    print(
        "Existing keys loaded:",
        len(existing_keys)
    )

    print(
        "New records:",
        len(new_records)
    )

    print()

    print("Saving new records...")

    save_success = save_new_records(
        new_records
    )

    if not save_success:

        print(
            "Failed to save new records."
        )

        print(
            "Incremental checkpoint "
            "will not be updated."
        )

        raise RuntimeError(
            "Failed to save incremental new records."
        )

    incremental_checkpoint = {
        "last_successful_run_id": run_id,
        "last_successful_offset": reconciliation_offset,
        "reconciliation_records": RECONCILIATION_RECORDS,
        "new_records_found": len(new_records)
    }

    save_incremental_checkpoint(
        incremental_checkpoint
    )

    print()

    print(
        "Incremental reconciliation complete."
    )

    print(
        "Records received:",
        len(records)
    )

    print(
        "Unique records:",
        len(unique_records)
    )

    print(
        "Duplicate records:",
        len(records) - len(unique_records)
    )

    print(
        "Existing keys loaded:",
        len(existing_keys)
    )

    print(
        "New records:",
        len(new_records)
    )

    # -----------------------------
    # Full ingestion
    # -----------------------------

    offset = checkpoint[
        "last_successful_offset"
    ]

    page_number = (
        checkpoint["last_successful_page"] + 1
    )

    if offset >= total:

        print()

        print(
            "Full ingestion is already up to date."
        )

        print()

        print("=" * 60)

        print(
            "Ingestion process finished."
        )

        print("=" * 60)

        print(
            "Final checkpoint:",
            checkpoint
        )

        return

    while True:

        result = fetch_page(
            offset,
            PAGE_SIZE
        )

        if result is None:

            print(
                "Page ingestion failed after retries."
            )

            print(
                "Stopping ingestion safely."
            )

            print(
                "The checkpoint remains unchanged."
            )

            raise RuntimeError(
                "Page ingestion failed after retries."
            )

        records = result["result"]["records"]

        count = len(records)

        print(
            f"Records received: {count}"
        )

        if count == 0:

            print(
                "API returned zero records."
            )

            print(
                "Stopping ingestion."
            )

            raise RuntimeError(
                "API returned zero records during ingestion."
            )

        save_page(
            result,
            page_number
        )

        new_offset = offset + count

        checkpoint[
            "last_successful_page"
        ] = page_number

        checkpoint[
            "last_successful_offset"
        ] = new_offset

        save_checkpoint(
            checkpoint
        )

        offset = new_offset

        if offset >= total:

            print()

            print(
                "All available records "
                "have been processed."
            )

            break

        if count < PAGE_SIZE:

            print()

            print(
                "Final page detected."
            )

            break

        page_number += 1

    print()

    print("=" * 60)

    print(
        "Ingestion process finished."
    )

    print("=" * 60)

    print(
        "Final checkpoint:",
        checkpoint
    )


if __name__ == "__main__":
    run_ingestion()