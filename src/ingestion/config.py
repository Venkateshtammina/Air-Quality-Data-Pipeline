RECONCILIATION_RECORDS = 1000

S3_BUCKET = "agri-data-engineering"

API_URL = "https://data.opencity.in/api/3/action/datastore_search"

RESOURCE_ID = "c39c61b2-771b-4eb9-9b0c-93ba511755c0"

DATASET_NAME = "air_quality"

S3_RAW_PREFIX = f"raw/{DATASET_NAME}"

S3_CHECKPOINT_KEY = (
    f"checkpoints/{DATASET_NAME}_checkpoint.json"
)

S3_INCREMENTAL_CHECKPOINT_KEY = (
    f"checkpoints/{DATASET_NAME}_incremental_checkpoint.json"
)

LOCAL_CHECKPOINT_FILE = "checkpoint.json"

PAGE_SIZE = 5000

MAX_RETRIES = 3

RETRY_DELAY_SECONDS = 2