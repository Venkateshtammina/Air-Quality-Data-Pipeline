from datetime import datetime


ingestion_date = datetime.now().date().isoformat()

run_id = datetime.now().strftime("%Y%m%d_%H%M%S")