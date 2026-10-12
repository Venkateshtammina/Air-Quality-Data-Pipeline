
import boto3
from dotenv import load_dotenv
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
)
