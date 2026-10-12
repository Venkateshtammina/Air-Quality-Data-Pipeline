
import os
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

api_key = os.getenv("OPENAI_API_KEY")

if not api_key:
    raise RuntimeError(
        "OPENAI_API_KEY was not found in the project .env file."
    )

client = OpenAI(api_key=api_key)

try:
    response = client.models.list()
    print("OpenAI API authentication successful.")
    print("Models available to this API key:", len(response.data))
except Exception as exc:
    print("OpenAI API check failed:", type(exc).__name__)
    raise
