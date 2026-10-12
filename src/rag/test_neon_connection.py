
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# Locate the project's root .env file
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

database_url = os.getenv("NEON_DATABASE_URL")

if not database_url:
    raise ValueError("NEON_DATABASE_URL is missing from the root .env file.")

try:
    with psycopg2.connect(database_url, connect_timeout=10) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT current_database(), current_user, version();"
            )
            database_name, database_user, version = cursor.fetchone()

            print("Neon connection successful!")
            print(f"Database: {database_name}")
            print(f"User: {database_user}")
            print(f"PostgreSQL: {version.split(',')[0]}")

except Exception as error:
    print(f"Connection failed: {type(error).__name__}")
    print("Check your Neon connection string and network access.")
