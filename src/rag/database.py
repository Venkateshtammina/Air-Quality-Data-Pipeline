
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from src.rag.sql_validator import validate_sql


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def execute_read_only_query(sql: str) -> list[dict]:
    """Validate and execute one read-only SQL query against Neon."""

    # Validate the query before opening a database connection.
    safe_sql = validate_sql(sql)

    database_url = os.getenv("NEON_DATABASE_URL")

    if not database_url:
        raise RuntimeError(
            "NEON_DATABASE_URL is missing from the root .env file."
        )

    with psycopg2.connect(
        database_url,
        connect_timeout=10,
        cursor_factory=RealDictCursor,
    ) as connection:
        connection.set_session(
            readonly=True,
            autocommit=False,
        )

        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '15s'")
            cursor.execute("SET LOCAL TIME ZONE 'UTC'")
            cursor.execute(safe_sql)

            if cursor.description is None:
                raise ValueError("The query did not return a result set.")

            return [dict(row) for row in cursor.fetchall()]
