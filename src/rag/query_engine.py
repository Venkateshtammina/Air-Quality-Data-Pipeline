
from decimal import Decimal
import calendar

from src.rag.database import execute_read_only_query
from src.rag.sql_generator import (
    generate_sql,
    generate_natural_language_answer,
)


def ask_database(question: str) -> dict:
    """Answer a natural-language question using the Neon database."""

    if not isinstance(question, str) or not question.strip():
        raise ValueError("Question must not be empty.")

    sql = generate_sql(question)

    # Stop before database execution if the question is unsupported.
    if sql.strip().rstrip(";").upper() == "UNSUPPORTED_QUERY":
        return {
            "question": question,
            "sql": None,
            "results": [],
            "formatted_results": (
                "I can't answer this question using the available "
                "air-quality database."
            ),
        }

    # Execute supported SQL against the database.
    results = execute_read_only_query(sql)

    # Handle empty results.
    if not results:
        formatted_results = "No results found."

    # Handle results containing only NULL values.
    elif all(
        value is None
        for row in results
        for value in row.values()
    ):
        formatted_results = (
            "I couldn't find an answer to this question "
            "in the available database results."
        )

    else:
        # Prepare a deterministic answer from database results.
        formatted_results = format_results(results)

        # Ask Gemini to explain the results.
        # If Gemini is unavailable, retain the database answer.
        try:
            natural_answer = generate_natural_language_answer(
                question,
                results,
            )

            formatted_results = (
                f"{natural_answer}\n\n"
                f"Database results:\n"
                f"{format_results(results)}"
            )

        except Exception as exc:
            print(
                f"Answer generation unavailable "
                f"({type(exc).__name__}). "
                "Using database results instead."
            )

    return {
        "question": question,
        "sql": sql,
        "results": results,
        "formatted_results": formatted_results,
    }


def format_results(results: list[dict]) -> str:
    """Convert query results into readable text."""

    if not results:
        return "No results found."

    labels = {
        "month": "Month",
        "average_pm25": "Average PM2.5",
        "total_records": "Total records",
    }

    lines = []

    for row in results:
        formatted_values = []

        for key, value in row.items():
            # Convert numeric month values to month names.
            if (
                key == "month"
                and isinstance(value, int)
                and 1 <= value <= 12
            ):
                value = calendar.month_name[value]

            # Format floating-point and Decimal values.
            if isinstance(value, (float, Decimal)):
                value = f"{value:.2f}"

            # Make column names easier to read.
            label = labels.get(
                key,
                key.replace("_", " ").capitalize(),
            )

            formatted_values.append(f"{label}: {value}")

        lines.append(", ".join(formatted_values))

    return "\n".join(lines)
