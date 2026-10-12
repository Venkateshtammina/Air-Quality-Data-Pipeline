
from src.rag.sql_validator import validate_sql


tests = [
    ("DROP TABLE public.fact_air_quality", False),
    ("SELECT * FROM public.users", False),
    ("SELECT 1; DROP TABLE public.fact_air_quality", False),
    (
        "SELECT COUNT(*) AS total_records FROM public.fact_air_quality",
        True,
    ),
    ("SELECT * FROM information_schema.tables", False),
    ("VALUES (1)", False),
    ("WITH x AS (SELECT 1) SELECT * FROM x", True),
]

for sql, should_pass in tests:
    try:
        validate_sql(sql)
        actual_pass = True
    except ValueError:
        actual_pass = False

    if actual_pass == should_pass:
        print(f"PASS: {sql}")
    else:
        print(f"FAIL: {sql}")
