import unittest

from src.rag.schema_context import SCHEMA_CONTEXT
from src.rag.sql_validator import validate_sql


class TestSQLValidator(unittest.TestCase):

    def test_rejects_drop(self):
        with self.assertRaises(ValueError):
            validate_sql("DROP TABLE public.fact_air_quality")

    def test_rejects_unapproved_table(self):
        with self.assertRaises(ValueError):
            validate_sql("SELECT * FROM public.users")

    def test_rejects_multiple_statements(self):
        with self.assertRaises(ValueError):
            validate_sql("SELECT 1; DROP TABLE public.fact_air_quality")

    def test_allows_count(self):
        validate_sql(
            "SELECT COUNT(*) AS total_records FROM public.fact_air_quality"
        )

    def test_rejects_information_schema(self):
        with self.assertRaises(ValueError):
            validate_sql("SELECT * FROM information_schema.tables")

    def test_rejects_values(self):
        with self.assertRaises(ValueError):
            validate_sql("VALUES (1)")

    def test_allows_simple_cte(self):
        validate_sql("WITH x AS (SELECT 1) SELECT * FROM x")

    def test_rejects_pg_tables_cte_shadow(self):
        with self.assertRaises(ValueError):
            validate_sql(
                "WITH pg_tables AS (SELECT * FROM pg_tables) "
                "SELECT * FROM pg_tables"
            )

    def test_rejects_pg_roles_cte_shadow(self):
        with self.assertRaises(ValueError):
            validate_sql(
                "WITH pg_roles AS (SELECT rolname FROM pg_roles) "
                "SELECT * FROM pg_roles"
            )

    def test_allows_chained_ctes(self):
        validate_sql(
            "WITH a AS (SELECT 1 AS n), b AS (SELECT n FROM a) "
            "SELECT * FROM b"
        )

    def test_allows_dim_date_cte_name(self):
        validate_sql(
            "WITH dim_date AS (SELECT * FROM dim_date) "
            "SELECT * FROM dim_date"
        )

    def test_allows_realistic_join_and_group(self):
        validate_sql(
            "SELECT d.month, AVG(f.pm25) AS average_pm25 "
            "FROM public.fact_air_quality AS f "
            "JOIN public.dim_date AS d ON f.date_key = d.date_key "
            "GROUP BY d.month"
        )

    def test_rejects_pg_sleep(self):
        with self.assertRaises(ValueError):
            validate_sql("SELECT pg_sleep(1)")

    def test_rejects_set_config(self):
        with self.assertRaises(ValueError):
            validate_sql("SELECT set_config('a','b',false)")

    def test_allows_supported_functions_and_schema_example(self):
        for function in (
            "AVG(pm25)", "COUNT(*)", "SUM(pm25)", "MIN(pm25)",
            "MAX(pm25)", "ROUND(AVG(pm25), 2)",
            "COALESCE(pm25, 0)", "CAST(pm25 AS INTEGER)",
            "EXTRACT(MONTH FROM reading_timestamp)",
            "DATE_TRUNC('month', reading_timestamp)",
        ):
            with self.subTest(function=function):
                validate_sql(
                    f"SELECT {function} FROM public.fact_air_quality"
                )

        example = SCHEMA_CONTEXT.split("SELECT\n", 1)[1].split("\n\"\"\"", 1)[0]
        validate_sql("SELECT\n" + example)


if __name__ == "__main__":
    unittest.main()
