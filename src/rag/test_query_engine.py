
import unittest
from unittest.mock import patch

from src.rag.query_engine import ask_database


class TestQueryEngine(unittest.TestCase):

    @patch("src.rag.query_engine.generate_natural_language_answer")
    @patch("src.rag.query_engine.execute_read_only_query")
    @patch("src.rag.query_engine.generate_sql")
    def test_average_pm25(
        self,
        mock_generate_sql,
        mock_execute_query,
        mock_generate_answer,
    ):
        mock_generate_sql.return_value = (
            "SELECT AVG(pm25) AS average_pm25 "
            "FROM public.fact_air_quality"
        )
        mock_execute_query.return_value = [
            {"average_pm25": 38.266342131894}
        ]
        mock_generate_answer.return_value = (
            "The average PM2.5 level was 38.27."
        )

        result = ask_database(
            "What is the average PM2.5 level in 2025?"
        )

        self.assertEqual(
            result["results"][0]["average_pm25"],
            38.266342131894,
        )
        self.assertIn("38.27", result["formatted_results"])
        mock_execute_query.assert_called_once()

    @patch("src.rag.query_engine.generate_natural_language_answer")
    @patch("src.rag.query_engine.execute_read_only_query")
    @patch("src.rag.query_engine.generate_sql")
    def test_record_count(
        self,
        mock_generate_sql,
        mock_execute_query,
        mock_generate_answer,
    ):
        mock_generate_sql.return_value = (
            "SELECT COUNT(*) AS total_records "
            "FROM public.fact_air_quality"
        )
        mock_execute_query.return_value = [
            {"total_records": 35040}
        ]
        mock_generate_answer.return_value = (
            "There were 35,040 records."
        )

        result = ask_database(
            "How many air-quality records were recorded in 2025?"
        )

        self.assertEqual(
            result["results"][0]["total_records"],
            35040,
        )
        self.assertIn("35,040", result["formatted_results"])
        mock_execute_query.assert_called_once()

    @patch("src.rag.query_engine.execute_read_only_query")
    @patch("src.rag.query_engine.generate_sql")
    def test_unsupported_question(
        self,
        mock_generate_sql,
        mock_execute_query,
    ):
        mock_generate_sql.return_value = "UNSUPPORTED_QUERY"

        result = ask_database(
            "What is the population of Bengaluru?"
        )

        self.assertIsNone(result["sql"])
        self.assertEqual(result["results"], [])
        self.assertIn(
            "can't answer",
            result["formatted_results"],
        )
        mock_execute_query.assert_not_called()


    @patch("src.rag.query_engine.generate_natural_language_answer")
    @patch("src.rag.query_engine.execute_read_only_query")
    @patch("src.rag.query_engine.generate_sql")
    def test_gemini_failure_uses_database_results(
        self,
        mock_generate_sql,
        mock_execute_query,
        mock_generate_answer,
    ):
        mock_generate_sql.return_value = (
            "SELECT AVG(pm25) AS average_pm25 "
            "FROM public.fact_air_quality"
        )
        mock_execute_query.return_value = [
            {"average_pm25": 38.266342131894}
        ]
        mock_generate_answer.side_effect = RuntimeError(
            "Simulated Gemini outage"
        )

        result = ask_database(
            "What is the average PM2.5 level in 2025?"
        )

        self.assertIn(
            "Average PM2.5: 38.27",
            result["formatted_results"],
        )


    @patch("src.rag.query_engine.generate_natural_language_answer")
    @patch("src.rag.query_engine.execute_read_only_query")
    @patch("src.rag.query_engine.generate_sql")
    def test_empty_results(
        self,
        mock_generate_sql,
        mock_execute_query,
        mock_generate_answer,
    ):
        mock_generate_sql.return_value = (
            "SELECT AVG(pm25) AS average_pm25 "
            "FROM public.fact_air_quality"
        )
        mock_execute_query.return_value = []

        result = ask_database(
            "What is the average PM2.5 level in 2025?"
        )

        self.assertEqual(
            result["formatted_results"],
            "No results found.",
        )
        mock_generate_answer.assert_not_called()


    @patch("src.rag.query_engine.generate_natural_language_answer")
    @patch("src.rag.query_engine.execute_read_only_query")
    @patch("src.rag.query_engine.generate_sql")
    def test_null_only_results(
        self,
        mock_generate_sql,
        mock_execute_query,
        mock_generate_answer,
    ):
        mock_generate_sql.return_value = (
            "SELECT AVG(pm25) AS average_pm25 "
            "FROM public.fact_air_quality"
        )
        mock_execute_query.return_value = [
            {"average_pm25": None}
        ]

        result = ask_database(
            "What is the average PM2.5 level in 2025?"
        )

        self.assertIn(
            "couldn't find an answer",
            result["formatted_results"],
        )
        mock_generate_answer.assert_not_called()




if __name__ == "__main__":
    unittest.main(verbosity=2)
