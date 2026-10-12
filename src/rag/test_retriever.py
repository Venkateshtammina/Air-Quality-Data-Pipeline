
import unittest
from unittest.mock import patch

import numpy as np

from src.rag import retriever


class TestRetriever(unittest.TestCase):

    @patch("src.rag.retriever.cosine_similarity")
    @patch("src.rag.retriever._index_knowledge")
    @patch("src.rag.retriever._load_model")
    def test_returns_most_relevant_entries(
        self,
        mock_load_model,
        mock_index_knowledge,
        mock_cosine_similarity,
    ):
        class FakeModel:
            def encode(self, texts, **kwargs):
                return np.array([[1.0, 0.0]])

        mock_load_model.return_value = FakeModel()

        mock_index_knowledge.return_value = np.zeros(
            (len(retriever.KNOWLEDGE_BASE), 2)
        )

        scores = np.zeros(len(retriever.KNOWLEDGE_BASE))
        scores[5] = 0.95
        scores[4] = 0.85
        scores[0] = 0.20

        mock_cosine_similarity.return_value = np.array([scores])

        results = retriever.retrieve_context(
            "Which month had the highest average PM2.5?",
            top_k=2,
        )

        self.assertEqual(len(results), 2)
        self.assertEqual(
            [item["id"] for item in results],
            ["monthly_ranking", "pm25_average"],
        )
        self.assertEqual(results[0]["score"], 0.95)

    def test_rejects_empty_question(self):
        with self.assertRaises(ValueError):
            retriever.retrieve_context("   ")

    def test_rejects_invalid_top_k(self):
        for invalid_value in (0, -1, True, 1.5, "3"):
            with self.subTest(top_k=invalid_value):
                with self.assertRaises(ValueError):
                    retriever.retrieve_context(
                        "Find average PM2.5",
                        top_k=invalid_value,
                    )


if __name__ == "__main__":
    unittest.main()
