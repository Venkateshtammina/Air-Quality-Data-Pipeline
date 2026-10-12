
from functools import lru_cache

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


# Small local embedding model; the first run downloads it.
EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# Small knowledge base for the air-quality text-to-SQL assistant.
KNOWLEDGE_BASE = [
    {
        "id": "fact_table",
        "text": (
            "The public.fact_air_quality table contains air-quality "
            "measurements including PM2.5, PM10, temperature, pressure, "
            "humidity, wind, rainfall, and gases. It connects to date, "
            "time, and station dimensions using foreign keys."
        ),
    },
    {
        "id": "date_table",
        "text": (
            "The public.dim_date table contains full_date, day, month, "
            "year, weekday_name, and is_weekend. Join it to "
            "fact_air_quality using date_key to filter or group readings "
            "by date, month, year, weekday, or weekend."
        ),
    },
    {
        "id": "time_table",
        "text": (
            "The public.dim_time table contains hour and hour_label. "
            "Join it to fact_air_quality using time_key to analyze "
            "air-quality measurements by hour of the day."
        ),
    },
    {
        "id": "station_table",
        "text": (
            "The public.dim_station table contains station_id, "
            "station_name, city, and state. Join it to fact_air_quality "
            "using station_key to filter or group readings by station "
            "or location."
        ),
    },
    {
        "id": "pm25_average",
        "text": (
            "To calculate average PM2.5, use AVG(fact_air_quality.pm25) "
            "and exclude rows where pm25 IS NULL. For yearly averages, "
            "join dim_date and filter by year. Give the result a clear "
            "alias such as average_pm25."
        ),
    },
    {
        "id": "monthly_ranking",
        "text": (
            "To find the month with the highest or lowest average "
            "pollution measurement, join fact_air_quality to dim_date, "
            "filter the required year, exclude null measurements, group "
            "by month, order by the average descending or ascending, "
            "and use LIMIT 1. Select both month and the average value."
        ),
    },
    {
        "id": "record_counts",
        "text": (
            "To count air-quality readings, use COUNT(*) on "
            "public.fact_air_quality. To count readings for a particular "
            "year, join dim_date and filter by year."
        ),
    },
    {
        "id": "joins",
        "text": (
            "Join fact_air_quality to dim_date on date_key, to dim_time "
            "on time_key, and to dim_station on station_key. Use explicit "
            "JOIN conditions and only the approved public tables."
        ),
    },
]


@lru_cache(maxsize=1)
def _load_model():
    """Load the embedding model once and reuse it."""
    return SentenceTransformer(EMBEDDING_MODEL_NAME, device="cpu")


@lru_cache(maxsize=1)
def _index_knowledge():
    """Create and cache embeddings for the knowledge base."""
    model = _load_model()
    documents = [item["text"] for item in KNOWLEDGE_BASE]

    embeddings = model.encode(
        documents,
        normalize_embeddings=True,
        show_progress_bar=False,
    )

    return embeddings


def retrieve_context(
    question: str,
    top_k: int = 3,
) -> list[dict]:
    """Retrieve the most semantically relevant knowledge entries."""

    if not isinstance(question, str) or not question.strip():
        raise ValueError("Question must not be empty.")

    if not isinstance(top_k, int) or isinstance(top_k, bool) or top_k < 1:
        raise ValueError("top_k must be a positive integer.")

    model = _load_model()
    knowledge_embeddings = _index_knowledge()

    question_embedding = model.encode(
        [question.strip()],
        normalize_embeddings=True,
        show_progress_bar=False,
    )

    scores = cosine_similarity(
        question_embedding,
        knowledge_embeddings,
    )[0]

    ranked_indices = scores.argsort()[::-1][:top_k]

    return [
        {
            "id": KNOWLEDGE_BASE[index]["id"],
            "text": KNOWLEDGE_BASE[index]["text"],
            "score": round(float(scores[index]), 4),
        }
        for index in ranked_indices
    ]
