import os
import json
import time
from typing import Literal, Optional

import snowflake.connector
from google import genai
from google.genai import types
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

MODEL = os.getenv("GEMINI_MODEL", "gemini-3.8-flash")

SAMPLE_N = 5
TOPICS = ["food quality", "delivery", "pricing", "service", "packaging", "other"]
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


class ReviewLabels(BaseModel):
    sentiment_label: Literal["positive", "negative", "neutral"]
    sentiment_score: float
    topic: Literal["food quality", "delivery", "pricing", "service", "packaging", "other"]
    key_issue: Optional[str]


SYSTEM_PROMPT = f"""
You classify customer reviews for a food delivery app.

For the review you are given, return:
- sentiment_label: positive, negative, or neutral
- sentiment_score: a number between -1.0 and 1.0
- topic: one of {TOPICS}
- key_issue: a short phrase of 6 words or less that describes the main issue in the review, if any. If there is no issue, return null
"""


def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def create_output_table(cursor):
    cursor.execute("CREATE SCHEMA IF NOT EXISTS ZOMATO.AI")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ZOMATO.AI.REVIEW_ENRICHED (
            REVIEW_ID STRING,
            SENTIMENT_LABEL STRING,
            SENTIMENT_SCORE FLOAT,
            TOPIC STRING,
            KEY_ISSUE STRING,
            MODEL STRING,
            ENRICHED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)


def get_reviews_to_enrich(cursor):
    cursor.execute(f"""
        SELECT REVIEW_ID, COMMENT
        FROM ZOMATO.RAW.REVIEWS
        WHERE REVIEW_ID NOT IN (SELECT REVIEW_ID FROM ZOMATO.AI.REVIEW_ENRICHED)
          AND COMMENT IS NOT NULL
        LIMIT {SAMPLE_N}
    """)
    return cursor.fetchall()


FALLBACK_MODELS = [m for m in os.getenv("GEMINI_FALLBACKS", "gemini-3.8-flash-lite").split(",") if m]


def classify_review(comment):
    """Try the primary model with backoff, then fall back to other models on 503/429."""
    last_err = None
    for model in [MODEL] + FALLBACK_MODELS:
        try:
            return _call_model(model, comment)
        except Exception as e:
            last_err = e
            if "503" in str(e) or "429" in str(e) or "404" in str(e):
                print(f"{model} unavailable, trying next model...")
                continue
            raise
    raise last_err


def _call_model(model, comment, retries=5):
    for attempt in range(retries):
        try:
            response = client.models.generate_content(
                model=model,
                contents=comment,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    temperature=0,
                    response_mime_type="application/json",
                    response_schema=ReviewLabels,
                ),
            )
            labels = json.loads(response.text)
            labels["_model"] = model
            return labels
        except Exception as e:
            # Back off on rate limits (429) / transient errors
            if attempt < retries - 1 and ("429" in str(e) or "503" in str(e)):
                wait = 2 ** (attempt + 2)
                print(f"Rate limited, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def save_results(cursor, results):
    """Insert all the enriched rows into Snowflake in one go."""
    print(f"Saving {len(results)} enriched reviews to Snowflake...")
    cursor.executemany(
        """
        INSERT INTO ZOMATO.AI.REVIEW_ENRICHED
            (review_id, sentiment_label, sentiment_score, topic, key_issue, model)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        results,
    )


def main():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        create_output_table(cursor)
        reviews = get_reviews_to_enrich(cursor)

        if len(reviews) == 0:
            print("No new reviews to enrich.")
            return

        print(f"Enriching {len(reviews)} reviews...")

        results = []
        for review_id, comment in reviews:
            print(f"Classifying review {review_id}: {comment}")
            try:
                labels = classify_review(comment)
                print(f"Labels for review {review_id}: {labels}")
                results.append((
                    review_id,
                    labels["sentiment_label"],
                    labels["sentiment_score"],
                    labels["topic"],
                    labels.get("key_issue"),
                    labels["_model"],
                ))
            except Exception as e:
                print(f"Error occurred while classifying review {review_id}: {e}")
            time.sleep(1)  # stay under free-tier RPM

        if results:
            save_results(cursor, results)
            conn.commit()
            print(f"Saved {len(results)} enriched reviews to Snowflake.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()