import os
import time

import numpy as np
import pandas as pd
import streamlit as st
import snowflake.connector
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-001")
CHAT_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.8-flash")
FALLBACK_MODELS = [m for m in os.getenv("GEMINI_FALLBACKS", "gemini-3.8-flash-lite").split(",") if m]
NEW_REVIEWS = 500
TOP_K = 5
EMBED_BATCH = 100  # Gemini max items per embed request
# Model name in the cache file so old OpenAI vectors are never reused
CACHE_FILE = f"review_embeddings_{EMBEDDING_MODEL}.parquet"

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


def with_retry(fn, retries=5):
    """Retry on Gemini rate limits (429) / overload (503) with exponential backoff."""
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            if attempt < retries - 1 and ("429" in str(e) or "503" in str(e)):
                time.sleep(2 ** (attempt + 2))
            else:
                raise


def read_reviews_from_snowflake():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    try:
        query = f"""
            SELECT REVIEW_ID, CITY, RATING, COMMENT
            FROM ZOMATO.STAGING.STG_REVIEWS SAMPLE ({NEW_REVIEWS} ROWS)
            WHERE COMMENT IS NOT NULL
        """
        df = conn.cursor().execute(query).fetch_pandas_all()
    finally:
        conn.close()

    df.columns = [col.lower() for col in df.columns]
    return df


def embed(texts, task_type):
    """Embed texts in batches. task_type: RETRIEVAL_DOCUMENT for reviews, RETRIEVAL_QUERY for questions."""
    vectors = []
    for i in range(0, len(texts), EMBED_BATCH):
        batch = texts[i:i + EMBED_BATCH]
        result = with_retry(lambda: client.models.embed_content(
            model=EMBEDDING_MODEL,
            contents=batch,
            config=types.EmbedContentConfig(task_type=task_type),
        ))
        vectors.extend(e.values for e in result.embeddings)
    return vectors


@st.cache_data()
def load_reviews():
    if os.path.exists(CACHE_FILE):
        return pd.read_parquet(CACHE_FILE)

    df = read_reviews_from_snowflake()
    df["embedding"] = embed(df["comment"].tolist(), "RETRIEVAL_DOCUMENT")
    df.to_parquet(CACHE_FILE)
    return df


def find_similar_reviews(question, df):
    q = np.array(embed([question], "RETRIEVAL_QUERY")[0])
    m = np.vstack(df["embedding"].to_numpy())
    scores = (m @ q) / (np.linalg.norm(m, axis=1) * np.linalg.norm(q))

    df = df.copy()
    df["score"] = scores
    return df.nlargest(TOP_K, "score")


def ask_llm(question, top_reviews):
    context = ""
    for _, row in top_reviews.iterrows():
        context += f"- ({row['city']}, {row['rating']} stars) {row['comment']}\n"

    system_prompt = (
        "Answer ONLY using the customer reviews provided. "
        "Be concise. If the reviews don't cover it, say so."
    )
    user_prompt = f"Question: {question}\n\nReviews:\n{context}"

    last_err = None
    for model in [CHAT_MODEL] + FALLBACK_MODELS:
        try:
            response = with_retry(lambda: client.models.generate_content(
                model=model,
                contents=user_prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0.2,
                ),
            ))
            return response.text
        except Exception as e:
            last_err = e
            if any(code in str(e) for code in ("503", "429", "404")):
                continue
            raise
    raise last_err


st.title("Chat with your Zomato Reviews")
st.caption(f"Searching {NEW_REVIEWS} reviews, answering with {CHAT_MODEL}")

review_df = load_reviews()

question = st.text_input(
    "Ask a question about your reviews:",
    placeholder="e.g. What are the most common complaints about delivery?",
)

if question:
    with st.spinner("Thinking..."):
        top_reviews = find_similar_reviews(question, review_df)
        answer = ask_llm(question, top_reviews)

    st.markdown("**Answer:**")
    st.write(answer)

    with st.expander("Reviews used to build this answer"):
        st.dataframe(top_reviews[["city", "rating", "comment", "score"]], hide_index=True)