import os
import re
import json
import time

import pandas as pd
import streamlit as st
import snowflake.connector
from google import genai
from google.genai import types
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

MODEL = os.getenv("GEMINI_MODEL", "gemini-3.8-flash")
FALLBACK_MODELS = [m for m in os.getenv("GEMINI_FALLBACKS", "gemini-3.8-flash-lite").split(",") if m]

FORBIDDEN_WORDS = ['drop', 'delete', 'truncate', 'alter', 'update', 'insert', 'create',
                   'replace', 'grant', 'revoke', 'merge', 'call', 'execute', 'put', 'copy']

EXAMPLE_QUESTIONS = [
    "Top 10 cities by GMV",
    "Which cuisine has the most orders?",
    "Average delivery time by city, worst first",
    "Cancel rate by payment method",
]

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

SCHEMA = """
Tables available (Snowflake). Use bare table names, no database or schema prefix.

FCT_ORDERS(order_id, order_date, customer_id, restaurant_id, city, cuisine,
           payment_method, order_status, is_delivered, sales_amount, discount,
           delivery_fee, gst, customer_rating, delivery_time_min)
DIM_RESTAURANTS(restaurant_id, restaurant_name, city, cuisine, rating, rating_count, cost_for_two)
DIM_CUSTOMERS(customer_id, customer_name, age, age_segment, gender, marital_status,
              occupation, income_band, education, family_size)
MART_DAILY_CITY_REVENUE(order_date, city, orders, delivered_orders, cancel_rate, gmv, aov)
MART_RESTAURANT_PERFORMANCE(restaurant_id, restaurant_name, city, cuisine,
                            orders, revenue, avg_customer_rating, avg_delivery_min)
MART_DELIVERY_SLA(city, order_hour, delivered_orders, p50, p90)   -- p50/p90 = delivery minutes

Note: gmv means delivered revenue. Prefer the MART_ tables when they fit the question.
"""

SYSTEM_PROMPT = f"""
You are a Snowflake SQL expert. Write ONE SELECT query that answers the question.

Rules:
- SELECT queries only, never modify data.
- Use bare table names (FCT_ORDERS, not ZOMATO.MARTS.FCT_ORDERS).
- Add a LIMIT of 100 or less, unless the question asks for a single total.
- Return the query in the "sql" field.

{SCHEMA}
"""


class SQLResponse(BaseModel):
    sql: str


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="MARTS",
        role="DBT_ROLE",
        enable_server_session_keep_alive_auto_detection=False,
    )


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


def call_gemini(question):
    last_err = None
    for model in [MODEL] + FALLBACK_MODELS:
        try:
            return with_retry(lambda: client.models.generate_content(
                model=model,
                contents=question,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    temperature=0,
                    response_mime_type="application/json",
                    response_schema=SQLResponse,
                ),
            ))
        except Exception as e:
            last_err = e
            if any(code in str(e) for code in ("503", "429", "404")):
                continue
            raise
    raise last_err


def generate_sql(question):
    response = call_gemini(question)
    sql = json.loads(response.text)["sql"]

    sql = sql.replace("ZOMATO.MARTS.", "").replace("ZOMATO.", "")
    return sql.strip().rstrip(";").strip()


def is_safe(sql):
    lowered = sql.lower().strip()

    if not (lowered.startswith("select") or lowered.startswith("with")):
        return False

    # Block multiple statements
    if ";" in lowered:
        return False

    # Whole-word match so columns like "updated_at" don't trigger a false block
    for word in FORBIDDEN_WORDS:
        if re.search(rf"\b{word}\b", lowered):
            return False

    return True


def run_query(sql):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        return cursor.execute(sql).fetch_pandas_all()
    finally:
        cursor.close()


st.title("Chat with your Zomato Data")
st.caption(f"Ask in English, {MODEL} writes the SQL, Snowflake runs it")

with st.sidebar:
    st.header("Example Questions")
    for q in EXAMPLE_QUESTIONS:
        st.markdown(f" - {q}")

question = st.text_input("Enter your question here",
                         placeholder="e.g. Top 10 restaurants by revenue in Bangalore")

if question:
    with st.spinner("Writing SQL..."):
        try:
            sql = generate_sql(question)
        except Exception as e:
            st.error(f"Gemini error: {e}")
            st.stop()

    st.code(sql, language="sql")

    if not is_safe(sql):
        st.error("The generated SQL is not safe to run. Please modify your question.")
    else:
        try:
            df = run_query(sql)
            st.success(f"{len(df)} rows returned")
            st.dataframe(df, hide_index=True)

            if len(df.columns) == 2 and pd.api.types.is_numeric_dtype(df.iloc[:, 1]):
                st.bar_chart(df, x=df.columns[0], y=df.columns[1])

        except Exception as e:
            st.error(f"Error running query: {e}")