import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
import redis
import json

# MongoDB Configuration
MONGO_URI = "mongodb://root:example@mongo:27017/"
DB_NAME = "bloomberg_db"
COLLECTION_NAME = "sentiment_articles"

# Redis Configuration
REDIS_HOST = 'redis'
REDIS_PORT = 6379

# Test MongoDB Connection
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
except Exception as e:
    st.error(f"Failed to connect to MongoDB: {e}")

# Test Redis Connection
try:
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
except Exception as e:
    st.error(f"Failed to connect to Redis: {e}")

# MongoDB Connection and Data Fetching
def fetch_mongo_data(limit=30000):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    data = list(collection.find({}, {"_id": 0}).limit(limit))  # Limit results
    return pd.DataFrame(data)  # Convert to DataFrame

# Redis Connection and Data Fetching
def fetch_redis_data():
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    keys = r.keys("finance_data:*")
    data = []
    for key in keys:
        entries = r.hgetall(key)
        for date, values in entries.items():
            parsed_values = json.loads(values)
            parsed_values.update({"symbol": key.replace("finance_data:", ""), "date": date})
            data.append(parsed_values)
    return pd.DataFrame(data)  # Convert to DataFrame

# Streamlit App
st.title("Interactive Dashboard")

# MongoDB Data - Sentiment Articles
st.header("Sentiment Over Time (MongoDB)")
mongo_data = fetch_mongo_data()
if not mongo_data.empty:  # Ensure DataFrame is not empty
    # Ensure required columns exist
    if {"date", "relative_sentiment", "symbol"}.issubset(mongo_data.columns):  # Ensure `symbol` exists
        # Convert `date` to datetime
        mongo_data["date"] = pd.to_datetime(mongo_data["date"])

        # Group by symbol and date, then compute the average sentiment
        grouped_mongo = (
            mongo_data.groupby([mongo_data["date"].dt.date, "symbol"])
            .agg({"relative_sentiment": "mean"})
            .rename(columns={"relative_sentiment": "avg_sentiment"})
            .reset_index()  # Reset index to make 'date' and 'symbol' columns
        )

        # Time Frame Selector
        min_date, max_date = grouped_mongo["date"].min(), grouped_mongo["date"].max()
        start_date, end_date = st.slider(
            "Select Date Range for Sentiment Data:",
            min_value=min_date,  # Already in date format
            max_value=max_date,  # Already in date format
            value=(min_date, max_date),  # Default slider range
        )

        # Filter grouped data by the selected time frame
        filtered_mongo = grouped_mongo[(grouped_mongo["date"] >= start_date) & (grouped_mongo["date"] <= end_date)]

        # Plot using Plotly
        fig_sentiment = px.line(
            filtered_mongo,
            x="date",
            y="avg_sentiment",  # Use the average sentiment
            color="symbol",     # Group by symbol
            title="Average Sentiment Over Time by Company",
            labels={"date": "Date", "avg_sentiment": "Average Sentiment", "symbol": "Company"},
        )
        st.plotly_chart(fig_sentiment)
    else:
        st.write("Sentiment data does not have required fields (`date`, `relative_sentiment`, and `symbol`).")
else:
    st.write("No sentiment data found in MongoDB.")

# Redis Data - Financial Data
st.header("Financial Data (Redis)")
redis_data = fetch_redis_data()
if not redis_data.empty:  # Ensure DataFrame is not empty
    # Convert `date` to datetime64 without time zone information
    redis_data["date"] = pd.to_datetime(redis_data["date"]).dt.date  # Convert to `datetime.date`
    redis_data = redis_data.sort_values(by="date")  # Sort by date

    # Time Frame Selector
    min_date, max_date = redis_data["date"].min(), redis_data["date"].max()
    start_date, end_date = st.slider(
        "Select Date Range for Financial Data:",
        min_value=min_date,  # Already datetime.date
        max_value=max_date,  # Already datetime.date
        value=(min_date, max_date),
        key="financial_slider",
    )

    # Filter data by selected time frame
    filtered_redis = redis_data[(redis_data["date"] >= start_date) & (redis_data["date"] <= end_date)]

    # Plot using Plotly
    fig_financial = px.line(
        filtered_redis,
        x="date",
        y="close",
        color="symbol",
        title="Financial Data Over Time",
        labels={"date": "Date", "close": "Close Price", "symbol": "Company"},
    )
    st.plotly_chart(fig_financial)
else:
    st.write("No financial data found in Redis.")