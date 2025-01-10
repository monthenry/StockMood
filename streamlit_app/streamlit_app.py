import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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

# MongoDB Data Fetching
def fetch_mongo_data(limit=30000):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    data = list(collection.find({}, {"_id": 0}).limit(limit))  # Limit results
    return pd.DataFrame(data)  # Convert to DataFrame

# Redis Data Fetching
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
st.title("Sentiment and Market Data Dashboard")

# Fetch and Process MongoDB Data
mongo_data = fetch_mongo_data()
if not mongo_data.empty and {"date", "relative_sentiment", "symbol"}.issubset(mongo_data.columns):
    mongo_data["date"] = pd.to_datetime(mongo_data["date"])
    grouped_mongo = (
        mongo_data.groupby([mongo_data["date"].dt.date, "symbol"])
        .agg({"relative_sentiment": "mean"})
        .rename(columns={"relative_sentiment": "avg_sentiment"})
        .reset_index()
    )
else:
    grouped_mongo = pd.DataFrame()

# Fetch and Process Redis Data
redis_data = fetch_redis_data()
if not redis_data.empty and {"date", "close", "symbol"}.issubset(redis_data.columns):
    redis_data["date"] = pd.to_datetime(redis_data["date"]).dt.date
else:
    redis_data = pd.DataFrame()

# Merge Sentiment and Market Data
if not grouped_mongo.empty and not redis_data.empty:
    combined_data = pd.merge(
        grouped_mongo,
        redis_data,
        on=["date", "symbol"],
        how="inner"  # Keep only matching rows
    )
else:
    combined_data = pd.DataFrame()

if not combined_data.empty:
    # Dropdown for selecting company
    companies = combined_data["symbol"].unique()
    selected_company = st.selectbox("Select a Company:", options=companies)

    # Filter by selected company
    filtered_data = combined_data[combined_data["symbol"] == selected_company]

    # Time Frame Selector
    min_date, max_date = filtered_data["date"].min(), filtered_data["date"].max()
    start_date, end_date = st.slider(
        "Select Date Range:",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
    )

    # Filter by timeframe
    filtered_data = filtered_data[
        (filtered_data["date"] >= start_date) & (filtered_data["date"] <= end_date)
    ]

    # Create dual-axis plot
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add sentiment data (primary y-axis)
    fig.add_trace(
        go.Scatter(
            x=filtered_data["date"],
            y=filtered_data["avg_sentiment"],
            name="Average Sentiment",
            line=dict(color="blue"),
        ),
        secondary_y=False,
    )

    # Add market data (secondary y-axis)
    fig.add_trace(
        go.Scatter(
            x=filtered_data["date"],
            y=filtered_data["close"],
            name="Close Price",
            line=dict(color="green"),
        ),
        secondary_y=True,
    )

    # Update layout
    fig.update_layout(
        title=f"Sentiment and Market Data for {selected_company}",
        xaxis_title="Date",
        yaxis_title="Average Sentiment",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    # Update secondary y-axis
    fig.update_yaxes(title_text="Average Sentiment", secondary_y=False)
    fig.update_yaxes(title_text="Close Price", secondary_y=True)

    # Display the plot
    st.plotly_chart(fig)
else:
    st.write("No combined data available to display.")
