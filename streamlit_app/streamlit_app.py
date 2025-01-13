import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pymongo import MongoClient
import redis
import json
import plotly.graph_objects as go

# Configuration
MONGO_URI = "mongodb://root:example@mongo:27017/"
DB_NAME = "bloomberg_db"
COLLECTION_NAME = "sentiment_articles"
REDIS_HOST = 'redis'
REDIS_PORT = 6379
POSTGRES_URI = 'postgresql://airflow:airflow@postgres:5432/airflow'

# Data Fetching Functions
def fetch_mongo_data(limit=30000):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    data = list(collection.find({}, {"_id": 0}).limit(limit))
    return pd.DataFrame(data)

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
    return pd.DataFrame(data)

def fetch_postgres_data(symbol):
    conn = psycopg2.connect(POSTGRES_URI)
    query = """
    SELECT date, avg_sentiment, close
    FROM financial_sentiment
    WHERE symbol = %s
    ORDER BY date
    """
    df = pd.read_sql(query, conn, params=(symbol,))
    conn.close()
    return df

# Plotting Functions
def plot_stock_and_sentiment(df_filtered):
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Average Sentiment', color='tab:blue')
    ax1.plot(df_filtered['date'], df_filtered['avg_sentiment'], color='tab:blue', label='Average Sentiment')
    ax1.tick_params(axis='y', labelcolor='tab:blue')

    ax2 = ax1.twinx()
    ax2.set_ylabel('Stock Price (Close)', color='tab:green')
    ax2.plot(df_filtered['date'], df_filtered['close'], color='tab:green', linestyle='--', label='Stock Price')
    ax2.tick_params(axis='y', labelcolor='tab:green')

    ax1.xaxis.set_major_locator(mdates.MonthLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)
    fig.tight_layout()
    st.pyplot(fig)

def plot_interactive_stock_evolution(filtered_data, articles):
    fig = go.Figure()
    for _, row in articles.iterrows():
        current_date = pd.to_datetime(row['date']).date()
        article_title = row['title']
        future_stocks = filtered_data[
            (filtered_data['date'] >= current_date) &
            (filtered_data['date'] <= current_date + pd.Timedelta(days=30))
        ]
        if not future_stocks.empty:
            future_stocks['days_since_article'] = (
                pd.to_datetime(future_stocks['date']) - pd.to_datetime(current_date)
            ).dt.days
            fig.add_trace(go.Scatter(
                x=future_stocks['days_since_article'],
                y=future_stocks['close'],
                mode='lines',
                name=f"Article: {article_title[:20]}...",
                text=[f"Title: {article_title}<br>Date: {current_date}<br>Close: {close}" for close in future_stocks['close']],
                line=dict(width=2, color='rgba(0, 100, 250, 0.5)')
            ))
    fig.update_layout(
        title="Interactive Stock Evolution After Positive Sentiment Articles",
        xaxis_title="Days After Article",
        yaxis_title="Close Price",
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)

# Streamlit UI
st.title('Financial Sentiment and Stock Price')
symbols = ['AAPL', 'GOOGL', 'MSFT']
symbol = st.selectbox('Select Company (Symbol)', symbols)

df = fetch_postgres_data(symbol)
df['date'] = pd.to_datetime(df['date']).dt.date

min_date, max_date = df['date'].min(), df['date'].max()
start_date, end_date = st.slider('Select Date Range', min_date, max_date, (min_date, max_date), format="YYYY-MM-DD")

df_filtered = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
st.subheader("Stock and Sentiment Correlation Over Time")
plot_stock_and_sentiment(df_filtered)

st.subheader("Stock Evolution After Positive Sentiment Articles")
min_sentiment = st.slider("Select Minimum Sentiment Threshold:", 0.65, 0.9, 0.75, 0.01)
mongo_data = fetch_mongo_data()
mongo_data["date"] = pd.to_datetime(mongo_data["date"])

if {"date", "relative_sentiment", "symbol"}.issubset(mongo_data.columns):
    grouped_mongo = mongo_data.groupby([mongo_data["date"].dt.date, "symbol"]).agg({"relative_sentiment": "mean"}).reset_index()
else:
    grouped_mongo = pd.DataFrame()

redis_data = fetch_redis_data()
if {"date", "close", "symbol"}.issubset(redis_data.columns):
    redis_data["date"] = pd.to_datetime(redis_data["date"]).dt.date
else:
    redis_data = pd.DataFrame()

combined_data = pd.merge(grouped_mongo, redis_data, on=["date", "symbol"], how="inner") if not grouped_mongo.empty and not redis_data.empty else pd.DataFrame()
filtered_data = combined_data[combined_data["symbol"] == symbol]
best_sentiment_articles = mongo_data[mongo_data["relative_sentiment"] >= min_sentiment]

plot_interactive_stock_evolution(filtered_data, best_sentiment_articles)
