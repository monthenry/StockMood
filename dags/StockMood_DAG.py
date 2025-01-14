import datetime
import os
import json
import csv
import re
from math import exp
import requests
import pandas as pd
import tarfile
from concurrent.futures import ThreadPoolExecutor
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pymongo import MongoClient
import redis
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import psycopg2
from sqlalchemy import create_engine
import logging

# MongoDB Configuration
MONGO_URI = "mongodb://root:example@mongo:27017/"
DB_NAME = "bloomberg_db"
COLLECTION_NAME = "sentiment_articles"

# Redis Configuration
REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_DB = 0

# InfluxDB Configuration
INFLUXDB_HOST = 'influxdb'
INFLUXDB_PORT = 8086
INFLUXDB_DB = 'financial_data'

# Set up your PostgreSQL connection string
POSTGRES_URI = 'postgresql://airflow:airflow@postgres:5432/airflow'

# Fonction pour vérifier la connexion Internet
def check_internet_connection():
    try:
        requests.get('https://www.google.com', timeout=5)
        return True
    except requests.ConnectionError:
        return False

# Fonction pour la branche : vérifie la connexion et décide quelle branche suivre
def branch_check_internet(output_folder):
    if check_internet_connection():
        return 'api_fetch_and_save_json'
    else:
        return 'local_fetch_and_save_json'

# Fonction pour récupérer des données d'une API et les enregistrer dans un fichier JSON
def api_fetch_and_save_json(output_folder):
    url = 'https://yfapi.net/v8/finance/spark?interval=1d&range=18y&symbols=AAPL%2CMSFT%2CGOOGL'
    headers = {
        'accept': 'application/json',
        'X-API-KEY': 'maDTTOTDX95rRJqe70YAx3uezGtScGqx3o2S2Iex'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        file_path = os.path.join(output_folder, 'yahoo_finance_data.json')
        with open(file_path, 'w') as fichier_json:
            json.dump(data, fichier_json, indent=4)
        print("Les données ont été enregistrées dans 'yahoo_finance_data.json'.")
    else:
        print(f"Échec de la récupération des données. Code statut HTTP : {response.status_code}")

# Fonction pour récupérer des données localement si pas de connexion internet
def local_fetch_and_save_json(output_folder):
    src_file_path = os.path.join('/opt/airflow/data/yahoo_finance_data_src/', 'yahoo_finance_data.json')
    dest_file_path = os.path.join(output_folder, 'yahoo_finance_data.json')

    if os.path.exists(src_file_path):
        with open(src_file_path, 'r') as src_file:
            data = json.load(src_file)
        
        with open(dest_file_path, 'w') as dest_file:
            json.dump(data, dest_file, indent=4)
        
        print(f"Les données ont été copiées de {src_file_path} à {dest_file_path}.")
    else:
        print(f"Fichier source {src_file_path} introuvable.")

def clean_json_data(output_folder):
    file_path = os.path.join(output_folder, 'yahoo_finance_data.json')

    if os.path.exists(file_path):
        with open(file_path, 'r') as fichier_json:
            data = json.load(fichier_json)

        for symbol, symbol_data in data.items():
            if 'timestamp' in symbol_data and 'close' in symbol_data:
                timestamps = symbol_data['timestamp']
                closes = symbol_data['close']

                # Create a DataFrame for more efficient processing
                df = pd.DataFrame({
                    'timestamp': timestamps,
                    'close': closes
                })

                # Convert timestamp to datetime and filter between 2007 and 2013
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                df = df[(df['timestamp'].dt.year >= 2010) & (df['timestamp'].dt.year <= 2013)]

                # Sort the DataFrame by timestamp (ascending order)
                df = df.sort_values(by='timestamp', ascending=True)

                # Update symbol data with filtered and sorted values
                symbol_data['timestamp'] = (df['timestamp'].astype(int) // 10**9).tolist()  # Convert to int and then to list
                symbol_data['close'] = df['close'].tolist()

        # Write cleaned and sorted data to a new JSON file
        cleaned_file_path = os.path.join(output_folder, 'yahoo_finance_data_cleaned.json')
        with open(cleaned_file_path, 'w') as fichier_json:
            json.dump(data, fichier_json, indent=4)

        print("Les données nettoyées et triées ont été enregistrées dans 'yahoo_finance_data_cleaned.json'.")
    else:
        print("Fichier JSON introuvable. Rien à nettoyer.")

def rearrange_json_data(output_folder):
    from datetime import datetime
    file_path = os.path.join(output_folder, 'yahoo_finance_data_cleaned.json')

    if os.path.exists(file_path):
        with open(file_path, 'r') as fichier_json:
            data = json.load(fichier_json)

        rearranged_data = {}

        for symbol, symbol_data in data.items():
            if 'timestamp' in symbol_data and 'close' in symbol_data:
                timestamps = symbol_data['timestamp']
                closes = symbol_data['close']

                # Calculer les daily changes et daily returns
                daily_changes = [closes[i] - closes[i - 1] if i > 0 else None for i in range(len(closes))]
                daily_returns = [
                    ((closes[i] - closes[i - 1]) / closes[i - 1]) * 100 if i > 0 and closes[i - 1] != 0 else None
                    for i in range(len(closes))
                ]

                # Réarranger les données dans le nouveau format
                rearranged_data[symbol] = [
                    {
                        "date": datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d'),
                        "close": close,
                        "daily_change": daily_change,
                        "daily_return": daily_return
                    }
                    for timestamp, close, daily_change, daily_return in zip(timestamps, closes, daily_changes, daily_returns)
                ]

        # Sauvegarder les données réarrangées dans un nouveau fichier
        rearranged_file_path = os.path.join(output_folder, 'yahoo_finance_data_rearranged.json')
        with open(rearranged_file_path, 'w') as fichier_json:
            json.dump(rearranged_data, fichier_json, indent=4)

        print("Les données ont été réarrangées et enregistrées dans 'yahoo_finance_data_rearranged.json'.")
    else:
        print("Fichier JSON introuvable. Rien à réarranger.")

def push_to_redis(output_folder):
    import json
    import os
    
    # Chemin du fichier JSON réarrangé
    rearranged_file_path = os.path.join(output_folder, 'yahoo_finance_data_rearranged.json')
    
    if not os.path.exists(rearranged_file_path):
        print(f"Fichier JSON {rearranged_file_path} introuvable.")
        return

    # Charger les données réarrangées
    with open(rearranged_file_path, 'r') as fichier_json:
        data = json.load(fichier_json)
    
    # Connexion à Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    
    # Pousser les données dans Redis
    for entreprise, valeurs in data.items():
        redis_key = f"finance_data:{entreprise}"
        
        # Supprimer les anciennes données si elles existent
        r.delete(redis_key)

        # Sauvegarder les données dans une hashmap Redis
        for item in valeurs:
            date = item["date"]
            # Sauvegarder chaque date comme une entrée dans le hashmap
            r.hset(redis_key, date, json.dumps({
                "close": item["close"],
                "daily_change": item["daily_change"],
                "daily_return": item["daily_return"]
            }))
        
        print(f"Données pour {entreprise} ajoutées à Redis sous la clé {redis_key}.")

# Extracts split parts from a folder into a combined tar.gz file and extracts its contents.
def extract_archive(input_folder, output_folder, output_file):
    # Check if the output tar file already exists
    if os.path.exists(os.path.join(output_folder, output_file)):
        print(f"{output_file} already exists. Skipping extraction.")
        return

    # Combine split parts into the original tar.gz file
    with open(os.path.join(output_folder, output_file), "wb") as outfile:
        for part in sorted(os.listdir(input_folder)):
            if part.startswith("20061020_20131126_bloomberg_news.tar.gz."):
                part_path = os.path.join(input_folder, part)
                with open(part_path, "rb") as infile:
                    outfile.write(infile.read())

    # Extract the tar.gz file
    with tarfile.open(os.path.join(output_folder, output_file), "r:gz") as tar:
        tar.extractall(output_folder)

    print("Extraction complete.")

# Process a single article
def process_article(article_path):
    """
    Reads a single article and returns a dictionary of its data if it's valid.
    """
    with open(article_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        # Ensure the article has the expected structure
        if len(lines) >= 4:
            title = lines[0].strip("-- ").strip()
            author = lines[1].strip("-- ").strip()
            date = lines[2].strip("-- ").strip()
            link = lines[3].strip("-- ").strip()
            content = "".join(lines[4:]).strip()

            return {
                "title": title,
                "author": author,
                "date": date,
                "link": link,
                "content": content
            }
        else:
            return None

# Converts extracted article files to a CSV format.
def transform_to_csv(input_folder, output_csv, max_workers=4, batch_size=100):
    # Check if the output CSV file already exists
    if os.path.exists(output_csv):
        print(f"{output_csv} already exists. Skipping transformation.")
        return

    # Initialize a list to store article data
    articles = []

    # Collect all article file paths
    article_paths = []
    for date_folder in os.listdir(input_folder):
        date_path = os.path.join(input_folder, date_folder)
        if os.path.isdir(date_path):
            # Iterate over all articles in the folder
            for article_file in os.listdir(date_path):
                article_path = os.path.join(date_path, article_file)
                if os.path.isfile(article_path):
                    article_paths.append(article_path)

    # Open the CSV file for incremental writing
    with open(output_csv, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        # Write the header to the CSV file (adjust column names to match your data)
        writer.writerow(["title", "author", "date", "link", "content"])

        # Process files in batches
        for i in range(0, len(article_paths), batch_size):
            batch = article_paths[i:i + batch_size]
            print(f"Processing batch {i // batch_size + 1} with {len(batch)} files.")

            # Use ThreadPoolExecutor to parallelize the processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = executor.map(process_article, batch)

            # Write valid articles directly to the CSV file
            for article in results:
                if article is not None:
                    writer.writerow(article.values())

    print("CSV generation complete.")

def clean_csv_data(output_folder, input_filename, output_filename):
    input_filepath = os.path.join(output_folder, input_filename)
    output_filepath = os.path.join(output_folder, output_filename)

    # Vérifier si le fichier existe
    if not os.path.exists(input_filepath):
        print(f"Fichier {input_filepath} introuvable.")
        return

    # Charger le fichier CSV avec pandas
    try:
        df = pd.read_csv(input_filepath)
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier : {e}")
        return

    # Nettoyer les données
    df = df.dropna(subset=['title', 'author', 'date', 'link', 'content'])

    # Supprimer les lignes où la date n'est pas une date valide
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])

    # Trier les données par date
    df = df.sort_values(by='date')

    # Sauvegarder le DataFrame nettoyé dans un nouveau fichier CSV
    try:
        df.to_csv(output_filepath, index=False)
        print(f"Les données nettoyées ont été enregistrées dans {output_filepath}.")
    except Exception as e:
        print(f"Erreur lors de l'écriture du fichier CSV : {e}")

def filter_and_convert_to_json(output_folder, input_filename, output_filename):
    input_filepath = os.path.join(output_folder, input_filename)
    output_filepath = os.path.join(output_folder, output_filename)

    # Vérifier si le fichier existe
    if not os.path.exists(input_filepath):
        print(f"Fichier {input_filepath} introuvable.")
        return

    # Charger le fichier CSV avec pandas (charger uniquement les colonnes nécessaires)
    try:
        df = pd.read_csv(input_filepath, usecols=['title', 'author', 'date', 'link', 'content'])
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier : {e}")
        return

    # Convertir la colonne 'date' en datetime pour faciliter le filtrage
    df['date'] = pd.to_datetime(df['date'], errors='coerce')  # 'coerce' pour gérer les erreurs de conversion
    df = df[df['date'] >= '2010-01-01'] # Filtrer les articles dont la date est après 2010
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

    # Remplacer les valeurs NaN dans 'content' par des chaînes vides
    df['content'] = df['content'].fillna('')

    # Initialiser le dictionnaire des articles filtrés
    articles_by_company = {"GOOGL": [], "MSFT": [], "AAPL": []}

    # Filtrer les articles en utilisant des masques booléens
    keywords = {
        "GOOGL": "Google",
        "MSFT": "Microsoft",
        "AAPL": "Apple"
    }

    for company, keyword in keywords.items():
        mask = df['content'].str.contains(keyword, case=False, na=False)
        filtered_df = df[mask]

        # Convertir en liste de dictionnaires
        articles_by_company[company] = filtered_df.to_dict(orient='records')

    # Sauvegarder le dictionnaire en format JSON
    try:
        with open(output_filepath, 'w', encoding='utf-8') as json_file:
            json.dump(articles_by_company, json_file, ensure_ascii=False, indent=4)
        print(f"Les données filtrées ont été enregistrées dans {output_filepath}.")
    except Exception as e:
        print(f"Erreur lors de l'écriture du fichier JSON : {e}")

def reformat_json(output_folder, input_filename, output_filename):
    input_filepath = os.path.join(output_folder, input_filename)
    output_filepath = os.path.join(output_folder, output_filename)

    # Vérifier si le fichier existe
    if not os.path.exists(input_filepath):
        print(f"Fichier {input_filepath} introuvable.")
        return

    # Charger le fichier JSON
    with open(input_filepath, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    # Fonction pour extraire la date et le lien à partir du contenu
    def extract_date_and_link(content):
        # Expression régulière pour capturer la date et le lien
        date_pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"  # format de la date
        link_pattern = r"http[s]?://[^\s]+"  # URL
        date_match = re.search(date_pattern, content)
        link_match = re.search(link_pattern, content)
        
        date = date_match.group(0) if date_match else None
        link = link_match.group(0) if link_match else None
        
        return date, link

    # Réorganiser les données pour chaque entreprise
    for company, articles in data.items():
        for article in articles:
            title = article.get("title")
            author = article.get("author")
            date = article.get("date")
            link = article.get("link")
            content = article.get("content", "")

            # Si le titre est NaN, réorganiser les informations
            if pd.isna(title):
                # Le titre vient de l'auteur
                article["title"] = author
                # L'auteur vient du lien
                article["author"] = link
                # Extraire la date et le lien du contenu
                extracted_date, extracted_link = extract_date_and_link(content)
                article["date"] = extracted_date if extracted_date else date
                article["link"] = extracted_link if extracted_link else link

            # Si le titre est non défini mais qu'il existe un author ou un link non valide, réorganiser
            if not title and author and link:
                article["title"] = author
                article["author"] = link
                extracted_date, extracted_link = extract_date_and_link(content)
                article["date"] = extracted_date if extracted_date else date
                article["link"] = extracted_link if extracted_link else link

    # Sauvegarder le JSON réorganisé dans un nouveau fichier
    with open(output_filepath, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

    print(f"Les données ont été réorganisées et enregistrées dans {output_filepath}.")

def analyze_sentiment_relative_to_company_in_dag(output_folder, input_filename, company_aliases):
    import nltk
    from nltk.sentiment import SentimentIntensityAnalyzer
    from nltk.tokenize import sent_tokenize

    nltk.download('punkt')
    nltk.download('punkt_tab')
    nltk.download('vader_lexicon')
    # Charger le fichier JSON contenant les articles
    input_filepath = os.path.join(output_folder, input_filename)
    
    if not os.path.exists(input_filepath):
        print(f"Fichier {input_filepath} introuvable.")
        return
    
    with open(input_filepath, 'r', encoding='utf-8') as infile:
        data = json.load(infile)
    
    sia = SentimentIntensityAnalyzer()

    # Traiter chaque entreprise et ses articles
    for symbol, articles in data.items():
        company_name = company_aliases.get(symbol, symbol)  # Récupère le nom de l'entreprise ou utilise le symbole
        for article in articles:
            #Vérifier si une note de sentiment existe déjà
            if 'relative_sentiment' in article:
                print(f"Sentiment déjà calculé pour l'article : {article.get('title', 'Sans titre')}")
                continue
            
            content = article.get('content', '')
            sentences = sent_tokenize(content)  # Diviser en phrases
            relevant_sentiments = []
            
            # Analyser chaque phrase contenant le nom de l'entreprise
            for sentence in sentences:
                if company_name.lower() in sentence.lower():
                    sentiment = sia.polarity_scores(sentence)['compound']
                    relevant_sentiments.append(sentiment)
            
            # Calculer un score global relatif à l'entreprise
            if relevant_sentiments:
                average_sentiment = sum(relevant_sentiments) / len(relevant_sentiments)
                normalized_sentiments = (average_sentiment + 1) / 2
                num_sentences = len(relevant_sentiments)
                
                # Pondération : plus il y a de phrases, plus on garde le score moyen
                weight = 1-exp(-num_sentences/4)  # Approche le score moyen avec plus de phrases
                
                adjusted_sentiment = weight * normalized_sentiments + (1 - weight) * 0.5
                if (article.get('title')=='Motorola Mobility to Move to Merchandise Mart in Chicago'):
                    print(f"Sentiment ajusté : {adjusted_sentiment}")
            else:
                # Aucun sentiment trouvé pour cette entreprise
                adjusted_sentiment = 0.5  # Neutre
            
            # Enrichir l'article avec le sentiment relatif
            article['relative_sentiment'] = adjusted_sentiment 
            if (article.get('title')=='Motorola Mobility to Move to Merchandise Mart in Chicago'):
                    print(article['relative_sentiment'])
    
    # Réécrire directement les données enrichies dans le fichier d'entrée
    with open(input_filepath, 'w', encoding='utf-8') as outfile:
        json.dump(data, outfile, ensure_ascii=False, indent=4)
    
    print(f"Les données enrichies avec les sentiments ont été enregistrées dans {input_filepath}.")

def push_to_mongo(output_folder):
    # Chemin du fichier JSON enrichi
    enriched_file_path = os.path.join(output_folder, 'bloomberg_articles_filtered.json')
    
    if not os.path.exists(enriched_file_path):
        print(f"Fichier JSON {enriched_file_path} introuvable.")
        return

    # Charger les données JSON
    with open(enriched_file_path, 'r') as fichier_json:
        data = json.load(fichier_json)

    # Connexion à MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]  # Nom de la base de données
    collection = db[COLLECTION_NAME]  # Nom de la collection

    # Insérer les données dans MongoDB
    for symbol, articles in data.items():
        for article in articles:
            # Ajout du champ 'symbol' pour chaque document
            article['symbol'] = symbol
            collection.insert_one(article)
            print(f"Document ajouté à MongoDB : {article}")

    print(f"Les données ont été insérées dans MongoDB dans la base 'bloomberg_db', collection 'sentiment_articles'.")

# Fetch data from MongoDB
def fetch_mongo_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    data = list(collection.find({}, {"_id": 0}))
    return pd.DataFrame(data)

# Fetch data from Redis
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

# Fill missing dates and forward-fill data
def preprocess_data():
    mongo_data = fetch_mongo_data()
    redis_data = fetch_redis_data()

    # Process MongoDB Sentiment Data
    if not mongo_data.empty:
        mongo_data["date"] = pd.to_datetime(mongo_data["date"])
        grouped_mongo = (
            mongo_data.groupby([mongo_data["date"].dt.date, "symbol"])
            .agg({"relative_sentiment": "mean"})
            .rename(columns={"relative_sentiment": "avg_sentiment"})
            .reset_index()
        )
    else:
        grouped_mongo = pd.DataFrame()

    # Process Redis Financial Data
    if not redis_data.empty:
        redis_data["date"] = pd.to_datetime(redis_data["date"]).dt.date
    else:
        redis_data = pd.DataFrame()

    # Merge and Fill Missing Dates
    if not grouped_mongo.empty and not redis_data.empty:
        combined = pd.merge(
            grouped_mongo,
            redis_data,
            on=["date", "symbol"],
            how="outer"
        )
        combined = combined.sort_values(["symbol", "date"])
        combined["date"] = pd.to_datetime(combined["date"])
        
        # Fill missing dates and forward-fill data
        date_range = pd.date_range(start=combined["date"].min(), end=combined["date"].max())
        filled_data = (
            combined.set_index("date")
            .groupby("symbol")
            .apply(lambda group: group.reindex(date_range).ffill().bfill())
            .reset_index(level=0, drop=True)
            .reset_index()
        )
        return filled_data

    return pd.DataFrame()

# Function to create the table if it doesn't exist
def create_table_if_not_exists():
    conn = psycopg2.connect(POSTGRES_URI)
    cursor = conn.cursor()
    
    # Create table if it does not exist
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS financial_sentiment (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        date TIMESTAMP NOT NULL,
        avg_sentiment FLOAT,
        close FLOAT
    );
    '''
    
    create_indexes_query = '''
    CREATE INDEX IF NOT EXISTS idx_financial_sentiment_date ON financial_sentiment (date);
    CREATE INDEX IF NOT EXISTS idx_financial_sentiment_symbol ON financial_sentiment (symbol);
    '''
    
    cursor.execute(create_table_query)
    cursor.execute(create_indexes_query)
    
    conn.commit()
    cursor.close()
    conn.close()

# Function to push data to PostgreSQL
def push_to_postgres():
    # Create table if not exists
    create_table_if_not_exists()

    # Process and insert data
    data = preprocess_data()

    # Debugging: Display columns of the DataFrame
    if data is not None:
        logging.info(f"DataFrame columns: {data.columns.tolist()}")
        logging.info(f"DataFrame head: {data.head()}")
    else:
        logging.error("The processed data is None or empty.")
        raise ValueError("Data is empty or not processed correctly.")

    # Rename index to date if necessary
    if 'index' in data.columns:
        data = data.rename(columns={'index': 'date'})

    # Set up PostgreSQL connection using SQLAlchemy
    engine = create_engine(POSTGRES_URI)

    # Prepare the data to insert into the database
    try:
        # Ensure 'date' is properly formatted
        data['date'] = pd.to_datetime(data['date'])
        
        # Select only required columns
        data_to_insert = data[['symbol', 'date', 'avg_sentiment', 'close']]
        
        # Convert date to string format if required by the database
        data_to_insert['date'] = data_to_insert['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    except KeyError as e:
        logging.error(f"KeyError: {e}. Available columns: {data.columns.tolist()}")
        raise

    # Insert data into PostgreSQL
    try:
        data_to_insert.to_sql('financial_sentiment', engine, if_exists='append', index=False)
        logging.info("Data successfully inserted into the 'financial_sentiment' table.")
    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")
        raise

# Création du DAG
# Paramètres par défaut du DAG
default_args_dict = {
    'start_date': datetime.datetime(2020, 1, 10, 0, 0, 0),  # Set to a future date so that it doesn't start automatically on unpause
    'concurrency': 10,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

### INGESTION PIPELINE ###
ingestion_dag = DAG(
    dag_id='StockMood_Ingestion',
    default_args=default_args_dict,
    catchup=False,
    schedule_interval=None,  # Only manual runs
    is_paused_upon_creation=False,  # DAG starts unpaused by default
    max_active_tasks=10
)

branch_task = BranchPythonOperator(
    task_id='branch_check_internet',
    python_callable=branch_check_internet,
    op_kwargs={'output_folder': '/opt/airflow/data'},
    dag=ingestion_dag
)

api_fetch_task = PythonOperator(
    task_id='api_fetch_and_save_json',
    python_callable=api_fetch_and_save_json,
    op_kwargs={'output_folder': '/opt/airflow/data'},
    dag=ingestion_dag
)

local_fetch_task = PythonOperator(
    task_id='local_fetch_and_save_json',
    python_callable=local_fetch_and_save_json,
    op_kwargs={'output_folder': '/opt/airflow/data'},
    dag=ingestion_dag
)

branch_merge_task = DummyOperator(
    task_id='branch_merge',
    dag=ingestion_dag,
    trigger_rule='one_success'
)

extract_archive_task = BashOperator(
    task_id='extract_archive',
    bash_command="""
        OUTPUT_FOLDER="/opt/airflow/data"
        INPUT_FOLDER="/opt/airflow/data/bloomberg_data_src"
        OUTPUT_FILE="20061020_20131126_bloomberg_news.tar.gz"

        if [ -f "${OUTPUT_FOLDER}/${OUTPUT_FILE}" ]; then
            echo "${OUTPUT_FILE} already exists. Skipping extraction."
        else
            cat $(ls ${INPUT_FOLDER}/20061020_20131126_bloomberg_news.tar.gz.* | sort) > ${OUTPUT_FOLDER}/${OUTPUT_FILE}
            tar -xzf ${OUTPUT_FOLDER}/${OUTPUT_FILE} -C ${OUTPUT_FOLDER}
            echo "Extraction complete."
        fi
    """,
    dag=ingestion_dag
)

transform_to_csv_task = PythonOperator(
    task_id='transform_to_csv',
    python_callable=transform_to_csv,
    op_kwargs={
        'input_folder': "/opt/airflow/data/20061020_20131126_bloomberg_news",
        'output_csv': "/opt/airflow/data/bloomberg_articles_aggregated.csv"
    },
    dag=ingestion_dag,
    trigger_rule = 'all_success'
)

trigger_wrangling_dag = TriggerDagRunOperator(
    task_id='trigger_wrangling_dag',
    trigger_dag_id='StockMood_Wrangling',
    dag=ingestion_dag,
    trigger_rule='all_success'
)

branch_task >> [api_fetch_task, local_fetch_task] >> branch_merge_task >> trigger_wrangling_dag
extract_archive_task >> transform_to_csv_task >> trigger_wrangling_dag

### WRANGLING PIPELINE ###
wrangling_dag = DAG(
    dag_id='StockMood_Wrangling',
    default_args=default_args_dict,
    catchup=False,
    schedule_interval=None,  # Only manual runs
    is_paused_upon_creation=False,  # DAG starts unpaused by default
    max_active_tasks=10
)

clean_csv_task = PythonOperator(
    task_id='clean_csv_data',
    python_callable=clean_csv_data,
    op_kwargs={
        'output_folder': '/opt/airflow/data',
        'input_filename': 'bloomberg_articles_aggregated.csv',
        'output_filename': 'bloomberg_articles_aggregated.csv'
    },
    dag=wrangling_dag
)

filter_and_convert_task = PythonOperator(
    task_id='filter_and_convert_to_json',
    python_callable=filter_and_convert_to_json,
    op_kwargs={
        'output_folder': '/opt/airflow/data',
        'input_filename': 'bloomberg_articles_aggregated.csv',
        'output_filename': 'bloomberg_articles_filtered.json'
    },
    dag=wrangling_dag,
    trigger_rule='none_failed_min_one_success'
)

reformat_json_task = PythonOperator(
    task_id='reformat_json',
    python_callable=reformat_json,
    op_kwargs={
        'output_folder': '/opt/airflow/data',
        'input_filename': 'bloomberg_articles_filtered.json',
        'output_filename': 'bloomberg_articles_filtered.json'
    },
    dag=wrangling_dag,
    trigger_rule='none_failed_min_one_success'
)

sentiment_analysis_task = PythonOperator(
    task_id='analyze_sentiment_relative_to_company',
    python_callable=analyze_sentiment_relative_to_company_in_dag,
    op_kwargs={
        'output_folder': '/opt/airflow/data',
        'input_filename': 'bloomberg_articles_filtered.json',
        'company_aliases': {
            "GOOGL": "Google",
            "AAPL": "Apple",
            "MSFT": "Microsoft"
        }
    },
    dag=wrangling_dag
)

push_to_mongo_task = PythonOperator(
    task_id='push_to_mongo',
    python_callable=push_to_mongo,
    op_kwargs={
        'output_folder': '/opt/airflow/data',
    },
    dag=wrangling_dag,
    trigger_rule='none_failed_min_one_success'
)

clean_task = PythonOperator(
    task_id='clean_json_data',
    python_callable=clean_json_data,
    op_kwargs={'output_folder': '/opt/airflow/data'},
    dag=wrangling_dag,
    trigger_rule='none_failed_min_one_success'
)

rearrange_task = PythonOperator(
    task_id='rearrange_json_data',
    python_callable=rearrange_json_data,
    op_kwargs={'output_folder': '/opt/airflow/data'},
    dag=wrangling_dag,
    trigger_rule='none_failed_min_one_success'
)

push_to_redis_task = PythonOperator(
    task_id='push_to_redis',
    python_callable=push_to_redis,
    op_kwargs={'output_folder': '/opt/airflow/data'},
    dag=wrangling_dag,
    trigger_rule='none_failed_min_one_success'
)

trigger_production_dag = TriggerDagRunOperator(
    task_id='trigger_production_dag',
    trigger_dag_id='StockMood_Production',
    dag=wrangling_dag,
    trigger_rule='all_success'
)

clean_csv_task >> filter_and_convert_task >> reformat_json_task >> sentiment_analysis_task >> push_to_mongo_task >> trigger_production_dag
clean_task >> rearrange_task >> push_to_redis_task >> trigger_production_dag

### PRODUCTION PIPELINE ###
production_dag = DAG(
    dag_id='StockMood_Production',
    default_args=default_args_dict,
    catchup=False,
    schedule_interval=None,  # Only manual runs
    is_paused_upon_creation=False,  # DAG starts unpaused by default
    max_active_tasks=10
)

production_dag = DAG(
    dag_id='StockMood_Production',
    default_args=default_args_dict,
    catchup=False,
    schedule_interval=None,  # Only manual runs
    is_paused_upon_creation=False,  # DAG starts unpaused by default
    max_active_tasks=10
)

# Tasks in the pipeline
fetch_mongo_task = PythonOperator(
    task_id='fetch_mongo_data',
    python_callable=fetch_mongo_data,
    dag=production_dag
)

fetch_redis_task = PythonOperator(
    task_id='fetch_redis_data',
    python_callable=fetch_redis_data,
    dag=production_dag
)

prepare_production_data_task = PythonOperator(
    task_id='prepare_production_data',
    python_callable=preprocess_data,
    dag=production_dag
)

push_postgres_task = PythonOperator(
    task_id='push_to_postgres',
    python_callable=push_to_postgres,
    dag=production_dag
)

# Task sequence
[fetch_mongo_task, fetch_redis_task] >> prepare_production_data_task >> push_postgres_task