import datetime
import os
import json
import requests
import redis
import csv
import pandas as pd
import json
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


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
        return 'fetch_and_save_json'
    else:
        return 'skip_fetch'


# Fonction pour récupérer des données d'une API et les enregistrer dans un fichier JSON
def fetch_and_save_json(output_folder):
    url = 'https://yfapi.net/v8/finance/spark?interval=1d&range=18y&symbols=AAPL%2CMSFT%2CGOOGL'
    headers = {
        'accept': 'application/json',
        'X-API-KEY': 'maDTTOTDX95rRJqe70YAx3uezGtScGqx3o2S2Iex'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        file_path = os.path.join(output_folder, 'donnees_api.json')
        with open(file_path, 'w') as fichier_json:
            json.dump(data, fichier_json, indent=4)
        print("Les données ont été enregistrées dans 'donnees_api.json'.")
    else:
        print(f"Échec de la récupération des données. Code statut HTTP : {response.status_code}")


# Fonction pour nettoyer les données JSON
def clean_json_data(output_folder):
    from datetime import datetime  # Option 1: Importer directement ici si nécessaire
    file_path = os.path.join(output_folder, 'donnees_api.json')

    if os.path.exists(file_path):
        with open(file_path, 'r') as fichier_json:
            data = json.load(fichier_json)

        for symbol, symbol_data in data.items():
            if 'timestamp' in symbol_data and 'close' in symbol_data:
                timestamps = symbol_data['timestamp']
                closes = symbol_data['close']

                # Convertir les timestamps en années et filtrer entre 2007 et 2013
                filtered_data = [
                    (timestamp, close)
                    for timestamp, close in zip(timestamps, closes)
                    if 2007 <= datetime.utcfromtimestamp(timestamp).year <= 2013
                ]

                # Réattribuer les données filtrées
                symbol_data['timestamp'] = [item[0] for item in filtered_data]
                symbol_data['close'] = [item[1] for item in filtered_data]

        # Écrire les données nettoyées dans le fichier
        with open(file_path, 'w') as fichier_json:
            json.dump(data, fichier_json, indent=4)

        print("Les données inutiles ont été supprimées du fichier JSON.")
    else:
        print("Fichier JSON introuvable. Rien à nettoyer.")


def rearrange_json_data(output_folder):
    from datetime import datetime
    file_path = os.path.join(output_folder, 'donnees_api.json')

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
        rearranged_file_path = os.path.join(output_folder, 'donnees_api_rearranged.json')
        with open(rearranged_file_path, 'w') as fichier_json:
            json.dump(rearranged_data, fichier_json, indent=4)

        print("Les données ont été réarrangées et enregistrées dans 'donnees_api_rearranged.json'.")
    else:
        print("Fichier JSON introuvable. Rien à réarranger.")

def push_to_redis(output_folder):
    import json
    import os
    
    # Chemin du fichier JSON réarrangé
    rearranged_file_path = os.path.join(output_folder, 'donnees_api_rearranged.json')
    
    if not os.path.exists(rearranged_file_path):
        print(f"Fichier JSON {rearranged_file_path} introuvable.")
        return

    # Charger les données réarrangées
    with open(rearranged_file_path, 'r') as fichier_json:
        data = json.load(fichier_json)
    
    # Connexion à Redis
    r = redis.Redis(host='redis', port=6379, db=0)
    
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

import pandas as pd
import json
import os

def filter_and_convert_to_json(output_folder, input_filename, output_filename):
    input_filepath = os.path.join(output_folder, input_filename)
    output_filepath = os.path.join(output_folder, output_filename)

    # Vérifier si le fichier existe
    if not os.path.exists(input_filepath):
        print(f"Fichier {input_filepath} introuvable.")
        return

    # Charger le fichier CSV avec pandas
    df = pd.read_csv(input_filepath, usecols=['title', 'author', 'date', 'link', 'content'])

    # Créer un dictionnaire pour stocker les articles filtrés par entreprise
    articles_by_company = {
        "GOOGL": [],
        "MSFT": [],
        "AAPL": []
    }

    # Itérer sur chaque ligne du DataFrame et vérifier le contenu
    for _, row in df.iterrows():
        content = row['content']
        
        # Vérifier que 'content' est une chaîne de caractères (pas NaN, None, ou float)
        if isinstance(content, str):
            # Recherche des mots-clés dans 'content'
            if 'Google' in content:
                articles_by_company['GOOGL'].append({
                    "title": row['title'],
                    "author": row['author'],
                    "date": row['date'],
                    "link": row['link'],
                    "content": row['content']
                })
            elif 'Microsoft' in content:
                articles_by_company['MSFT'].append({
                    "title": row['title'],
                    "author": row['author'],
                    "date": row['date'],
                    "link": row['link'],
                    "content": row['content']
                })
            elif 'Apple' in content:
                articles_by_company['AAPL'].append({
                    "title": row['title'],
                    "author": row['author'],
                    "date": row['date'],
                    "link": row['link'],
                    "content": row['content']
                })

    # Sauvegarder le dictionnaire en format JSON
    with open(output_filepath, 'w', encoding='utf-8') as json_file:
        json.dump(articles_by_company, json_file, ensure_ascii=False, indent=4)

    print(f"Les données filtrées ont été enregistrées dans {output_filepath}.")




# Paramètres par défaut du DAG
default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    dag_id='ingestion',
    default_args=default_args_dict,
    catchup=False
)

# Tâches
branch_task = BranchPythonOperator(
    task_id='branch_check_internet',
    python_callable=branch_check_internet,
    op_kwargs={'output_folder': '/opt/airflow/dags'},
    dag=dag
)

fetch_task = PythonOperator(
    task_id='fetch_and_save_json',
    python_callable=fetch_and_save_json,
    op_kwargs={'output_folder': '/opt/airflow/dags'},
    dag=dag
)

skip_fetch_task = DummyOperator(
    task_id='skip_fetch',
    dag=dag
)

clean_task = PythonOperator(
    task_id='clean_json_data',
    python_callable=clean_json_data,
    op_kwargs={'output_folder': '/opt/airflow/dags'},
    dag=dag,
    trigger_rule='none_failed_min_one_success'
)

rearrange_task = PythonOperator(
    task_id='rearrange_json_data',
    python_callable=rearrange_json_data,
    op_kwargs={'output_folder': '/opt/airflow/dags'},
    dag=dag,
    trigger_rule='none_failed_min_one_success'
)

push_to_redis_task = PythonOperator(
    task_id='push_to_redis',
    python_callable=push_to_redis,
    op_kwargs={'output_folder': '/opt/airflow/dags'},
    dag=dag,
    trigger_rule='none_failed_min_one_success'
)

# Mettre à jour l'ordre des tâches


filter_and_convert_task = PythonOperator(
    task_id='filter_and_convert_to_json',
    python_callable=filter_and_convert_to_json,
    op_kwargs={
        'output_folder': '/opt/airflow/dags',
        'input_filename': 'aggregated_bloomberg_articles.csv',
        'output_filename': 'filtered_bloomberg_articles.json'
    },
    dag=dag,
    trigger_rule='none_failed_min_one_success'  # Exécuter cette tâche même si certaines échouent
)



end_task = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='none_failed_min_one_success'
)


# Définition de l'ordre des tâches
branch_task >> [fetch_task, skip_fetch_task]
fetch_task >> clean_task
skip_fetch_task >> clean_task
clean_task >> rearrange_task
rearrange_task >> push_to_redis_task
push_to_redis_task >> end_task