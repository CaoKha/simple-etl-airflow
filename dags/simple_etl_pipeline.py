from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
from fake_useragent import UserAgent

# Chemins de fichiers
RAW_CSV_PATH: str = "/opt/airflow/data/raw_data/sample_data.csv"
JSON_OUTPUT_PATH: str = "/opt/airflow/data/json_data/transformed_data.json"
HTML_OUTPUT_PATH: str = "/opt/airflow/data/html_data/response.html"

# Création des dossiers de sortie si nécessaire
os.makedirs(os.path.dirname(JSON_OUTPUT_PATH), exist_ok=True)
os.makedirs(os.path.dirname(HTML_OUTPUT_PATH), exist_ok=True)


def extract_transform() -> None:
    """
    Fonction pour extraire des données depuis un fichier CSV et les transformer en format JSON.
    Les données transformées sont sauvegardées dans un fichier JSON.
    """
    try:
        # Lecture du fichier CSV
        df: pd.DataFrame = pd.read_csv(RAW_CSV_PATH)
        # Transformation des données en format JSON
        data_json: str | None = df.to_json(orient="records", indent=4)
        if data_json is not None:
            # Sauvegarde des données JSON dans un fichier
            with open(JSON_OUTPUT_PATH, "w", encoding="utf-8") as f:
                f.write(data_json)
            print(
                f"Transformation réussie : les données sont sauvegardées dans {JSON_OUTPUT_PATH}"
            )
        else:
            print("Aucune données à transformer.")
    except Exception as e:
        print(f"Erreur lors de l'extraction/transformation des données : {e}")
        raise


def fetch_and_save_html() -> None:
    """
    Fonction pour effectuer une requête HTTP GET vers un site web et sauvegarder la réponse HTML dans un fichier.
    """
    try:
        ua = UserAgent()
        headers = {
            "User-Agent": ua.random,  # éviter bot detection
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
        }
        response: requests.Response = requests.get(
            "https://group.bnpparibas", headers=headers
        )
        response.encoding = "utf-8"
        print("Requête HTTP accomplie. response.status_code:", response.status_code)
        # Sauvegarde de la réponse HTML dans un fichier
        with open(HTML_OUTPUT_PATH, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(
            f"Requête HTTP réussie : le fichier HTML est sauvegardé dans {HTML_OUTPUT_PATH}"
        )
    except Exception as e:
        print(f"Erreur lors de la requête HTTP : {e}")
        raise


# Configuration par défaut du DAG
default_args: dict = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 16),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Déclaration du DAG avec les tâches
with DAG(
    dag_id="simple_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Un DAG simple avec ETL et requête HTTP parallèle.",
) as dag:
    task_extract_transform: PythonOperator = PythonOperator(
        task_id="extract_and_transform",
        python_callable=extract_transform,
    )

    task_fetch_html: PythonOperator = PythonOperator(
        task_id="fetch_and_save_html",
        python_callable=fetch_and_save_html,
    )

    # Les deux tâches s'exécutent en parallèle
    [task_extract_transform, task_fetch_html]
