import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

# Informations compte Google Cloud
SERVICE_ACCOUNT_FILE = '/opt/airflow/cle_carttrend.json'
PROJECT_ID = 'rising-mercury-463817-s5'
DATASET_ID = 'carttrend_rawdata'
DBT_PROJECT_DIR = '/opt/airflow/carttrend_project'

FILES = {
    "Carttrend_Clients": "https://docs.google.com/spreadsheets/d/1PkZuSLHn0eZQLjhBx8qdZ_bh_wzgMbenrYyMGYrxBic/export?format=csv",
    "Carttrend_Campaigns": "https://docs.google.com/spreadsheets/d/1_WxFdSWGGCNreMgSWf9nfuP-Ye_RnCX1Xs5ubnjGp9s/export?format=csv",
    "Carttrend_Commandes": "https://docs.google.com/spreadsheets/d/1QVXmhf9b2OSpUVb7uBOQOClk19ldleNYQcloKCrHlgA/export?format=csv",
    "Carttrend_Details_Commandes": "https://docs.google.com/spreadsheets/d/1kN4O2D-LIvbLSTse2RsguJMPwdMWKtVY6dEl_4hcyqw/export?format=csv",
    "Carttrend_Entrepots_Machine": "https://docs.google.com/spreadsheets/d/1s9R6eJPlC0Vwz_OPRTZ43XXfknBAXktn/export?format=csv",
    "Carttrend_Entrepots": "https://docs.google.com/spreadsheets/d/1FSP2Gv31H1lnpLh6nmaNFcKlCE11OlbA/export?format=csv",
    "Carttrend_Posts": "https://docs.google.com/spreadsheets/d/1N81drG9zhp9VBZh3LqPoQ01cMvXol1kX43hqhQtAZ44/export?format=csv",
    "Carttrend_Produits": "https://docs.google.com/spreadsheets/d/1I4KHaFSEMMJ2E7OEO-v1KWbYfOGUBGiC8XCUVvFHs2I/export?format=csv",
    "Carttrend_Promotions": "https://docs.google.com/spreadsheets/d/1p2O-Zgmhcmfov1BkLb7Rx9k2iwg65kFcgVyYwb4CYs4/export?format=csv",
    "Carttrend_Satisfaction": "https://docs.google.com/spreadsheets/d/1G7rST778z_zcewJX9CuURwIqTSKfWCU_i6ZJ9P8edzM/export?format=csv"
}

# Authentification
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Création du dataset 
def create_dataset_if_not_exists():
    try:
        bq_client.get_dataset(f"{PROJECT_ID}.{DATASET_ID}")
        print(f"Dataset {DATASET_ID} déjà existant.")
    except NotFound:
        dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
        bq_client.create_dataset(dataset)
        print(f"Dataset {DATASET_ID} créé.")

# Pré-Traitement et chargement des fichiers CSV vers BigQuery
def process_and_upload_file(file_url, file_name):
    result = {"table": file_name, "status": "success", "rows": 0, "error": ""}
    try:
        df = pd.read_csv(file_url)
        df.columns = (
            df.columns
            .str.replace(r"[’'\"/\\()\[\]{}:;,.]", "_", regex=True)
            .str.replace(r"é|è|ê", "e", regex=True)
            .str.replace(r"â", "a", regex=True)
            .str.replace(r"ô", "o", regex=True)
            .str.strip()
            .str.lower()
        )
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{file_name.lower()}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV
        )
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        result["rows"] = len(df)
        print(f"Upload terminé : {table_id} ({len(df)} lignes)")
    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        print(f"Erreur pour {file_name} : {e}")
    return result

def download_and_upload_all_files():
    create_dataset_if_not_exists()
    log = []
    for file_name, file_url in FILES.items():
        res = process_and_upload_file(file_url, file_name)
        log.append(res)
    # Affichage du log final
    print("\n===== LOG COMPLET D'IMPORT CSV =====")
    for entry in log:
        if entry["status"] == "success":
            print(f"{entry['table']}: {entry['rows']} lignes importées")
        else:
            print(f"{entry['table']}: Erreur -> {entry['error']}")
    print("===================================")

# DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2025, 8, 14),
    'catchup': False
}

dag = DAG(
    dag_id='carttrend_csv_to_bigquery_with_dbt_full',
    default_args=default_args,
    description='Pipeline complet : CSV → BigQuery → dbt run/test/docs avec logs détaillés',
    schedule_interval="@daily",
    tags=['carttrend', 'bq', 'sheets', 'dbt']
)

# Tâches
upload_csv_task = PythonOperator(
    task_id='import_csv_links_to_bq',
    python_callable=download_and_upload_all_files,
    dag=dag
)

dbt_run_task = BashOperator(
    task_id='dbt_run_transformations',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir /opt/airflow",
    dag=dag
)

dbt_test_task = BashOperator(
    task_id='dbt_test_data_quality',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir /opt/airflow",
    dag=dag
)

dbt_docs_task = BashOperator(
    task_id='dbt_generate_docs',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir /opt/airflow",
    dag=dag
)

upload_csv_task >> dbt_run_task >> dbt_test_task >> dbt_docs_task
