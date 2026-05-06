from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import glob
import json
import logging
import os


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "aljazeera_scraping_pipeline",
    default_args=default_args,
    description="Pipeline de scraping Al Jazeera - batch toutes les heures",
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 4, 18),
    catchup=False,
    tags=["scraping", "aljazeera", "batch"],
)

DATA_DIR = "/opt/airflow/data"
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def validate_data_quality(**context):
    issues = {"no_title": 0, "no_date": 0, "short_content": 0}
    valid = 0
    for filepath in glob.glob(os.path.join(SILVER_DIR, "aljazeera_silver_*.json")):
        with open(filepath, "r", encoding="utf-8") as f:
            articles = json.load(f)
            for article in articles:
                if not article.get("title"):
                    issues["no_title"] += 1
                if not article.get("date_published"):
                    issues["no_date"] += 1
                if len(article.get("content", "")) < 100:
                    issues["short_content"] += 1
                else:
                    valid += 1
    logging.info(f"Articles valides : {valid}")
    logging.info(f"Sans titre : {issues['no_title']}")
    logging.info(f"Sans date : {issues['no_date']}")
    logging.info(f"Contenu trop court : {issues['short_content']}")
    return issues


def count_silver_articles(**context):
    total = 0
    urls = set()
    for filepath in glob.glob(os.path.join(SILVER_DIR, "aljazeera_silver_*.json")):
        with open(filepath, "r", encoding="utf-8") as f:
            articles = json.load(f)
            total += len(articles)
            for article in articles:
                url = article.get("url")
                if url:
                    urls.add(url)
    logging.info(f"Total articles silver : {total}")
    logging.info(f"Articles silver uniques : {len(urls)}")
    context["ti"].xcom_push(key="total_articles", value=total)
    context["ti"].xcom_push(key="unique_articles", value=len(urls))
    return len(urls)


def log_pipeline_summary(**context):
    ti = context["ti"]
    total = ti.xcom_pull(key="total_articles", task_ids="count_silver_articles") or 0
    unique = ti.xcom_pull(key="unique_articles", task_ids="count_silver_articles") or 0
    logging.info("=" * 50)
    logging.info("RÉSUMÉ DU PIPELINE")
    logging.info(f"  Heure d'exécution  : {datetime.now()}")
    logging.info(f"  Total articles     : {total}")
    logging.info(f"  Articles uniques   : {unique}")
    logging.info("=" * 50)


t1_scrape = BashOperator(
    task_id="scrape_bronze",
    bash_command="python /opt/airflow/scripts/scrape_aljazeera.py",
    dag=dag,
)

t2_transform = BashOperator(
    task_id="transform_silver",
    bash_command="python /opt/airflow/scripts/transform_articles.py",
    dag=dag,
)

t3_gold = BashOperator(
    task_id="aggregate_gold",
    bash_command="python /opt/airflow/scripts/aggregate_gold.py",
    dag=dag,
)

t4_validate_schema = BashOperator(
    task_id="validate_schema",
    bash_command="python /opt/airflow/scripts/validate_schema.py",
    dag=dag,
)

t5_quality = PythonOperator(
    task_id="validate_data_quality",
    python_callable=validate_data_quality,
    dag=dag,
)

t6_upload = BashOperator(
    task_id="upload_to_minio",
    bash_command="python /opt/airflow/scripts/upload_to_minio.py",
    dag=dag,
)

t7_warehouse = BashOperator(
    task_id="load_warehouse",
    bash_command="python /opt/airflow/scripts/load_warehouse.py",
    dag=dag,
)

t8_summary = PythonOperator(
    task_id="log_pipeline_summary",
    python_callable=log_pipeline_summary,
    dag=dag,
)

t1_scrape >> t2_transform >> t3_gold >> t4_validate_schema >> t5_quality >> t6_upload >> t7_warehouse >> t8_summary