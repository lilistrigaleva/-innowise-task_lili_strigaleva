import os
import re
import logging
from datetime import datetime, timedelta

import pandas as pd
from pymongo import MongoClient
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from airflow.models import Variable

# --- ИМПОРТ ИЗ ВАШЕГО НОВОГО МОДУЛЯ ---
# Airflow автоматически добавляет dags/ в PYTHONPATH, поэтому можно импортировать 'utils'
from utils.data_processing import (
    check_file_empty,
    load_csv_and_save,
    replace_null_values,
    sort_by_date,
    clean_content,
    load_to_mongodb,
)

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

# File paths (configurable)
# Prefer environment variable `DATA_DIR`, then Airflow Variable `DATA_DIR`.
# Default is the container path `/opt/airflow/data` (used when running in Docker).
DATA_DIR = os.environ.get("DATA_DIR") or Variable.get("DATA_DIR", default_var="/opt/airflow/data")
CSV_FILE = os.path.join(DATA_DIR, "tiktok_google_play_reviews.csv")

MONGO_CONN_ID = "mongodb_tiktok"

# MongoDB configuration
# MONGO_HOST = "mongo"  # service name from docker-compose
# MONGO_PORT = 27017
# MONGO_USER = "root"
# MONGO_PASSWORD = "example"
# MONGO_DB = "tiktok_reviews"
# MONGO_COLLECTION = "reviews"

# Dataset for inter-DAG communication
dataset_update = Dataset(f"file://{CSV_FILE}")

# Logging
logger = logging.getLogger(__name__)

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2025, 1, 1),
}

dag = DAG(
    dag_id="dag_1_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline: CSV -> Pandas cleanup -> MongoDB + Dataset trigger",
    schedule=None,  # Triggered manually or by external event
    catchup=False,
    tags=["etl", "pandas", "mongodb"],
)

# ============================================================================
# DAG TASKS
# ============================================================================

# Task 1: Wait for CSV file
wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath=CSV_FILE,
    poke_interval=5,  # Check every 5 seconds
    timeout=60,  # Timeout after 5 minutes
    dag=dag,
)

# Task 2: Branch based on file emptiness
check_file = BranchPythonOperator(
    task_id="check_file_empty",
    python_callable=check_file_empty,
    dag=dag,
)

# Task 3: If file is empty
empty_file_task = BashOperator(
    task_id="empty_file_task",
    bash_command='echo "File is empty. Skipping processing."',
    dag=dag,
)

# Task 4: TaskGroup for data processing
with TaskGroup("processing_group", dag=dag) as processing_group:
    load_csv_task = PythonOperator(
        task_id="load_csv_and_save",
        python_callable=load_csv_and_save,
        dag=dag,
    )

    replace_nulls_task = PythonOperator(
        task_id="replace_nulls",
        python_callable=replace_null_values,
        dag=dag,
    )

    sort_dates_task = PythonOperator(
        task_id="sort_dates",
        python_callable=sort_by_date,
        dag=dag,
    )

    clean_content_task = PythonOperator(
        task_id="clean_content",
        python_callable=clean_content,
        dag=dag,
    )

    load_mongo_task = PythonOperator(
        task_id="load_mongodb",
        python_callable=load_to_mongodb,
        # ПЕРЕДАЕМ ID СОЕДИНЕНИЯ В ФУНКЦИЮ load_to_mongodb
        op_kwargs={'mongo_conn_id': MONGO_CONN_ID}, 
        dag=dag,
    )

    # Chain tasks inside TaskGroup
    load_csv_task >> replace_nulls_task >> sort_dates_task >> clean_content_task >> load_mongo_task

# Task 5: Update Dataset to trigger DAG 2
update_dataset = PythonOperator(
    task_id="publish_dataset_update",
    python_callable=lambda: logger.info("Dataset update published for DAG 2 trigger"),
    outlets=[dataset_update],
    dag=dag,
)

# ============================================================================
# DAG DEPENDENCIES (Task Flow)
# ============================================================================

wait_for_file >> check_file
check_file >> [empty_file_task, processing_group]
processing_group >> update_dataset
