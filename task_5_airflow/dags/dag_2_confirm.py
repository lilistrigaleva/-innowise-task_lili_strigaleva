import logging
from datetime import datetime, timedelta

from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from airflow.providers.mongo.hooks.mongo import MongoHook

from utils.data_processing import (
    confirm_data_load,
    data_quality_check,
    run_all_mongodb_analytics,
    
)

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================
MONGO_CONN_ID = "mongodb_tiktok"

dataset_trigger = Dataset("file:///opt/airflow/data/tiktok_google_play_reviews.csv")

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
    dag_id="dag_2_confirm_data_load",
    default_args=default_args,
    description="Confirmation DAG: triggered by Dataset update from DAG 1",
    schedule=[dataset_trigger],  # Triggered when Dataset is updated
    catchup=False,
    tags=["confirmation", "mongodb", "triggered"],
)

# ============================================================================
# DAG TASKS
# ============================================================================

confirm_task = PythonOperator(
    task_id="confirm_data_load",
    python_callable=confirm_data_load,
    op_kwargs={'mongo_conn_id': MONGO_CONN_ID},
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id="data_quality_check",
    python_callable=data_quality_check,
    op_kwargs={'mongo_conn_id': MONGO_CONN_ID},
    dag=dag,
)

analytics_task = PythonOperator(
    task_id="run_mongodb_analytics",
    python_callable=run_all_mongodb_analytics,
    op_kwargs={'mongo_conn_id': MONGO_CONN_ID}, 
    dag=dag,
)
# ============================================================================
# DAG DEPENDENCIES
# ============================================================================

confirm_task >> quality_check_task >> analytics_task
