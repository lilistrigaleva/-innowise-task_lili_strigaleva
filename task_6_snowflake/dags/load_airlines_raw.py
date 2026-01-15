from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import logging

# Импортируем нашу функцию из плагинов
# Примечание: Airflow видит папку plugins как корень
from utils.snowflake_loader import upload_csv_to_snowflake

# Константы
SNOWFLAKE_CONN_ID = 'snowflake_default'
# --- ДОБАВЛЯЕМ ЭТУ СТРОКУ ---
DATABASE_NAME = 'AIRLINES_DWH' 
# ----------------------------
TABLE_NAME = 'AIRLINES_RAW'
SCHEMA_NAME = 'RAW_DATA'
FILE_PATH = '/opt/airflow/data/airline_dataset.csv'

DDL_QUERY = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{SCHEMA_NAME}.{TABLE_NAME} (
        PASSENGER_ID         VARCHAR,
        FIRST_NAME           VARCHAR,
        LAST_NAME            VARCHAR,
        GENDER               VARCHAR,
        AGE                  VARCHAR,
        NATIONALITY          VARCHAR,
        AIRPORT_NAME         VARCHAR,
        AIRPORT_COUNTRY_CODE VARCHAR,
        COUNTRY_NAME         VARCHAR,
        AIRPORT_CONTINENT    VARCHAR,
        CONTINENTS           VARCHAR,
        DEPARTURE_DATE       VARCHAR,
        ARRIVAL_AIRPORT      VARCHAR,
        PILOT_NAME           VARCHAR,
        FLIGHT_STATUS        VARCHAR,
        TICKET_TYPE          VARCHAR,
        PASSENGER_STATUS     VARCHAR
    );
"""

@dag(
    dag_id='01_load_airlines_modular',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['snowflake', 'modular'],
    template_searchpath='/opt/airflow/dags/include'
)
def airline_pipeline():
    # 1. DDL
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=DDL_QUERY
    )

    # 2. Python Task
    @task(task_id="run_loader_logic")
    def run_loading():
        upload_csv_to_snowflake(
            file_path=FILE_PATH,
            table_name=TABLE_NAME,
            schema_name=SCHEMA_NAME,
            conn_id=SNOWFLAKE_CONN_ID,
            database_name=DATABASE_NAME 
        )
    
    # 3. Transformation Task 
    transform_to_core = SQLExecuteQueryOperator(
        task_id="transform_raw_to_core",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="CALL AIRLINES_DWH.CORE_DATA.LOAD_AIRLINES_TO_CORE();", 
        show_return_value_in_logs=True

    )

    create_table >> run_loading()>> transform_to_core

airline_pipeline()