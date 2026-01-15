import logging
import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Инициализируем логгер для ЭТОГО модуля
logger = logging.getLogger(__name__)

def upload_csv_to_snowflake(file_path: str, table_name: str, schema_name: str, conn_id: str, database_name: str):
    """
    Функция содержит чистую логику загрузки.
    Она ничего не знает про DAG, Task или Airflow context.
    Она просто делает работу.
    """
    
    # Логируем старт
    logger.info(f"Запуск модуля загрузки. Файл: {file_path}")

    # Проверки
    if not os.path.exists(file_path):
        logger.error(f"Файл не найден: {file_path}")
        raise FileNotFoundError(f"Файл {file_path} отсутствует")

    # Работа с базой
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    
    try:
        logger.info("Формирование SQL команд...")
        put_cmd = f"PUT file://{file_path} @%{table_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        
        # В COPY INTO тоже добавим базу для надежности
        copy_cmd = f"""
            COPY INTO {database_name}.{schema_name}.{table_name}
            FROM @%{table_name}
            FILE_FORMAT = (
                TYPE = 'CSV', 
                FIELD_DELIMITER = ';', 
                SKIP_HEADER = 1, 
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE' 
        """
        
        logger.info("Отправка команд в Snowflake...")
        # Выполняем
        # --- ВОТ ИЗМЕНЕННЫЙ БЛОК ---
        # Мы выполняем команды последовательно:
        # 1. Выбираем Базу (USE DATABASE) - без этого была ошибка
        # 2. Выбираем Схему (USE SCHEMA)
        # 3. Грузим файл (PUT)
        # 4. Вставляем данные (COPY)
        hook.run([
            f"USE DATABASE {database_name}", 
            f"USE SCHEMA {schema_name}", 
            put_cmd, 
            copy_cmd
        ], autocommit=True)
        # ---------------------------
        
        logger.info(f"Успешно загружено в {schema_name}.{table_name}")
        
    except Exception as e:
        logger.exception("Произошла ошибка во время загрузки в Snowflake")
        raise e