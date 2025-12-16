import os
import re
import logging
import pandas as pd
from pymongo import MongoClient
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook


# КОНФИГУРАЦИЯ (копируем из dag_1_etl.py)
DATA_DIR = os.environ.get("DATA_DIR") or Variable.get("DATA_DIR", default_var="/opt/airflow/data")
CSV_FILE = os.path.join(DATA_DIR, "tiktok_google_play_reviews.csv")

# MONGO_HOST = "mongo"
# MONGO_PORT = 27017
# MONGO_USER = "root"
# MONGO_PASSWORD = "example"
MONGO_DB = "tiktok_reviews"
MONGO_COLLECTION = "reviews"

logger = logging.getLogger(__name__)

# ============================================================================
# HELPER FUNCTIONS  dag_1_etl.py
# ============================================================================

def check_file_empty():

    try:
        if not os.path.exists(CSV_FILE):
            logger.error(f"File not found: {CSV_FILE}")
            return "empty_file_task"

        file_size = os.path.getsize(CSV_FILE)
        logger.info(f"File size: {file_size} bytes")

        if file_size == 0:
            logger.warning(f"File is empty: {CSV_FILE}")
            return "empty_file_task"
        else:
            logger.info(f"File has data: {CSV_FILE}, proceeding with processing")
            return "processing_group.load_csv_and_save"
    except Exception as e:
        logger.error(f"Error checking file: {e}")
        return "empty_file_task"


# Файл: plugins/utils/data_processing.py

# ...

def load_csv_and_save(**context):
    # ... (константы и объявление) ...
    run_id = context['dag_run'].run_id
    temp_file_path = os.path.join(DATA_DIR, f"temp_processed_data_{run_id}.csv")

    logger.info(f"Loading CSV from {CSV_FILE}")
    try:
        # --- ИЗМЕНЕНИЕ: Используем UTF-8, но игнорируем некорректные байты ---
        df = pd.read_csv(
            CSV_FILE, 
            encoding='utf-8', 
            encoding_errors='ignore',
            dtype={'content': str})
         
        logger.info(f"Loaded {len(df)} rows, columns: {list(df.columns)}")

        # if "at" in df.columns:
        #     logger.info("Converting 'at' column to datetime")
        #     df["at"] = pd.to_datetime(
        #         df["at"], 
        #         format="%d.%m.%Y %H:%M:%S",  # твой формат дат
        #         errors="coerce"
        #     )

        #     # 🔹 Преобразуем в Python datetime для Mongo
        #     records = df.to_dict("records")
        #     for r in records:
        #         if pd.notnull(r["at"]):
        #             r["at"] = r["at"].to_pydatetime()
        #     logger.info("Column 'at' successfully converted to datetime")
        
        df.to_csv(temp_file_path, index=False, encoding='utf-8') 
        logger.info(f"DataFrame saved to temporary file: {temp_file_path}")
        
        return temp_file_path
        
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        raise


def replace_null_values(**context):

    ti = context["task_instance"]
    # Получаем путь, который был передан через XCom (маленькая строка)
    temp_file_path = ti.xcom_pull(task_ids="processing_group.load_csv_and_save")
    
    logger.info(f"Reading data from {temp_file_path}")
    df = pd.read_csv(temp_file_path)
    
    logger.info("Replacing 'null' with '-'")

    null_strings = ['null', 'Null', 'NULL', 'nan', 'NaN', 'NONE', 'None']
    df = df.replace(null_strings, "-", regex=False)
    
    df = df.fillna("-")

    # Перезаписываем файл
    df.to_csv(temp_file_path, index=False, encoding='utf-8')
    
    logger.info(f"After null replacement: {df.shape}")
    return temp_file_path


def sort_by_date(**context):

    ti = context["task_instance"]
    
    # 1. Получаем ПУТЬ из XCom (вместо JSON-строки)
    temp_file_path = ti.xcom_pull(task_ids="processing_group.replace_nulls")
    
    # 2. ИСПОЛЬЗУЕМ ПРАВИЛЬНУЮ ФУНКЦИЮ ДЛЯ ЧТЕНИЯ ФАЙЛА
    logger.info(f"Reading data from {temp_file_path}")
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    df = pd.read_csv(temp_file_path) # <--- ВОТ ЧТО ДОЛЖНО БЫТЬ
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    df["at"] = pd.to_datetime(df["at"])
    df = df.sort_values("at")
    # ... (остальная логика сортировки) ...
    
    # 3. Перезаписываем файл
    df.to_csv(temp_file_path, index=False, encoding='utf-8')
    return temp_file_path

def clean_content(**context):
    
    ti = context["task_instance"]
    
    # 1. Получаем ПУТЬ к Parquet-файлу, а не JSON
    temp_file_path = ti.xcom_pull(task_ids="processing_group.sort_dates")
    
    logger.info(f"Reading data from {temp_file_path}")
    # 2. ЧИТАЕМ ИЗ PARQUET
    df = pd.read_csv(temp_file_path)
    
    logger.info("Cleaning 'content' column (remove emojis)")
    
    def strict_clean(text):
        # FIX: Handle floats/doubles causing MongoDB crash
        if pd.isna(text) or text == "-":
            return ""

        # Force convert to string to avoid "found: double" error
        text = str(text)

        if text.lower() == 'nan':
            return ""

        # Clean regex
        cleaned = re.sub(r'[^a-zA-Z0-9\s\.,?!:;\(\)\'\_]+', ' ', text, flags=re.U).strip()
        return cleaned

    # Apply to ALL columns that might be text
    if 'content' in df.columns and df['content'].dtype == 'object':
        df['content'] = df['content'].apply(strict_clean)

    # Final cleanup: remove empty rows created by cleaning
    # (Оставляем, так как хотим удалить строки, где content пуст)
    if 'content' in df.columns:
        df = df[df['content'] != ""]
        
    df.to_csv(temp_file_path, index=False, encoding='utf-8')
    logger.info(f"DataFrame saved back to temporary file: {temp_file_path}")
    # 4. Возвращаем путь к файлу для следующей задачи (XCom передает только путь)
    return temp_file_path


def load_to_mongodb(mongo_conn_id,**context):
    
    ti = context["task_instance"]
    
    # 1. Получаем ПУТЬ к финальному csv-файлу
    temp_file_path = ti.xcom_pull(task_ids="processing_group.clean_content")

    logger.info(f"Reading final data from {temp_file_path}")
    df = pd.read_csv(temp_file_path)

    hook = MongoHook(mongo_conn_id=mongo_conn_id)
    
    # --- Код подключения к MongoDB ---
    try:
        # Убедитесь, что все константы (MONGO_HOST, MONGO_DB и т.д.) доступны в этом модуле!
        client = hook.get_conn() 
        client.admin.command("ping")

        logger.info(f"MongoDB connection successful via Airflow Connection ID: {mongo_conn_id}")
        
        # Используем константы MONGO_DB и MONGO_COLLECTION, которые остались в data_processing.py
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # 3. ОЧИСТКА: Удаляем временный файл ДО вставки (если упадет, файл останется для отладки)
        # os.remove(temp_file_path) 
        # logger.info(f"Temporary file removed: {temp_file_path}")
        
        # 4. Вставка
        records = df.to_dict("records")
        logger.info(f"Inserting {len(records)} records into {MONGO_DB}.{MONGO_COLLECTION}")
        
        result = collection.insert_many(records)
        logger.info(f"Inserted {len(result.inserted_ids)} documents")
        
        client.close()
        logger.info("MongoDB connection closed.")
        
        return f"Successfully loaded {len(records)} records to MongoDB"
        
    except Exception as e:
        logger.error(f"Error loading to MongoDB: {e}")
        # Если здесь произойдет сбой (например, ошибка БД), файл уже удален.
        # Если сбой произойдет до os.remove, файл останется.
        raise

# ============================================================================
# HELPER FUNCTIONS  dag_2_confirm.py
# ============================================================================

def confirm_data_load(mongo_conn_id):

    logger.info("DAG 2 triggered: Confirming data load...")
    
    try:
        hook = MongoHook(mongo_conn_id=mongo_conn_id)

        client = hook.get_conn() 
        client.admin.command("ping")
        logger.info(f"MongoDB connection successful via Hook {mongo_conn_id}")
        
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        doc_count = collection.count_documents({})
        logger.info(f"Total documents in {MONGO_COLLECTION}: {doc_count}")
        
        # Get one sample document
        sample_doc = collection.find_one()
        if sample_doc:
            logger.info(f"Sample document: {sample_doc}")
        
        client.close()
        
        return f"Confirmation complete. Total records: {doc_count}"
        
    except Exception as e:
        logger.error(f"Error confirming data load: {e}")
        raise


def data_quality_check(mongo_conn_id):

    logger.info("Performing data quality checks...")
    
    try:
        hook = MongoHook(mongo_conn_id=mongo_conn_id)

        client = hook.get_conn() 
        client.admin.command("ping")
        
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Sample aggregation: count records per rating (if rating field exists)
        pipeline = [
            {
                "$group": {
                    "_id": "$rating",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        logger.info(f"Record count by rating: {results}")
        
        client.close()
        return "Data quality checks passed"
        
    except Exception as e:
        logger.error(f"Error performing data quality checks: {e}")
        raise

# ============================================================================
# ANALYSTICS FUNCTIONS  
# ============================================================================
def process_and_save_report(collection, pipeline, report_title): 
    """
    Выполняет агрегацию и логирует результат. (Без сохранения в CSV)
    """
    logger.info(f"\n--- {report_title} ---")
    
    # 1. Выполнение агрегации
    results = list(collection.aggregate(pipeline))
    
    logger.info(f"Найдено результатов: {len(results)}")
    
    # 2. Вывод результатов в лог (консоль Airflow)
    if results:
        # Для красивого вывода в консоль, используем Pandas для форматирования
        df_report = pd.DataFrame(results)
        logger.info(f"Первые 5 результатов:\n{df_report.head().to_string()}")
    else:
        logger.info("Результаты не найдены.")

    # 3. Возврат результата (для XCom)
    return results

# ----------------------------------------------------------------------------
# ОСНОВНАЯ ФУНКЦИЯ (MAIN)
# ----------------------------------------------------------------------------

# def print_section(title: str):
#     logger.info("\n" + "=" * 60)
#     logger.info(title)
#     logger.info("=" * 60)


# # QUERY 1: Топ-5 наиболее частых комментариев
# def show_top_comments(collection):
#     logger.info("TOP 5 MOST FREQUENT COMMENTS")

#     pipeline = [
#         {"$group": {"_id": "$content", "count": {"$sum": 1}}},
#         {"$sort": {"count": -1}},
#         {"$limit": 5}
#     ]
#     results = list(collection.aggregate(pipeline))

#     if not results:
#         logger.info("No data found.")
#     else:
#         logger.info(f"{'COUNT':<10} | {'COMMENT TEXT'}")
#         logger.info("-" * 60)
#         for row in results:
#             logger.info(f"{row['count']:<10} | {row['_id']}")


# # QUERY 2: Комментарии короче 5 символов
# def show_short_comments(collection):
#     logger.info("SHORT COMMENTS (< 5 chars)")

#     pipeline = [
#         {"$match": {"content": {"$ne": None}}},
#         {"$project": {"content": 1, "length": {"$strLenCP": "$content"}}},
#         {"$match": {"length": {"$lt": 5}}},
#         {"$limit": 10}
#     ]
#     results = list(collection.aggregate(pipeline))

#     if not results:
#         logger.info("No short comments found.")
#     else:
#         logger.info(f"{'LENGTH':<10} | {'CONTENT'}")
#         logger.info("-" * 60)
#         for row in results:
#             logger.info(f"{row['length']:<10} | {row['content']}")


# # QUERY 3: Средний рейтинг по дням
# def show_avg_rating(collection):
#     logger.info("AVERAGE RATING BY DATE")

#     pipeline = [
#         {"$match": {"at": {"$ne": None}, "score_numeric": {"$ne": None}}},
#         {"$addFields": {
#             "date_str": {
#                 "$dateToString": {"format": "%Y-%m-%d", "date": "$at"}
#             }
#         }},
#         {"$group": {"_id": "$date_str", "avg_score": {"$avg": "$score_numeric"}}},
#         {"$sort": {"_id": 1}},
#         {"$limit": 10}
#     ]
#     results = list(collection.aggregate(pipeline))

#     if not results:
#         logger.info("No data found.")
#     else:
#         logger.info(f"{'DATE':<15} | {'AVG SCORE'}")
#         logger.info("-" * 60)
#         for row in results:
#             val = row.get('avg_score')
#             score = round(val, 2) if val is not None else 0.0
#             logger.info(f"{row['_id']:<15} | {score}")


# # MAIN EXECUTION
# def run_all_mongodb_analytics(mongo_conn_id):
#     logger.info("--- Начинаем выполнение аналитических запросов MongoDB ---")
#     client = None
#     try:
#         hook = MongoHook(mongo_conn_id=mongo_conn_id)
#         client = hook.get_conn()
#         client.admin.command("ping")
#         logger.info(f"MongoDB connection successful via Hook {mongo_conn_id}")

#         collection = client[MONGO_DB][MONGO_COLLECTION]

#         # Запуск отдельных функций
#         show_top_comments(collection)
#         show_short_comments(collection)
#         show_avg_rating(collection)

#     except Exception as e:
#         logger.error(f"Critical error in analytics: {e}")
#         raise
#     finally:
#         if client:
#             client.close()
#             logger.info("MongoDB connection closed.")

#     logger.info("--- Аналитика завершена ---")
#     return "Analytics queries completed successfully."

def run_all_mongodb_analytics(mongo_conn_id):
    
    logger.info("--- Начинаем выполнение аналитических запросов MongoDB ---")

    client = None
    collection = None

    try:
        # 1. ПОДКЛЮЧЕНИЕ К MONGODB (Использует константы из etl_logic.py)
        hook = MongoHook(mongo_conn_id=mongo_conn_id)
        client = hook.get_conn()

        client.admin.command("ping")
        logger.info(f"Прямое подключение к MongoDB успешно через Hook {mongo_conn_id}.")
        
        collection = client[MONGO_DB][MONGO_COLLECTION]
        
        # -------------------------------------------------------------
        # 2. ОПРЕДЕЛЕНИЕ ПАЙПЛАЙНОВ И ВЫЗОВЫ
        # -------------------------------------------------------------
        
        # 2.1. ТОП-5 НАИБОЛЕЕ ЧАСТЫХ КОММЕНТАРИЕВ (QUERY 1)
        pipeline_top_5 = [
            {"$group": {"_id": "$content", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        process_and_save_report(
            collection, 
            pipeline_top_5, 
            "1. Топ-5 наиболее частых комментариев"
        )
        
        # 2.2. ЗАПИСИ, ГДЕ ДЛИНА ПОЛЯ «СОДЕРЖАНИЕ» МЕНЕЕ 5 СИМВОЛОВ (QUERY 2)
        pipeline_short_content = [
            {"$match": {"content": {"$ne": None}}}, # Фильтруем пустые значения
            {"$project": {"content": 1, "length": {"$strLenCP": "$content"}}},
            {"$match": {"length": {"$lt": 5}}},
            {"$limit": 10}
        ]
        process_and_save_report(
            collection, 
            pipeline_short_content, 
            "2. Записи, где длина поля «содержание» < 5 символов"
        )
        
        # 2.3. СРЕДНИЙ РЕЙТИНГ ЗА КАЖДЫЙ ДЕНЬ (QUERY 3)
        pipeline_avg_rating = [
            {"$match": {"at": {"$ne": None}, "score": {"$ne": None}}},
            
            # Преобразуем строку в Date
            {"$addFields": {
                "at_date": {
                    "$dateFromString": {
                        "dateString": "$at",
                        "format": "%Y-%m-%d %H:%M:%S"
                    }
                }
            }},
            
            # Форматируем дату в строку (только день)
            {"$addFields": {
                "datetime_str": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$at_date"
                    }
                }
            }},
            
            {"$group": {"_id": "$datetime_str", "avg_score": {"$avg": "$score"}}},
            {"$sort": {"_id": 1}},
            {"$limit": 10}
        ]

        process_and_save_report(
            collection, 
            pipeline_avg_rating, 
            "3. Средний рейтинг за каждый день"
        )
        
    except Exception as e:
        logger.error(f"Критическая ошибка (Mongo/Pipe): {e}")
        # Ошибка будет передана в Airflow, и таск упадет
        raise 
        
    finally:
        if client: 
            client.close()
            logger.info("Соединение с MongoDB закрыто.")

    logger.info("--- Аналитика завершена ---")
    return "Analytics queries completed successfully."

    
