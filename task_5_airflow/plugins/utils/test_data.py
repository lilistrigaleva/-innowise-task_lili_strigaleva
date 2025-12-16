import logging
import pandas as pd

logger = logging.getLogger(__name__)
    
def sort_by_date():
    df = pd.read_csv ("D:/work/airflow_docker/data/tiktok_google_play_reviews.csv") # <--- ВОТ ЧТО ДОЛЖНО БЫТЬ

    logger.info(f"Reading data from {df.shape} rows")
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # df = pd.read_csv(data) # <--- ВОТ ЧТО ДОЛЖНО БЫТЬ
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    df["at"] = pd.to_datetime(df["at"])

    # 3. Перезаписываем файл
    print(df.isna().sum())
    print(df.head())
    # df.to_csv(data, index=False, encoding='utf-8')
    # return data


if __name__ == "__main__":
    sort_by_date()