import psycopg2
import os

class DatabaseConnector:
    def __init__(self):
        self.db_params = {
            'dbname': os.getenv('DB_NAME', 'postgres'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASS', 'masterkey'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.conn = None
        self.cursor = None
        
    def __enter__(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.cursor = self.conn.cursor()
            return self.cursor
        except psycopg2.OperationalError as e:
            print(f"Ошибка подключения к БД: {e}")
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type:    
                self.conn.rollback()
            else: 
                self.conn.commit()
            self.cursor.close()
            self.conn.close()