# main.py
import argparse
from db_connector import DatabaseConnector
from data_loader import DataLoader
from query_executor import QueryExecutor
from formatters import get_formatter

def main():
    """Главная функция для выполнения скрипта."""
    parser = argparse.ArgumentParser(description="Скрипт для загрузки данных и выполнения запросов к БД.")
    parser.add_argument('--students', type=str, required=True, help="Путь к файлу студентов (students.json)")
    parser.add_argument('--rooms', type=str, required=True, help="Путь к файлу комнат (rooms.json)")
    parser.add_argument('--format', type=str, choices=['json', 'xml'], required=True, help="Выходной формат (json или xml)")
    
    args = parser.parse_args()
    
    db = DatabaseConnector()
    
    try:
        with db as cursor:
            loader = DataLoader(cursor)
            loader.load_data(args.rooms, args.students)
            
            executor = QueryExecutor(cursor)
            results = {
                "students_per_room": executor.get_students_per_room(),
                "top5_rooms_min_avg_age": executor.get_top5_rooms_by_min_avg_age(),
                "top5_rooms_max_age_diff": executor.get_top5_rooms_by_max_age_diff(),
                "rooms_with_mixed_genders": executor.get_rooms_with_mixed_genders()
            }
            
            formatter = get_formatter(args.format)
            output = formatter.format(results)
            print(output)
            
    except Exception as e:
        print(f"Произошла критическая ошибка: {e}")

if __name__ == "__main__":
    main()