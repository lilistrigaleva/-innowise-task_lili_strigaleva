import json

class DataLoader:
    def __init__(self, cursor):
        self.cursor = cursor       
        
    def load_data(self, rooms_path: str, students_path: str):
        self._load_rooms(rooms_path)
        self._load_students(students_path)
        print("Данные успешно загружены.")
        
    def _load_rooms(self, file_path: str):
        with open(file_path, 'r', encoding = 'UTF-8') as f:
            rooms = json.load(f)
            
        insert_query = "INSERT INTO rooms (id, name) VALUES (%s, %s);"
        data_to_insert = [(room['id'], room['name'],) for room in rooms]
        
        self.cursor.executemany(insert_query, data_to_insert)
        print(f"Загружено {len(data_to_insert)} комнат.")
        
    def _load_students(self, file_path: str):
        with open(file_path, 'r', encoding = 'UTF-8') as f:
            students = json.load(f)
            
        insert_query = "INSERT INTO students (id, name, birthday, sex, room_id) VALUES (%s, %s, %s, %s, %s);"
        data_to_insert = [
            (
                s['id'],
                s['name'],
                s['birthday'],
                s['sex'],
                s['room']
                
            ) for s in students
        ]
        self.cursor.executemany(insert_query, data_to_insert)
        print(f"Закружено {len(data_to_insert)} студентов.")
        
        