import json

file_to_check = r'data\students.json'

try:
    with open(file_to_check, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"✅ Файл {file_to_check} в порядке!")
except Exception as e:
    print(f"❌ Ошибка в файле {file_to_check}: {e}")