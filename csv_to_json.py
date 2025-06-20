# Для вызова python csv_to_json.py

import csv
import json

# Путь к входному CSV-файлу
input_csv = "data/test_data.csv"
# Путь к выходному JSON-файлу
output_json = "data/test_data_lines.json"

# Преобразование CSV в JSON с сохранением каждой строки как отдельного JSON-объекта
with open(input_csv, mode="r", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    with open(output_json, mode="w", encoding="utf-8") as jsonfile:
        for row in reader:
            # Записываем каждую строку как отдельный JSON-объект
            jsonfile.write(json.dumps(row) + "\n")

print(f"CSV успешно преобразован в JSON с построчной записью: {output_json}")