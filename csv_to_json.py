import csv
import json
import sys

def csv_to_json(csv_file_path):
    """Convert CSV file to JSON lines format"""
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            yield json.dumps(row)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python csv_to_json.py <input_csv_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    for json_line in csv_to_json(input_file):
        print(json_line)