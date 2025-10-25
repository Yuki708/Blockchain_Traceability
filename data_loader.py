# client/data_loader.py
import csv
import random

def load_email_data(filepath, limit=100):
    data = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= limit:
                break
            data.append({
                'id': row['id'],
                'from': row['from'],
                'to': row['to'],
                'size': row['size']
            })
    return data
