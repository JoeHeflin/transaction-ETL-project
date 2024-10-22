import os
import hashlib
import json
from config.config import CATEGORY_CONFIG_FILE

def load_known_vendors():    
    if os.path.exists(CATEGORY_CONFIG_FILE):
        with open(CATEGORY_CONFIG_FILE, 'r') as file:
            return json.load(file).get('known_vendors', {})
    return {}

def load_category_list():
    if os.path.exists(CATEGORY_CONFIG_FILE):
        with open(CATEGORY_CONFIG_FILE, 'r') as file:
            categories = json.load(file)
            expense = categories.get('list', {}).get('expense', [])
            income = categories.get('list', {}).get('income', [])
            other = categories.get('list', {}).get('other', [])
            return expense + income + other
    return []


def hash_row(row, hash_columns):
    row_data = ''.join(str(row[col]) for col in hash_columns)
    return hashlib.md5(row_data.encode()).hexdigest()

def find_duplicate_rows(df, column):
    return df[df.duplicated(subset=[column], keep=False)]
