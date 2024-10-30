import os
import hashlib
import json
import yaml
from ..config.config import CATEGORY_CONFIG_FILE
import pandas as pd
from ..utils.logger import logger


def load_known_vendors():
    if os.path.exists(CATEGORY_CONFIG_FILE):
        return load_from_json(CATEGORY_CONFIG_FILE).get("known_vendors", {}) # todo: move path check to lower fn, change to error catch
    else:
        logger.error("No category found at {}".format(CATEGORY_CONFIG_FILE))
        return {}


def load_category_list(category_types=["expense", "income", "other"]):
    list = []
    if os.path.exists(CATEGORY_CONFIG_FILE):
        categories = load_from_json(CATEGORY_CONFIG_FILE).get("list", {})
        for type in category_types:
            if type in categories:
                list += categories[type]
    return list


def hash_row(row, hash_columns):
    row_data = "".join(str(row[col]) for col in hash_columns)
    return hashlib.md5(row_data.encode()).hexdigest()


def find_duplicate_rows(df, column):
    return df[df.duplicated(subset=[column], keep=False)]

def extract_csv_data(file_name):
    return pd.read_csv(file_name)

def load_from_json(file_name):
    with open(file_name, "r") as file:
        return json.load(file)
    
def load_from_yaml(file_name):
    with open(file_name, "r") as file:
        return yaml.safe_load(file)
