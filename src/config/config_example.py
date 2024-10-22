import os

BASE_DIR = ''

DATA_DIR_NAME = 'data'
PENDING_DIR_NAME = 'pending'
FINAL_DIR_NAME = 'final'
LOGS_DIR_NAME = 'logs'
CONFIG_DIR_NAME = 'src/config'
INPUT_DIR_NAME = 'input'
ARCHIVE_DIR_NAME = 'archive'

DATA_DIR = os.path.join(BASE_DIR, DATA_DIR_NAME)
PENDING_DIR = os.path.join(DATA_DIR, PENDING_DIR_NAME)
FINAL_DIR = os.path.join(DATA_DIR, FINAL_DIR_NAME)
INPUT_DIR = os.path.join(DATA_DIR, INPUT_DIR_NAME)
ARCHIVE_DIR = os.path.join(DATA_DIR, ARCHIVE_DIR_NAME)
CONFIG_DIR = os.path.join(BASE_DIR, CONFIG_DIR_NAME)
LOGS_DIR = os.path.join(BASE_DIR, LOGS_DIR_NAME)

CATEGORY_CONFIG_FILE = os.path.join(BASE_DIR, CONFIG_DIR_NAME, 'categories_example.json')
PENDING_FILE = os.path.join(PENDING_DIR,'transactions_pending.csv')
FINAL_FILE = os.path.join(FINAL_DIR,'transactions_final.csv')