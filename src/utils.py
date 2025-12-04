import pandas as pd
import datetime
import os

RAW_DIR = "data/raw/"
PROCESSED_DIR = "data/processed/"


def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)


def get_latest_two_files():
    ensure_dirs()
    files = sorted(os.listdir(RAW_DIR))
    csv_files = [f for f in files if f.endswith(".csv")]

    if len(csv_files) < 2:
        return None, None

    return csv_files[-2], csv_files[-1]


def load_csv(path):
    return pd.read_csv(path)
  
