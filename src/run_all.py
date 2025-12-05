import os
import pandas as pd

from fetch_bhavcopy import download_bhavcopy
from utils import load_csv_safely
from compare_engine import detect_signals
from signal_engine import save_signals
from scripts.build_dashboard import build_html_dashboard


RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
SIGNALS_DIR = "data/signals"


def ensure_dirs():
    for d in [RAW_DIR, PROCESSED_DIR, SIGNALS_DIR]:
        os.makedirs(d, exist_ok=True)


def run_pipeline():
    print("🔽 Downloading today BhavCopy...")
    today_csv = download_bhavcopy(RAW_DIR)
    if today_csv is None:
        print("❌ ERROR: Bhavcopy download failed.")
        return

    print("📥 Loading today CSV...")
    today_df = load_csv_safely(today_csv)
    if today_df is None or today_df.empty:
        print("❌ ERROR: Today CSV empty or unreadable.")
        return

    # Find previous processed file
    prev_files = sorted(os.listdir(PROCESSED_DIR))
    prev_df = pd.DataFrame()

    if prev_files:
        prev_csv = os.path.join(PROCESSED_DIR, prev_files[-1])
        print(f"📥 Loading previous file: {prev_csv}")
        prev_df = load_csv_safely(prev_csv)

    # Save today's file as processed snapshot
    processed_path = os.path.join(PROCESSED_DIR, os.path.basename(today_csv))
    today_df.to_csv(processed_path, index=False)

    print("📊 Comparing today vs previous & generating signals...")
    signals_df = detect_signals(prev_df, today_df)

    signals_path = os.path.join(SIGNALS_DIR, "signals_latest.csv")
    save_signals(signals_df, signals_path)

    print("🌐 Rebuilding dashboard...")
    build_html_dashboard(signals_df)

    print("✅ DONE — Pipeline completed successfully!")


if __name__ == "__main__":
    ensure_dirs()
    run_pipeline()
    
