import os
import pandas as pd


def ensure_folder(path):
    """Create folder if not exists."""
    os.makedirs(path, exist_ok=True)


def load_csv_safely(path):
    """
    Load CSV file safely.
    If file missing → return empty DataFrame.
    """
    if not os.path.exists(path):
        print(f"⚠️ CSV not found: {path}")
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"❌ Error loading CSV {path} → {e}")
        return pd.DataFrame()


def save_csv_safely(df, path):
    """Save CSV safely."""
    try:
        df.to_csv(path, index=False)
        print(f"✅ Saved: {path}")
    except Exception as e:
        print(f"❌ Failed to save CSV {path}: {e}")
        
