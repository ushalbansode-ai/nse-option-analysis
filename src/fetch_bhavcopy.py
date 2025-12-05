import os
import requests
import zipfile
from datetime import datetime

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"


def ensure_dirs():
    for d in [RAW_DIR, PROCESSED_DIR]:
        os.makedirs(d, exist_ok=True)


def fetch_bhavcopy():
    ensure_dirs()

    today = datetime.now().strftime("%d-%m-%Y")
    zip_name = f"bhav_{today}.zip"
    zip_path = os.path.join(RAW_DIR, zip_name)

    url = f"https://www1.nseindia.com/content/historical/DERIVATIVES/{today}/fo.zip"

    print(f"[INFO] Downloading bhavcopy for {today}")

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www1.nseindia.com/products/content/derivatives/equities/historical_fo.htm"
    }

    try:
        r = requests.get(url, headers=headers, timeout=20)
    except Exception as e:
        print("[ERROR] Network error:", e)
        return None

    # Save file
    with open(zip_path, "wb") as f:
        f.write(r.content)

    print("[INFO] ZIP saved:", zip_path)

    # ---------------------------------------------------------
    # VALIDATE ZIP — check if real zip or HTML error
    # ---------------------------------------------------------
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(RAW_DIR)
            files = z.namelist()
            print("[INFO] Extracted:", files)
            return max(files)  # return CSV name
    except zipfile.BadZipFile:
        print("[WARNING] NSE returned a NON-ZIP file (HTML error page)")
        print("[WARNING] Removing bad file:", zip_path)
        os.remove(zip_path)
        return None
        
