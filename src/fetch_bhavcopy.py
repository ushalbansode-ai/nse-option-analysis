import os
import requests
import datetime
import pandas as pd


NSE_URL = "https://www.nseindia.com/api/market-data-pre-open?key=FO"


def download_bhavcopy(output_folder):
    """Download F&O bhavcopy from NSE API."""
    today = datetime.datetime.now().strftime("%Y%m%d")
    filename = f"bhavcopy_{today}.csv"
    full_path = os.path.join(output_folder, filename)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    try:
        r = requests.get(NSE_URL, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()

        if "data" not in data or "fno" not in data["data"]:
            print("❌ FO data missing")
            return None

        df = pd.DataFrame(data["data"]["fno"])
        df.to_csv(full_path, index=False)

        print(f"✅ Bhavcopy downloaded → {full_path}")
        return full_path

    except Exception as e:
        print("❌ Error downloading bhavcopy:", e)
        return None
        
