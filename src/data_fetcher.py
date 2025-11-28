"""
Data fetching module for NSE bhavcopy
"""

import os
import requests
import zipfile
import datetime
from config.settings import BASE_DIR

class DataFetcher:
    def __init__(self):
        self.base_url = (
            "https://archives.nseindia.com/content/fo/"
            "BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip"
        )
    
    def fetch_latest_bhavcopy(self):
        """Fetch latest derivatives bhavcopy from NSE"""
        print("Fetching latest bhavcopy...")

        today = datetime.date.today()
        tried = []

        for i in range(0, 4):
            d = today - datetime.timedelta(days=i)
            date_str = d.strftime("%Y%m%d")

            url = self.base_url.format(date=date_str)
            tried.append(url)

            print(f"Trying: {url}")

            try:
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)

                if r.status_code == 200:
                    zip_path = f"{BASE_DIR}/raw/Bhav_{date_str}.zip"
                    os.makedirs(os.path.dirname(zip_path), exist_ok=True)
                    
                    with open(zip_path, "wb") as f:
                        f.write(r.content)
                    print(f"Downloaded: {url}")

                    # Extract ZIP
                    with zipfile.ZipFile(zip_path, "r") as z:
                        z.extractall(f"{BASE_DIR}/raw/")
                        extracted_file = z.namelist()[0]

                    print(f"Extracted: {extracted_file}")
                    return f"{BASE_DIR}/raw/{extracted_file}"

                else:
                    print(f"Failed: {r.status_code}")

            except requests.RequestException as e:
                print(f"Request failed: {e}")

        print("Tried URLs:")
        for u in tried:
            print(f" - {u}")

        raise Exception("No bhavcopy found for last 4 days")
