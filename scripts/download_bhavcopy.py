#!/usr/bin/env python3
# scripts/download_bhavcopy.py
import os
import requests
from datetime import datetime, timedelta
import zipfile
from io import BytesIO
import time


RAW_DIR = "data/raw"
ZIP_DIR = os.path.join(RAW_DIR, "zips")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(ZIP_DIR, exist_ok=True)
HEADERS = {
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
"Accept-Language": "en-US,en;q=0.9",
"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
"Referer": "https://www.nseindia.com/"
}




def get_bhavcopy_url(date):
d = date.strftime("%Y%m%d")
return f"https://www1.nseindia.com/content/fo/UDiFF_Common_Bhavcopy_FO_{d}.zip"




def try_download_for_date(date):
url = get_bhavcopy_url(date)
print(f"Trying {url}")
try:
with requests.Session() as s:
s.headers.update(HEADERS)
s.get("https://www.nseindia.com", timeout=10)
time.sleep(0.5)
r = s.get(url, timeout=20)
if r.status_code != 200:
print("Status:", r.status_code)
return False


z = zipfile.ZipFile(BytesIO(r.content))
csv_names = [n for n in z.namelist() if n.lower().endswith('.csv')]
if not csv_names:
print("No CSV inside zip.")
return False


zipname = os.path.join(ZIP_DIR, f"UDiFF_{date.strftime('%Y%m%d')}.zip")
with open(zipname, "wb") as f:
f.write(r.content)
print("Saved zip:", zipname)


csvname = csv_names[0]
out_csv = os.path.join(RAW_DIR, f"{date.strftime('%Y-%m-%d')}.csv")
with z.open(csvname) as src, open(out_csv, "wb") as dst:
dst.write(src.read())
print("Saved csv:", out_csv)
return True
except Exception as e:
print("Error:", e)
return False
def fetch_latest(max_lookback_days=7):
today = datetime.now()
date = today
for i in range(max_lookback_days):
if try_download_for_date(date):
return True
date = date - timedelta(days=1)
return False




if __name__ == "__main__":
ok = fetch_latest(7)
if not ok:
print("Failed to fetch bhavcopy in last 7 days.")
