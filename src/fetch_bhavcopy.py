import requests
import datetime

def fetch_dat():
    today = datetime.datetime.now().strftime("%d%m%Y")
    url = f"https://www.nseindia.com/content/historical/DERIVATIVES/{today}/fo{today}bhav.csv.zip"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    print(f"Downloading: {url}")
    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        raise Exception("Failed to download DAT bhavcopy")

    return r.content
  
