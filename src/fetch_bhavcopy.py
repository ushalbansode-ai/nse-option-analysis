import requests
import datetime

BASE_URL = "https://archives.nseindia.com/content/fo/fo{date}bhav.DAT"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com"
}

def is_weekend(date):
    # Monday = 0 ... Sunday = 6
    return date.weekday() >= 5

def try_download(date_str):
    url = BASE_URL.format(date=date_str)
    print(f"Trying: {url}")
    resp = requests.get(url, headers=HEADERS)

    if resp.status_code == 200:
        print(f"✔ Found bhavcopy: {date_str}")
        return resp.content

    return None


def fetch_dat():
    """
    Auto-detects the latest available DAT bhavcopy:
    - Skips weekends
    - Looks back up to 7 days
    """

    today = datetime.datetime.now()

    # Try last 7 days
    for delta in range(0, 7):
        day = today - datetime.timedelta(days=delta)

        # Skip Saturday & Sunday
        if is_weekend(day):
            print(f"Skipping weekend: {day.strftime('%d-%m-%Y')}")
            continue

        date_str = day.strftime("%d%m%Y")
        data = try_download(date_str)

        if data:
            return data

    raise Exception("No bhavcopy found for last 7 days!")
    
