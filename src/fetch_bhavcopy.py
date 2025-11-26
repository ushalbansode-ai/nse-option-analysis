import requests
import datetime
from io import BytesIO
import zipfile

BASE_URL = "https://archives.nseindia.com/content/fo/"


def try_download(url):
    print(f"Trying: {url}")
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    if resp.status_code == 200:
        return resp.content
    return None


def fetch_bhavcopy():
    today = datetime.date.today()

    for i in range(7):  # try last 7 days
        d = today - datetime.timedelta(days=i)

        if d.weekday() >= 5:   # skip Sat-Sun
            print(f"Skipping weekend: {d}")
            continue

        dd = d.strftime("%d")
        mm = d.strftime("%m")
        yy = d.strftime("%Y")

        ########## NEW 2025 FILE NAME PATTERNS ##########

        # 1️⃣ NEW NSE DAT file format
        dat_name = f"FNO_BC{dd}{mm}{yy}.DAT"
        dat_url = BASE_URL + dat_name

        # 2️⃣ NEW ZIP Bhavcopy format
        zip_name = f"BhavCopy_NSE_FO_0_0_0_{yy}{mm}{dd}_F_0000.csv.zip"
        zip_url = BASE_URL + zip_name

        ########## Try DAT ##########
        dat_file = try_download(dat_url)
        if dat_file:
            print("✓ DAT file found")
            return dat_file

        ########## Try ZIP ##########
        zip_file = try_download(zip_url)
        if zip_file:
            print("✓ ZIP file found — extracting")
            z = zipfile.ZipFile(BytesIO(zip_file))
            first_csv = z.namelist()[0]
            return z.read(first_csv)

    raise Exception("No DAT or ZIP bhavcopy found in last 7 days.")
    
