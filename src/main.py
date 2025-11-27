import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import zipfile

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ----------------------------
# 1) DOWNLOAD NSE F&O BHAVCOPY
# ----------------------------

def download_bhavcopy(date):
    """Download NSE F&O Bhavcopy for a specific date."""
    date_str = date.strftime("%d%m%Y")
    filename = f"fo{date_str}bhav.csv.zip"
    url = f"https://archives.nseindia.com/content/historical/DERIVATIVES/{date.strftime('%Y')}/{date.strftime('%b').upper()}/{filename}"

    print(f"Trying: {url}")

    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    if resp.status_code != 200:
        print(f"Failed: {resp.status_code}")
        return None

    zip_path = os.path.join(OUTPUT_DIR, filename)
    with open(zip_path, "wb") as f:
        f.write(resp.content)

    print("Downloaded:", url)

    # Extract CSV file
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(OUTPUT_DIR)
        csv_name = z.namelist()[0]
        print("Extracted:", csv_name)

    return os.path.join(OUTPUT_DIR, csv_name)


# --------------------------------
# 2) COMPUTE SIGNALS (FIXED LOGIC)
# --------------------------------

def compute_signals(df):
    required = ["SYMBOL", "INSTRUMENT", "EXPIRY_DT", "OPEN", "CLOSE",
                "OPEN_INT", "CHG_IN_OI", "CONTRACTS"]

    for col in required:
        if col not in df.columns:
            raise Exception(f"Missing column: {col}")

    df["OI_Change"] = df["CHG_IN_OI"]
    df["Volume"] = df["CONTRACTS"]

    df["Signal"] = "Neutral"

    # Long Build-up: Price ↑ and OI ↑
    df.loc[(df["CLOSE"] > df["OPEN"]) & (df["CHG_IN_OI"] > 0),
           "Signal"] = "Long Buildup"

    # Short Build-up: Price ↓ and OI ↑
    df.loc[(df["CLOSE"] < df["OPEN"]) & (df["CHG_IN_OI"] > 0),
           "Signal"] = "Short Buildup"

    # Long Unwinding: Price ↓ and OI ↓
    df.loc[(df["CLOSE"] < df["OPEN"]) & (df["CHG_IN_OI"] < 0),
           "Signal"] = "Long Unwinding"

    # Short Covering: Price ↑ and OI ↓
    df.loc[(df["CLOSE"] > df["OPEN"]) & (df["CHG_IN_OI"] < 0),
           "Signal"] = "Short Covering"

    return df


# ------------------------------
# 3) MAIN EXECUTION FLOW
# ------------------------------

def main():
    print("Fetching latest NSE F&O bhavcopy...")

    today = datetime.today()
    bhav_path = None

    # Try last 3 trading days automatically
    for i in range(0, 3):
        d = today - timedelta(days=i)
        try_file = download_bhavcopy(d)
        if try_file:
            bhav_path = try_file
            break

    if not bhav_path:
        raise Exception("Could not download any bhavcopy file!")

    print("Loading:", bhav_path)
    df = pd.read_csv(bhav_path)

    print("Rows:", len(df))

    print("Computing signals...")
    df = compute_signals(df)

    # Save CSV + JSON
    out_csv = os.path.join(OUTPUT_DIR, "fno_signals.csv")
    out_json = os.path.join(OUTPUT_DIR, "fno_signals.json")

    df.to_csv(out_csv, index=False)
    df.to_json(out_json, orient="records", indent=2)

    print("Saved:", out_csv)
    print("Saved:", out_json)
    print("Completed.")


if __name__ == "__main__":
    main()
    
