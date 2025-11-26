import pandas as pd
import io

def parse_dat_bytes(raw_bytes):
    txt = raw_bytes.decode("utf-8", errors="ignore")

    df = pd.read_csv(
        io.StringIO(txt),
        sep="|"
    )

    df.columns = df.columns.str.strip()
    return df
    
