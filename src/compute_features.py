import pandas as pd

def compute_features(df):
    df = df.copy()

    # Ensure numeric columns
    for col in ["OPEN", "HIGH", "LOW", "CLOSE", "OPEN_INT", "CHG_IN_OI"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # PRICE CHANGE %
    df["RET"] = (df["CLOSE"] - df["OPEN"]) / df["OPEN"] * 100

    # OI % CHANGE
    df["OI_PCT"] = df["CHG_IN_OI"] / df["OPEN_INT"].replace(0, pd.NA) * 100

    # SIMPLE MOMENTUM FLAG
    df["MOMENTUM"] = (df["CLOSE"] > df["OPEN"]).astype(int)

    return df
    
