import pandas as pd


def generate_signals(df):
    """Generate trading signals based on price changes and volume."""
    if df.empty:
        return df
    
    # Clean column names (strip whitespace)
    df.columns = df.columns.str.strip()
    
    # Initialize signals column
    df["signal"] = "HOLD"
    
    # Check for available columns
    available_cols = df.columns.tolist()
    
    if "change" in available_cols:
        # Simple momentum signal
        df.loc[df["change"] > 0, "signal"] = "BUY"
        df.loc[df["change"] < 0, "signal"] = "SELL"
    
    # If we have volume data, add volume confirmation
    if "VOLUME" in available_cols and "change" in available_cols:
        # High volume + positive change = STRONG BUY
        df.loc[(df["change"] > 0) & (df["VOLUME"] > df["VOLUME"].mean()), "signal"] = "STRONG_BUY"
        df.loc[(df["change"] < 0) & (df["VOLUME"] > df["VOLUME"].mean()), "signal"] = "STRONG_SELL"
    
    return df[["symbol", "signal", "change"] + [col for col in available_cols if col not in ["symbol", "signal", "change"]]]
