import pandas as pd


def compare_with_previous(latest_df, previous_df):
    """Simple comparison logic."""
    if previous_df.empty:
        latest_df["change"] = 0
        return latest_df

    merged = latest_df.merge(
        previous_df,
        on="symbol",
        how="left",
        suffixes=("", "_prev")
    )

    merged["change"] = merged["close"] - merged["close_prev"].fillna(merged["close"])
    return merged
    
