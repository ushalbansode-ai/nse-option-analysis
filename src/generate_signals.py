import pandas as pd
import numpy as np

# -------------------------------------------------------------------
# SIMPLE, CLEAN SIGNAL GENERATOR
# Works with your computed features:
#   - VOLUME_SPIKE
#   - REVERSAL
#   - PREMIUM_DISCOUNT
#   - DELTA_VOLUME
# -------------------------------------------------------------------

def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Generate combined buy/sell signals from computed features."""

    # Ensure columns exist
    required_cols = [
        "VOLUME_SPIKE",
        "REVERSAL",
        "PREMIUM_DISCOUNT",
        "DELTA_VOLUME",
        "CLOSE"
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan

    # -------------------------------------------------------------------
    # BUY / SELL LOGIC
    # -------------------------------------------------------------------

    # BUY SIGNAL
    df["BUY_SIGNAL"] = (
        (df["VOLUME_SPIKE"] == 1) &
        (df["REVERSAL"] == 1) &
        (df["DELTA_VOLUME"] > 0)
    ).astype(int)

    # SELL SIGNAL
    df["SELL_SIGNAL"] = (
        (df["VOLUME_SPIKE"] == 1) &
        (df["REVERSAL"] == -1) &
        (df["DELTA_VOLUME"] < 0)
    ).astype(int)

    # -------------------------------------------------------------------
    # SIGNAL SCORE (weighted)
    # -------------------------------------------------------------------
    df["SIGNAL_SCORE"] = (
        (df["VOLUME_SPIKE"] * 2) +
        (df["REVERSAL"] * 2) +
        (np.sign(df["DELTA_VOLUME"]) * 1)
    )

    # -------------------------------------------------------------------
    # FINAL ACTION TEXT
    # -------------------------------------------------------------------
    def to_action(row):
        if row["BUY_SIGNAL"] == 1:
            return "BUY"
        if row["SELL_SIGNAL"] == 1:
            return "SELL"
        return "HOLD"

    df["ACTION"] = df.apply(to_action, axis=1)

    return df
  
