import pandas as pd
from src.utils.helpers import safe_div

class UnderratedFactors:
    def analyze_oi_volume_ratio(self, df: pd.DataFrame) -> pd.DataFrame:
        df["oi_volume_ratio"] = df.apply(
            lambda r: safe_div(r.get("openInterest", 0), r.get("totalTradedVolume", 0), 0.0),
            axis=1
        )
        return df

    def analyze_bid_ask_spread(self, df: pd.DataFrame) -> pd.DataFrame:
        df["bid_ask_spread"] = df.apply(
            lambda r: safe_div((r.get("askPrice", 0) - r.get("bidPrice", 0)), r.get("lastPrice", 1), 0.0),
            axis=1
        )
        return df

    def analyze_oi_concentration(self, df: pd.DataFrame) -> pd.DataFrame:
        total_oi = df["openInterest"].sum() if "openInterest" in df.columns else 0
        df["oi_concentration"] = df["openInterest"].apply(lambda x: safe_div(x, total_oi, 0.0)) if total_oi else 0.0
        return df
      
