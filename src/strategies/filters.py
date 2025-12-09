import pandas as pd

class Filters:
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Remove absurd spreads and zero prices
        df = df[(df["lastPrice"] > 0) & (df["bid_ask_spread"] >= 0) & (df["bid_ask_spread"] <= 0.15)]
        return df
      
