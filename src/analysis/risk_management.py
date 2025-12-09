import pandas as pd

class RiskManagement:
    def apply_liquidity_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Basic liquidity guards
        df = df[(df["totalTradedVolume"] >= 100) & (df["openInterest"] >= 1000)]
        return df

    def position_sizing(self, budget_per_trade: float, last_price: float) -> int:
        if last_price <= 0:
            return 0
        # Simple sizing: risk-aware lot calculation (placeholder)
        units = int(budget_per_trade // last_price)
        return max(units, 0)
      
