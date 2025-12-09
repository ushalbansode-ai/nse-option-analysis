class OptionBuyingStrategies:
    def find_breakout_opportunities(self, df, underlying_price):
        opportunities = []
        for _, row in df.iterrows():
            if row.get("composite_score", 0) > 0.6 and row.get("totalTradedVolume", 0) > 200:
                opportunities.append({
                    "optionType": row.get("optionType"),
                    "strikePrice": row.get("strikePrice"),
                    "lastPrice": row.get("lastPrice"),
                    "score": round(row.get("composite_score", 0), 4),
                    "oi_volume_ratio": round(row.get("oi_volume_ratio", 0), 4),
                    "bid_ask_spread": round(row.get("bid_ask_spread", 0), 4),
                    "distance_to_spot": round(abs(underlying_price - row.get("strikePrice", underlying_price)), 2),
                })
        return opportunities
        
