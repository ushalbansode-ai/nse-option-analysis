class ScoringEngine:
    def compute_scores(self, df):
        df["composite_score"] = (
            0.5 * (1 / (1 + df["oi_volume_ratio"])) +
            0.3 * (1 - df["bid_ask_spread"].clip(0,1)) +
            0.2 * df["oi_concentration"]
        )
        return df
        
