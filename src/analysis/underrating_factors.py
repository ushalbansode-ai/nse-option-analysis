import pandas as pd
import numpy as np
from typing import Dict

class UnderratingFactors:
    def __init__(self):
        pass
    
    def analyze_oi_volume_ratio(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['oi_volume_ratio'] = df['openInterest'] / (df['totalTradedVolume'] + 1)
        df['oi_volume_signal'] = df['oi_volume_ratio'].apply(
            lambda x: 'NEW_POSITION' if x > 5 else ('NORMAL' if x > 1 else 'CLOSE_POSITION')
        )
        return df
    
    def analyze_bid_ask_spread(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['bid_ask_spread'] = df['askPrice'] - df['bidPrice']
        df['spread_percentage'] = (df['bid_ask_spread'] / df['lastPrice']) * 100
        df['liquidity_level'] = df['spread_percentage'].apply(
            lambda x: 'HIGH' if x <= 2 else ('MEDIUM' if x <= 5 else 'LOW')
        )
        return df
    
    def analyze_oi_concentration(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        total_oi = df['openInterest'].sum()
        df['oi_percentage'] = (df['openInterest'] / total_oi) * 100
        df['oi_concentration'] = df['oi_percentage'].apply(
            lambda x: 'HIGH' if x > 2 else ('MEDIUM' if x > 0.5 else 'LOW')
        )
        return df
