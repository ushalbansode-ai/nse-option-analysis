import pandas as pd
import numpy as np
from typing import Dict, List

class OptionBuyingStrategies:
    def __init__(self):
        self.risk_tolerance = 0.02
    
    def find_breakout_opportunities(self, df: pd.DataFrame, underlying_price: float) -> List[Dict]:
        opportunities = []
        
        # Filter for high composite score options
        high_score_options = df[df['composite_score'] > 0.7].copy()
        
        for _, option in high_score_options.iterrows():
            is_otm = (option['pC'] == 'CE' and option['strikePrice'] > underlying_price) or \
                     (option['pC'] == 'PE' and option['strikePrice'] < underlying_price)
            
            if is_otm and option['impliedVolatility'] > 0.2:
                opportunity = {
                    'strike': option['strikePrice'],
                    'type': option['pC'],
                    'premium': option['lastPrice'],
                    'iv': option['impliedVolatility'],
                    'oi': option['openInterest'],
                    'volume': option['totalTradedVolume'],
                    'score': option['composite_score'],
                    'strategy': 'BREAKOUT_LONG',
                    'target_strike': self._calculate_target_strike(option, underlying_price),
                    'stop_loss': option['lastPrice'] * 0.5
                }
                opportunities.append(opportunity)
        
        return opportunities
    
    def _calculate_target_strike(self, option, underlying_price):
        if option['pC'] == 'CE':
            return option['strikePrice'] + 50
        else:
            return option['strikePrice'] - 50
