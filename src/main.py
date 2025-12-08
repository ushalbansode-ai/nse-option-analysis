import time
import pandas as pd
from datetime import datetime
import json
import logging
from typing import Dict, List

from src.data.nse_data_fetcher import NSEDataFetcher
from src.data.data_processor import DataProcessor
from src.analysis.underrating_factors import UnderratingFactors
from src.strategies.option_buying_strategies import OptionBuyingStrategies

class OptionAnalysisEngine:
    def __init__(self):
        self.fetcher = NSEDataFetcher()
        self.processor = DataProcessor()
        self.underrating_analyzer = UnderratingFactors()
        self.strategies = OptionBuyingStrategies()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('option_analysis.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def run_analysis(self, symbol: str) -> Dict:
        self.logger.info(f"Starting analysis for {symbol}")
        
        try:
            raw_data = self.fetcher.fetch_option_chain(symbol)
            if not raw_data:
                self.logger.error(f"Failed to fetch data for {symbol}")
                return {}
            
            processed_data = self.processor.process_option_chain(raw_data)
            if not processed_data:
                self.logger.error(f"Failed to process data for {symbol}")
                return {}
            
            calls = processed_data['calls']
            puts = processed_data['puts']
            
            # Add underrated factors analysis
            calls = self.underrating_analyzer.analyze_oi_volume_ratio(calls)
            calls = self.underrating_analyzer.analyze_bid_ask_spread(calls)
            calls = self.underrating_analyzer.analyze_oi_concentration(calls)
            
            puts = self.underrating_analyzer.analyze_oi_volume_ratio(puts)
            puts = self.underrating_analyzer.analyze_bid_ask_spread(puts)
            puts = self.underrating_analyzer.analyze_oi_concentration(puts)
            
            # Calculate composite scores (simplified for now)
            calls['composite_score'] = 0.5  # Placeholder - implement proper scoring
            puts['composite_score'] = 0.5   # Placeholder - implement proper scoring
            
            # Find trading opportunities
            underlying_price = processed_data['underlying_value']
            breakout_calls = self.strategies.find_breakout_opportunities(calls, underlying_price)
            breakout_puts = self.strategies.find_breakout_opportunities(puts, underlying_price)
            
            all_opportunities = breakout_calls + breakout_puts
            # Sort by score (implement proper ranking)
            ranked_opportunities = sorted(all_opportunities, key=lambda x: x['score'], reverse=True)
            
            result = {
                'symbol': symbol,
                'timestamp': processed_data['timestamp'],
                'underlying_price': underlying_price,
                'calls': calls.to_dict('records'),
                'puts': puts.to_dict('records'),
                'opportunities': ranked_opportunities[:10],
                'summary': {
                    'total_calls': len(calls),
                    'total_puts': len(puts),
                    'high_score_calls': len(calls[calls['composite_score'] > 0.7]),
                    'high_score_puts': len(puts[puts['composite_score'] > 0.7]),
                    'total_opportunities': len(ranked_opportunities)
                }
            }
            
            self.logger.info(f"Analysis completed for {symbol}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error in analysis for {symbol}: {str(e)}")
            return {}

if __name__ == "__main__":
    import os
    os.makedirs('results', exist_ok=True)
    
    engine = OptionAnalysisEngine()
    
    # Test with NIFTY
    result = engine.run_analysis('NIFTY')
    
    if result:
        filename = f"results/nifty_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"Results saved to {filename}")
    else:
        print("No results generated")
        
