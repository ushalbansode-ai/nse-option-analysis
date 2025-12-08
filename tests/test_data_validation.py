import pytest
import pandas as pd
from src.data.data_processor import DataProcessor
from src.analysis.underrating_factors import UnderratingFactors

def test_basic_data_processing():
    processor = DataProcessor()
    
    sample_data = {
        'filtered': {
            'data': [
                {
                    'expiryDate': '2025-12-15',
                    'CE': {
                        'strikePrice': 21000,
                        'lastPrice': 150,
                        'openInterest': 1000,
                        'totalTradedVolume': 500,
                        'impliedVolatility': 0.25,
                        'bidPrice': 149,
                        'askPrice': 151,
                        'pC': 'CE'
                    }
                }
            ]
        },
        'timestamp': '2025-12-08T10:30:00',
        'underlyingValue': 21050.0
    }
    
    result = processor.process_option_chain(sample_data)
    
    assert 'calls' in result
    assert 'puts' in result
    assert result['underlying_value'] == 21050.0

def test_underrating_factors():
    analyzer = UnderratingFactors()
    
    df = pd.DataFrame({
        'strikePrice': [21000, 21100, 21200],
        'openInterest': [1000, 2000, 500],
        'totalTradedVolume': [500, 1000, 250],
        'bidPrice': [149, 100, 60],
        'askPrice': [151, 102, 62],
        'lastPrice': [150, 101, 61],
        'pC': ['CE', 'CE', 'CE']
    })
    
    df_with_oi_ratio = analyzer.analyze_oi_volume_ratio(df)
    assert 'oi_volume_ratio' in df_with_oi_ratio.columns
    
    df_with_spread = analyzer.analyze_bid_ask_spread(df_with_oi_ratio)
    assert 'bid_ask_spread' in df_with_spread.columns
