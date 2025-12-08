import pandas as pd
import numpy as np
from typing import Dict
import json

class DataProcessor:
    def __init__(self):
        pass
    
    def process_option_chain(self, raw_data: Dict) -> Dict:
        # Validate input
        if not raw_data or 'filtered' not in raw_data:
            return {}
        
        filtered_data = raw_data['filtered']
        records = filtered_data.get('data', [])
        
        call_data = []
        put_data = []
        
        for record in records:
            expiry_date = record.get('expiryDate')
            
            if 'CE' in record:
                ce_data = record['CE']
                ce_data['expiryDate'] = expiry_date
                call_data.append(ce_data)
            
            if 'PE' in record:
                pe_data = record['PE']
                pe_data['expiryDate'] = expiry_date
                put_data.append(pe_data)
        
        calls_df = pd.DataFrame(call_data)
        puts_df = pd.DataFrame(put_data)
        
        return {
            'calls': calls_df,
            'puts': puts_df,
            'timestamp': raw_data.get('timestamp'),
            'underlying_value': raw_data.get('underlyingValue', 0.0)
        }
        
