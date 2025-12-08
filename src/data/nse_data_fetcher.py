import requests
import time
import random
from datetime import datetime
import json
import pandas as pd
from typing import Dict, Optional

class NSEDataFetcher:
    def __init__(self):
        self.base_url = "https://www.nseindia.com"
        self.session = requests.Session()
        self._setup_session()
        
    def _setup_session(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        self.session.headers.update(headers)
        
    def get_cookies(self):
        try:
            response = self.session.get(self.base_url)
            return response.cookies
        except Exception as e:
            print(f"Error getting cookies: {e}")
            return None
    
    def fetch_option_chain(self, symbol: str) -> Optional[Dict]:
        cookies = self.get_cookies()
        if not cookies:
            return None
            
        time.sleep(random.uniform(2, 4))  # Rate limiting
        
        url = f"{self.base_url}/api/option-chain-equities?symbol={symbol.upper()}"
        
        try:
            response = self.session.get(
                url,
                cookies=cookies,
                headers={'Referer': self.base_url}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error: {response.status_code}")
                return None
        except Exception as e:
            print(f"Exception: {e}")
            return None
