"""
Configuration settings for the project
"""

import os

# Base directories (these will be created automatically)
BASE_DIR = "data"
OUT_DIR = "outputs"

# Create directories if they don't exist
def setup_directories():
    """Create all necessary directories"""
    directories = [
        BASE_DIR, 
        f"{BASE_DIR}/raw",           # Raw downloaded data
        f"{BASE_DIR}/processed",      # Processed data
        f"{OUT_DIR}/signals",         # Generated signals
        f"{OUT_DIR}/reports"          # Analysis reports
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

# Run directory setup when this module is imported
setup_directories()

# Strategy parameters
STRATEGY_PARAMS = {
    'price_change_threshold': 1.0,  # percentage
    'oi_change_threshold': 0.05,    # 5% of total OI
    'volume_spike_threshold': 1000000,
    'option_volume_threshold': 10000,
    'max_pain_threshold': 0.02,     # 2% from current price
}

# NSE URLs
NSE_URLS = {
    'fo_bhavcopy': "https://archives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip"
}
