"""
Data processing module for futures and options data - NSE BhavCopy Version
"""

import pandas as pd
import numpy as np
import re
from collections import defaultdict

class DataProcessor:
    def __init__(self):
        pass
    
    def load_data(self, csv_path):
        """Load CSV data into DataFrame"""
        print(f"Loading data from: {csv_path}")
        try:
            df = pd.read_csv(csv_path)
            print(f"✅ Loaded {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return pd.DataFrame()
    
    def separate_futures_options(self, df):
        """Separate futures and options data using NSE BhavCopy format"""
        futures_data = []
        options_data = []
        
        print("🔧 Parsing NSE BhavCopy instruments...")
        
        if df.empty:
            print("❌ Empty dataframe provided")
            return pd.DataFrame(), pd.DataFrame()
        
        # Check if we have the instrument type column
        instrument_col = 'FinInstrmTp'
        if instrument_col not in df.columns:
            print(f"❌ Instrument column '{instrument_col}' not found")
            print(f"✅ Available columns: {df.columns.tolist()}")
            return pd.DataFrame(), pd.DataFrame()
        
        print(f"📊 Unique instrument types: {df[instrument_col].unique()}")
        
        # Convert instrument type to string for comparison
        df[instrument_col] = df[instrument_col].astype(str)
        
        # NSE Futures: STF (Stock Futures), IDF (Index Futures)
        futures_mask = (
            df[instrument_col].str.contains('FUT', case=False, na=False) |
            df[instrument_col].str.contains('STF', case=False, na=False) |
            df[instrument_col].str.contains('IDF', case=False, na=False)
        )
        
        # NSE Options: STO (Stock Options), IDO (Index Options)
        options_mask = (
            df[instrument_col].str.contains('OPT', case=False, na=False) |
            df[instrument_col].str.contains('STO', case=False, na=False) |
            df[instrument_col].str.contains('IDO', case=False, na=False)
        )
        
        # Also check for CE/PE in option type column if available
        if 'OptnTp' in df.columns:
            ce_pe_mask = df['OptnTp'].isin(['CE', 'PE'])
            options_mask = options_mask | ce_pe_mask
            print(f"📊 Option types found: {df[ce_pe_mask]['OptnTp'].value_counts().to_dict()}")
        
        # Apply masks
        if futures_mask.any():
            futures_df = df[futures_mask].copy()
            print(f"✅ Futures contracts found: {len(futures_df)}")
            if 'TckrSymb' in futures_df.columns:
                print(f"📊 Futures sample: {futures_df['TckrSymb'].head(5).tolist()}")
        else:
            futures_df = pd.DataFrame()
            print("❌ No futures contracts found")
        
        if options_mask.any():
            options_df = df[options_mask].copy()
            print(f"✅ Options contracts found: {len(options_df)}")
            if 'TckrSymb' in options_df.columns:
                print(f"📊 Options sample: {options_df['TckrSymb'].head(5).tolist()}")
            
            # Show option type distribution
            if 'OptnTp' in options_df.columns:
                optn_type_counts = options_df['OptnTp'].value_counts()
                print(f"📊 Option type distribution: {optn_type_counts.to_dict()}")
        else:
            options_df = pd.DataFrame()
            print("❌ No options contracts found")
        
        return futures_df, options_df
    
    def parse_instrument_symbol(self, symbol):
        """Parse instrument symbol to extract underlying, expiry, type, strike - NSE Format"""
        symbol_str = str(symbol).strip()
        
        # NSE Futures pattern: RELIANCE26DEC2024FUT, NIFTY26DEC2024FUT
        fut_pattern = r'^([A-Z]+)(\d{2}[A-Z]{3}\d{4})FUT$'
        fut_match = re.match(fut_pattern, symbol_str)
        
        if fut_match:
            underlying = fut_match.group(1)
            expiry_str = fut_match.group(2)
            return {
                'instrument_type': 'FUT',
                'underlying': underlying,
                'expiry_str': expiry_str,
                'strike': None,
                'option_type': None
            }
        
        # NSE Options pattern: RELIANCE26DEC202418000CE, NIFTY26DEC202418000PE
        opt_pattern = r'^([A-Z]+)(\d{2}[A-Z]{3}\d{4})(\d+)(CE|PE)$'
        opt_match = re.match(opt_pattern, symbol_str)
        
        if opt_match:
            underlying = opt_match.group(1)
            expiry_str = opt_match.group(2)
            strike = float(opt_match.group(3))
            option_type = opt_match.group(4)
            
            return {
                'instrument_type': 'OPT',
                'underlying': underlying,
                'expiry_str': expiry_str,
                'strike': strike,
                'option_type': option_type
            }
        
        return {'instrument_type': 'UNKNOWN', 'underlying': symbol_str, 'strike': None, 'option_type': None}
    
    def create_option_chain(self, options_df, futures_df):
        """Create option chain structure for analysis using NSE columns"""
        print("🔗 Building option chain from NSE data...")
        
        option_chain = defaultdict(lambda: defaultdict(dict))
        underlying_prices = {}
        
        # Get current prices from futures for each underlying
        print("💰 Extracting underlying prices from futures...")
        for _, future in futures_df.iterrows():
            symbol = future['TckrSymb']
            parsed = self.parse_instrument_symbol(symbol)
            underlying = parsed['underlying']
            
            # Use LastPric column for current price
            if 'LastPric' in future and pd.notna(future['LastPric']):
                underlying_prices[underlying] = float(future['LastPric'])
                print(f"   💰 {underlying}: {underlying_prices[underlying]}")
        
        # If no futures prices, try to use UndrlygPric from options
        if not underlying_prices and 'UndrlygPric' in options_df.columns:
            print("💰 Using UndrlygPric from options data...")
            for _, option in options_df.iterrows():
                symbol = option['TckrSymb']
                parsed = self.parse_instrument_symbol(symbol)
                underlying = parsed['underlying']
                
                if underlying not in underlying_prices and pd.notna(option['UndrlygPric']):
                    underlying_prices[underlying] = float(option['UndrlygPric'])
                    print(f"   💰 {underlying}: {underlying_prices[underlying]}")
        
        # Organize options by underlying, expiry, strike, and type
        print("📊 Organizing options into chain structure...")
        for _, option in options_df.iterrows():
            symbol = option['TckrSymb']
            parsed = self.parse_instrument_symbol(symbol)
            underlying = parsed['underlying']
            strike = parsed['strike']
            option_type = parsed['option_type']
            expiry = parsed.get('expiry_str', 'UNKNOWN')
            
            if underlying not in underlying_prices:
                continue
            
            current_price = underlying_prices[underlying]
            
            # Calculate moneyness
            if strike and current_price:
                distance_from_spot = abs(current_price - strike)
                is_ATM = (distance_from_spot / current_price) < 0.02  # Within 2%
                is_ITM = (
                    (option_type == 'CE' and strike < current_price) or 
                    (option_type == 'PE' and strike > current_price)
                )
            else:
                distance_from_spot = 0
                is_ATM = False
                is_ITM = False
            
            # Store option data in chain
            option_chain[underlying][expiry][(strike, option_type)] = {
                'last_price': float(option['LastPric']) if 'LastPric' in option and pd.notna(option['LastPric']) else 0,
                'oi': int(option['OpnIntrst']) if 'OpnIntrst' in option and pd.notna(option['OpnIntrst']) else 0,
                'oi_change': int(option['ChngInOpnIntrst']) if 'ChngInOpnIntrst' in option and pd.notna(option['ChngInOpnIntrst']) else 0,
                'volume': int(option['TtlTradgVol']) if 'TtlTradgVol' in option and pd.notna(option['TtlTradgVol']) else 0,
                'prev_close': float(option['PrvsClsgPric']) if 'PrvsClsgPric' in option and pd.notna(option['PrvsClsgPric']) else 0,
                'distance_from_spot': distance_from_spot,
                'is_ATM': is_ATM,
                'is_ITM': is_ITM,
                'underlying_price': current_price
            }
        
        print(f"✅ Option chain created for {len(option_chain)} underlyings")
        
        # Print chain summary
        for underlying, expiries in list(option_chain.items())[:3]:  # Show first 3
            total_options = sum(len(strikes) for strikes in expiries.values())
            print(f"   📈 {underlying}: {len(expiries)} expiries, {total_options} options")
        
        return option_chain, underlying_prices
    
    def enhance_futures_data(self, futures_df):
        """Add calculated fields to futures data"""
        if futures_df.empty:
            return futures_df
        
        enhanced_df = futures_df.copy()
        
        # Calculate price change if previous close is available
        if 'PrvsClsgPric' in enhanced_df.columns and 'LastPric' in enhanced_df.columns:
            enhanced_df['price_change'] = enhanced_df['LastPric'] - enhanced_df['PrvsClsgPric']
            enhanced_df['price_change_pct'] = (enhanced_df['price_change'] / enhanced_df['PrvsClsgPric']) * 100
        
        # Calculate OI change percentage if available
        if 'ChngInOpnIntrst' in enhanced_df.columns and 'OpnIntrst' in enhanced_df.columns:
            enhanced_df['oi_change_pct'] = (enhanced_df['ChngInOpnIntrst'] / enhanced_df['OpnIntrst']) * 100
        
        print(f"✅ Enhanced {len(enhanced_df)} futures contracts with calculated fields")
        return enhanced_df
    
    def enhance_options_data(self, options_df):
        """Add calculated fields to options data"""
        if options_df.empty:
            return options_df
        
        enhanced_df = options_df.copy()
        
        # Parse instrument symbols to extract underlying, strike, etc.
        enhanced_df['parsed_symbol'] = enhanced_df['TckrSymb'].apply(self.parse_instrument_symbol)
        enhanced_df['underlying'] = enhanced_df['parsed_symbol'].apply(lambda x: x['underlying'])
        enhanced_df['strike'] = enhanced_df['parsed_symbol'].apply(lambda x: x['strike'])
        enhanced_df['option_type'] = enhanced_df['parsed_symbol'].apply(lambda x: x['option_type'])
        enhanced_df['expiry_str'] = enhanced_df['parsed_symbol'].apply(lambda x: x.get('expiry_str', 'UNKNOWN'))
        
        # Calculate price change if previous close is available
        if 'PrvsClsgPric' in enhanced_df.columns and 'LastPric' in enhanced_df.columns:
            enhanced_df['price_change'] = enhanced_df['LastPric'] - enhanced_df['PrvsClsgPric']
            enhanced_df['price_change_pct'] = (enhanced_df['price_change'] / enhanced_df['PrvsClsgPric']) * 100
        
        # Calculate OI change percentage if available
        if 'ChngInOpnIntrst' in enhanced_df.columns and 'OpnIntrst' in enhanced_df.columns:
            enhanced_df['oi_change_pct'] = (enhanced_df['ChngInOpnIntrst'] / enhanced_df['OpnIntrst']) * 100
        
        print(f"✅ Enhanced {len(enhanced_df)} options contracts with calculated fields")
        print(f"📊 Unique underlyings: {enhanced_df['underlying'].nunique()}")
        print(f"📊 Option types: {enhanced_df['option_type'].value_counts().to_dict()}")
        
        return enhanced_df
