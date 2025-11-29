import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class CombinedAnalyzer:
    def __init__(self, historical_manager):
        self.historical_manager = historical_manager
    
    def analyze_combined(self, futures_df, options_df, prev_futures=None, prev_options=None):
        """Enhanced combined analysis with correct NSE column names"""
        
        if futures_df.empty or options_df.empty:
            print("❌ No data available for analysis")
            return pd.DataFrame()
        
        print("🔍 Using NSE BhavCopy column structure...")
        print(f"Futures columns: {futures_df.columns.tolist()}")
        print(f"Options columns: {options_df.columns.tolist()}")
        
        # Map column names to standard ones
        futures_df = self.standardize_column_names(futures_df, 'futures')
        options_df = self.standardize_column_names(options_df, 'options')
        
        print("✅ Standardized column names for analysis")
        
        # Create option chains
        print("🔗 Creating option chains...")
        option_chains = self.create_option_chain(options_df, futures_df)
        
        if option_chains is None:
            return pd.DataFrame()
        
        # Analyze opportunities
        opportunities = self.identify_opportunities(futures_df, option_chains, prev_futures)
        
        return opportunities
    
    def standardize_column_names(self, df, data_type):
        """Standardize NSE BhavCopy column names to our expected format"""
        
        # NSE BhavCopy column mappings based on your provided structure
        column_mappings = {
            'futures': {
                'symbol': ['TckrSymb'],
                'underlying': ['UndrlygPric'],  # This is the key fix - using correct column name
                'lastPrice': ['LastPric'],
                'openInterest': ['OpnIntrst'],
                'volume': ['TtlTradgVol'],
                'highPrice': ['HghPric'],
                'lowPrice': ['LwPric'],
                'openPrice': ['OpnPric'],
                'closePrice': ['ClsPric'],
                'settlementPrice': ['SttlmPric']
            },
            'options': {
                'symbol': ['TckrSymb'],
                'underlying': ['UndrlygPric'],  # This is the key fix - using correct column name
                'strikePrice': ['StrkPric'],
                'optionType': ['OptnTp'],
                'lastPrice': ['LastPric'],
                'openInterest': ['OpnIntrst'],
                'volume': ['TtlTradgVol'],
                'highPrice': ['HghPric'],
                'lowPrice': ['LwPric'],
                'openPrice': ['OpnPric'],
                'closePrice': ['ClsPric'],
                'settlementPrice': ['SttlmPric']
            }
        }
        
        mappings = column_mappings.get(data_type, {})
        standardized_df = df.copy()
        
        for standard_name, possible_names in mappings.items():
            for possible_name in possible_names:
                if possible_name in df.columns:
                    if standard_name != possible_name:
                        standardized_df[standard_name] = df[possible_name]
                        print(f"   🔄 Mapped '{possible_name}' -> '{standard_name}'")
                    break
        
        return standardized_df
    
    def create_option_chain(self, options_df, futures_df):
        """Create option chain with proper error handling"""
        
        try:
            # Check if required columns exist after standardization
            required_columns = ['underlying', 'strikePrice', 'optionType', 'lastPrice', 'openInterest']
            
            missing_columns = [col for col in required_columns if col not in options_df.columns]
            if missing_columns:
                print(f"❌ Missing required columns in options: {missing_columns}")
                print(f"✅ Available columns: {options_df.columns.tolist()}")
                return None
            
            if 'underlying' not in futures_df.columns or 'lastPrice' not in futures_df.columns:
                print(f"❌ Missing required columns in futures")
                print(f"✅ Available columns: {futures_df.columns.tolist()}")
                return None
            
            print("🔗 Grouping options by underlying symbol...")
            
            # Get unique underlying symbols from options
            unique_underlyings = options_df['underlying'].unique()
            print(f"📊 Found {len(unique_underlyings)} underlying symbols")
            
            option_chains = {}
            for underlying in unique_underlyings[:10]:  # Limit to first 10 for demo
                chain_options = options_df[options_df['underlying'] == underlying]
                analysis = self.analyze_single_chain(chain_options, futures_df, underlying)
                if analysis:
                    option_chains[underlying] = analysis
            
            return option_chains
            
        except Exception as e:
            print(f"❌ Error creating option chain: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def analyze_single_chain(self, chain_options, futures_df, underlying_symbol):
        """Analyze a single option chain"""
        try:
            # Find corresponding future price
            future_rows = futures_df[futures_df['symbol'] == underlying_symbol]
            if future_rows.empty:
                # Try to find by matching the first part of the symbol
                base_symbol = underlying_symbol.split('-')[0] if '-' in underlying_symbol else underlying_symbol
                future_rows = futures_df[futures_df['symbol'].str.startswith(base_symbol)]
            
            if future_rows.empty:
                print(f"⚠️ No future found for underlying: {underlying_symbol}")
                return None
            
            underlying_price = future_rows['lastPrice'].iloc[0]
            
            # Separate calls and puts
            calls = chain_options[chain_options['optionType'] == 'CE']
            puts = chain_options[chain_options['optionType'] == 'PE']
            
            analysis_result = {
                'underlying': underlying_symbol,
                'underlying_price': float(underlying_price),
                'total_oi': int(chain_options['openInterest'].sum()),
                'call_oi': int(calls['openInterest'].sum()),
                'put_oi': int(puts['openInterest'].sum()),
                'call_volume': int(calls['volume'].sum()) if 'volume' in chain_options.columns else 0,
                'put_volume': int(puts['volume'].sum()) if 'volume' in chain_options.columns else 0,
                'total_volume': int(chain_options['volume'].sum()) if 'volume' in chain_options.columns else 0
            }
            
            # Calculate OI ratios
            if analysis_result['put_oi'] > 0:
                analysis_result['oi_ratio'] = analysis_result['call_oi'] / analysis_result['put_oi']
            else:
                analysis_result['oi_ratio'] = float('inf')
            
            print(f"   📈 {underlying_symbol}: Price={underlying_price}, CallOI={analysis_result['call_oi']}, PutOI={analysis_result['put_oi']}")
            
            return analysis_result
            
        except Exception as e:
            print(f"❌ Error analyzing chain for {underlying_symbol}: {e}")
            return None
    
    def identify_opportunities(self, futures_df, option_chains, prev_futures):
        """Identify trading opportunities based on option chain analysis"""
        opportunities = []
        
        try:
            print("🎯 Identifying trading opportunities...")
            
            for symbol, chain_data in option_chains.items():
                if chain_data is None:
                    continue
                
                # Basic opportunity analysis based on OI ratios and price action
                opportunity = {
                    'symbol': symbol,
                    'current_price': chain_data['underlying_price'],
                    'call_oi': chain_data['call_oi'],
                    'put_oi': chain_data['put_oi'],
                    'total_oi': chain_data['total_oi'],
                    'oi_ratio': chain_data['oi_ratio'],
                    'call_volume': chain_data['call_volume'],
                    'put_volume': chain_data['put_volume'],
                    'total_volume': chain_data['total_volume'],
                    'setup': self.identify_setup(chain_data),
                    'recommendation': self.generate_recommendation(chain_data),
                    'confidence': self.calculate_confidence(chain_data),
                    'strike_guidance': self.suggest_strikes(chain_data),
                    'reason': self.generate_reason(chain_data),
                    'data_quality': 'HIGH' if prev_futures is not None else 'LOW',
                    'has_historical_data': prev_futures is not None,
                    'price_change_pct': 0.0,  # Would need previous data for this
                    'oi_change_pct': 0.0,     # Would need previous data for this
                    'volume_change_pct': 0.0, # Would need previous data for this
                    'max_pain': self.calculate_max_pain(chain_data)
                }
                
                opportunities.append(opportunity)
            
            print(f"✅ Identified {len(opportunities)} potential opportunities")
            return pd.DataFrame(opportunities)
            
        except Exception as e:
            print(f"❌ Error identifying opportunities: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def identify_setup(self, chain_data):
        """Identify the trading setup based on OI analysis"""
        oi_ratio = chain_data['oi_ratio']
        
        if oi_ratio > 2.0:
            return "Put Writing Dominance"
        elif oi_ratio > 1.5:
            return "Call Writing Pressure"
        elif oi_ratio < 0.5:
            return "Call Writing Dominance"
        elif oi_ratio < 0.7:
            return "Put Writing Pressure"
        else:
            return "Neutral OI Balance"
    
    def generate_recommendation(self, chain_data):
        """Generate trading recommendation"""
        setup = self.identify_setup(chain_data)
        
        if "Put Writing" in setup and "Dominance" in setup:
            return "STRONG CALL"
        elif "Call Writing" in setup and "Dominance" in setup:
            return "STRONG PUT"
        elif "Put Writing" in setup:
            return "CALL"
        elif "Call Writing" in setup:
            return "PUT"
        else:
            return "NEUTRAL"
    
    def calculate_confidence(self, chain_data):
        """Calculate confidence level"""
        oi_ratio = chain_data['oi_ratio']
        total_oi = chain_data['total_oi']
        
        if (oi_ratio > 2.0 or oi_ratio < 0.5) and total_oi > 100000:
            return "High"
        elif (oi_ratio > 1.5 or oi_ratio < 0.7) and total_oi > 50000:
            return "Medium"
        else:
            return "Low"
    
    def suggest_strikes(self, chain_data):
        """Suggest strike prices for trading"""
        price = chain_data['underlying_price']
        
        # Round to nearest strike interval (usually 50 or 100 for NSE)
        if price > 1000:
            interval = 100
        elif price > 500:
            interval = 50
        else:
            interval = 20
        
        atm_strike = round(price / interval) * interval
        return f"ATM: {atm_strike}, OTM: {atm_strike + interval}"
    
    def generate_reason(self, chain_data):
        """Generate reason for recommendation"""
        setup = self.identify_setup(chain_data)
        oi_ratio = chain_data['oi_ratio']
        
        if oi_ratio > 1:
            return f"Call OI {oi_ratio:.1f}x higher than Put OI - {setup}"
        else:
            return f"Put OI {1/oi_ratio:.1f}x higher than Call OI - {setup}"
    
    def calculate_max_pain(self, chain_data):
        """Calculate max pain (simplified)"""
        # This is a simplified version - real max pain requires full chain analysis
        price = chain_data['underlying_price']
        return f"~{round(price * 0.98)}-{round(price * 1.02)}"
