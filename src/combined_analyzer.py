"""
Enhanced Combined Futures + Options Analyzer with Previous Day Data
"""

import pandas as pd
import datetime
from collections import defaultdict

class CombinedAnalyzer:
    def __init__(self, historical_manager):
        self.historical_manager = historical_manager
    
    def analyze_combined(self, futures_df, options_df, prev_futures=None, prev_options=None):
        """
        Enhanced combined analysis using BOTH current and previous day data
        """
        opportunities = []
        
        print("🔍 Running ENHANCED COMBINED ANALYSIS...")
        print(f"   📅 Current data: {len(futures_df)} futures, {len(options_df)} options")
        print(f"   📅 Previous data: {len(prev_futures) if prev_futures is not None else 0} futures")
        
        # Create option chain structure
        option_chain = self._create_option_chain(options_df, futures_df)
        
        # Calculate ACTUAL changes from previous day data
        if prev_futures is not None:
            print("📈 Using ACTUAL historical data for comparisons...")
            oi_changes = self.historical_manager.calculate_oi_change(futures_df, prev_futures)
            volume_changes = self.historical_manager.calculate_volume_change(futures_df, prev_futures)
            price_changes = self.historical_manager.calculate_price_change(futures_df, prev_futures)
            data_source = "HISTORICAL"
        else:
            print("⚠️ Using CURRENT DAY data only (no historical data available)")
            oi_changes = self._calculate_current_day_changes(futures_df)
            volume_changes = self._calculate_current_day_volume(futures_df)
            price_changes = self._calculate_current_day_price(futures_df)
            data_source = "CURRENT_ONLY"
        
        # Analyze each underlying with BOTH current and previous data
        analyzed_count = 0
        for _, future in futures_df.iterrows():
            symbol = future['underlying']
            
            if symbol not in option_chain:
                continue  # Skip if no options data for this symbol
            
            analyzed_count += 1
            
            # Get data changes
            price_data = price_changes.get(symbol, {})
            oi_data = oi_changes.get(symbol, {})
            volume_data = volume_changes.get(symbol, {})
            
            current_price = price_data.get('current_price', future['LastPric'])
            price_change_pct = price_data.get('change_percentage', 0)
            oi_change_pct = oi_data.get('percentage_change', 0)
            volume_change_pct = volume_data.get('change_percentage', 0)
            has_historical = oi_data.get('has_historical_data', False)
            
            # Analyze options data for this symbol
            options_analysis = self._analyze_options_for_symbol(symbol, option_chain, current_price, prev_options)
            
            # Generate COMBINED verdict with historical context
            combined_opportunity = self._generate_combined_verdict(
                symbol, future, options_analysis, 
                price_change_pct, oi_change_pct, volume_change_pct,
                current_price, has_historical, data_source
            )
            
            if combined_opportunity:
                opportunities.append(combined_opportunity)
        
        print(f"✅ Analyzed {analyzed_count} symbols with options data")
        return pd.DataFrame(opportunities)
    
    def _calculate_current_day_changes(self, futures_df):
        """Calculate changes using only current day data"""
        oi_changes = {}
        for _, future in futures_df.iterrows():
            symbol = future['underlying']
            current_oi = future['OpnIntrst']
            oi_change = future['ChngInOpnIntrst']
            oi_change_pct = (oi_change / current_oi) * 100 if current_oi > 0 else 0
            
            oi_changes[symbol] = {
                'absolute_change': oi_change,
                'percentage_change': oi_change_pct,
                'current_oi': current_oi,
                'previous_oi': current_oi - oi_change,
                'has_historical_data': False
            }
        return oi_changes
    
    def _calculate_current_day_volume(self, futures_df):
        """Calculate volume using only current day data"""
        volume_changes = {}
        for _, future in futures_df.iterrows():
            symbol = future['underlying']
            current_volume = future['TtlTradgVol']
            
            volume_changes[symbol] = {
                'current_volume': current_volume,
                'previous_volume': current_volume,  # Assume same without historical
                'absolute_change': 0,
                'change_percentage': 0,
                'has_historical_data': False
            }
        return volume_changes
    
    def _calculate_current_day_price(self, futures_df):
        """Calculate price changes using only current day data"""
        price_changes = {}
        for _, future in futures_df.iterrows():
            symbol = future['underlying']
            current_price = future['LastPric']
            prev_close = future['PrvsClsgPric']
            price_change = current_price - prev_close
            price_change_pct = (price_change / prev_close) * 100 if prev_close > 0 else 0
            
            price_changes[symbol] = {
                'current_price': current_price,
                'previous_close': prev_close,
                'absolute_change': price_change,
                'change_percentage': price_change_pct,
                'has_historical_data': False
            }
        return price_changes
    
    def _create_option_chain(self, options_df, futures_df):
        """Create option chain structure"""
        option_chain = defaultdict(lambda: defaultdict(dict))
        underlying_prices = {}
        
        # Get current prices from futures
        for _, future in futures_df.iterrows():
            underlying_prices[future['underlying']] = future['LastPric']
        
        # Organize options
        for _, option in options_df.iterrows():
            underlying = option['underlying']
            strike = option['strike']
            option_type = option['option_type']
            expiry = option.get('expiry_str', 'UNKNOWN')
            
            if underlying not in underlying_prices:
                continue
                
            current_price = underlying_prices[underlying]
            
            option_chain[underlying][expiry][(strike, option_type)] = {
                'last_price': option['LastPric'],
                'oi': option['OpnIntrst'],
                'oi_change': option['ChngInOpnIntrst'],
                'volume': option['TtlTradgVol'],
                'prev_close': option['PrvsClsgPric'],
                'distance_from_spot': abs(current_price - strike),
                'is_ATM': abs(current_price - strike) / current_price < 0.02,
                'is_ITM': (option_type == 'CE' and strike < current_price) or 
                         (option_type == 'PE' and strike > current_price)
            }
        
        return option_chain
    
    def _analyze_options_for_symbol(self, symbol, option_chain, current_price, prev_options=None):
        """Analyze options data for a specific symbol with historical context"""
        analysis = {
            'call_oi_trend': 0,
            'put_oi_trend': 0,
            'call_volume': 0,
            'put_volume': 0,
            'atm_call_oi_change': 0,
            'atm_put_oi_change': 0,
            'max_pain': None,
            'oi_ratio': 0,
            'pcr_oi': 0,
            'has_historical_options': prev_options is not None
        }
        
        if symbol not in option_chain:
            return analysis
        
        symbol_chain = option_chain[symbol]
        
        # Analyze all expiries
        for expiry, strikes in symbol_chain.items():
            total_call_oi = 0
            total_put_oi = 0
            total_call_oi_change = 0
            total_put_oi_change = 0
            total_call_volume = 0
            total_put_volume = 0
            
            strike_oi = {}
            
            for (strike, opt_type), data in strikes.items():
                if opt_type == 'CE':
                    total_call_oi += data['oi']
                    total_call_oi_change += data['oi_change']
                    total_call_volume += data['volume']
                else:  # PE
                    total_put_oi += data['oi']
                    total_put_oi_change += data['oi_change']
                    total_put_volume += data['volume']
                
                # Track for max pain
                if strike not in strike_oi:
                    strike_oi[strike] = {'call_oi': 0, 'put_oi': 0}
                strike_oi[strike][f'{opt_type.lower()}_oi'] += data['oi']
                
                # Track ATM changes
                if data.get('is_ATM', False):
                    if opt_type == 'CE':
                        analysis['atm_call_oi_change'] += data['oi_change']
                    else:
                        analysis['atm_put_oi_change'] += data['oi_change']
            
            # Calculate OI ratios
            if total_put_oi > 0:
                analysis['oi_ratio'] = total_call_oi / total_put_oi
            if total_call_oi > 0:
                analysis['pcr_oi'] = total_put_oi / total_call_oi
            
            analysis['call_oi_trend'] = total_call_oi_change
            analysis['put_oi_trend'] = total_put_oi_change
            analysis['call_volume'] = total_call_volume
            analysis['put_volume'] = total_put_volume
            
            # Calculate max pain
            if strike_oi:
                max_pain_strike = None
                min_net_oi = float('inf')
                
                for strike, ois in strike_oi.items():
                    net_oi = abs(ois['call_oi'] - ois['put_oi'])
                    if net_oi < min_net_oi:
                        min_net_oi = net_oi
                        max_pain_strike = strike
                
                analysis['max_pain'] = max_pain_strike
        
        return analysis
    
    def _generate_combined_verdict(self, symbol, future, options_analysis, 
                                 price_change_pct, oi_change_pct, volume_change_pct,
                                 current_price, has_historical, data_source):
        """
        Generate FINAL VERDICT by combining current and previous day data
        """
        
        # Extract options data
        call_oi_trend = options_analysis['call_oi_trend']
        put_oi_trend = options_analysis['put_oi_trend']
        oi_ratio = options_analysis['oi_ratio']
        max_pain = options_analysis['max_pain']
        atm_call_oi_change = options_analysis['atm_call_oi_change']
        atm_put_oi_change = options_analysis['atm_put_oi_change']
        has_historical_options = options_analysis['has_historical_options']
        
        # Data quality indicator
        data_quality = "HIGH" if has_historical and has_historical_options else "MEDIUM" if has_historical else "LOW"
        
        # STRONG BULLISH SIGNAL: Previous vs Current Day Analysis
        strong_bullish = (
            price_change_pct > 1.0 and           # Price up significantly
            oi_change_pct > 5.0 and              # OI increase (long buildup)
            volume_change_pct > 25.0 and         # Volume spike
            call_oi_trend > 0 and                # Call OI increasing
            put_oi_trend < 0 and                 # Put OI decreasing
            oi_ratio > 1.2 and                   # More calls than puts
            has_historical                       # Has proper historical data
        )
        
        # STRONG BEARISH SIGNAL: Previous vs Current Day Analysis
        strong_bearish = (
            price_change_pct < -1.0 and          # Price down significantly
            oi_change_pct > 5.0 and              # OI increase (short buildup)
            volume_change_pct > 25.0 and         # Volume spike
            put_oi_trend > 0 and                 # Put OI increasing
            call_oi_trend < 0 and                # Call OI decreasing
            oi_ratio < 0.8 and                   # More puts than calls
            has_historical                       # Has proper historical data
        )
        
        # MODERATE BULLISH: Weaker signals or missing historical data
        moderate_bullish = (
            price_change_pct > 0.5 and
            (oi_change_pct > 2.0 or not has_historical) and
            call_oi_trend > 0 and
            (max_pain is None or current_price > max_pain)
        )
        
        # MODERATE BEARISH: Weaker signals or missing historical data
        moderate_bearish = (
            price_change_pct < -0.5 and
            (oi_change_pct > 2.0 or not has_historical) and
            put_oi_trend > 0 and
            (max_pain is None or current_price < max_pain)
        )
        
        # Generate verdict with data source info
        opportunity = None
        
        if strong_bullish:
            opportunity = self._create_opportunity(
                symbol, "STRONG BULLISH", "BUY CALL", 
                price_change_pct, oi_change_pct, volume_change_pct,
                call_oi_trend, put_oi_trend, current_price, max_pain,
                f"Futures: Long Build-up + Options: Call OI↑ Put OI↓ + Volume Spike | Data: {data_source}",
                "High", data_quality, has_historical
            )
        
        elif strong_bearish:
            opportunity = self._create_opportunity(
                symbol, "STRONG BEARISH", "BUY PUT", 
                price_change_pct, oi_change_pct, volume_change_pct,
                call_oi_trend, put_oi_trend, current_price, max_pain,
                f"Futures: Short Build-up + Options: Put OI↑ Call OI↓ + Volume Spike | Data: {data_source}",
                "High", data_quality, has_historical
            )
        
        elif moderate_bullish:
            opportunity = self._create_opportunity(
                symbol, "MODERATE BULLISH", "BUY CALL", 
                price_change_pct, oi_change_pct, volume_change_pct,
                call_oi_trend, put_oi_trend, current_price, max_pain,
                f"Futures: Price↑ OI↑ + Options: Call OI↑ + Above Max Pain | Data: {data_source}",
                "Medium", data_quality, has_historical
            )
        
        elif moderate_bearish:
            opportunity = self._create_opportunity(
                symbol, "MODERATE BEARISH", "BUY PUT", 
                price_change_pct, oi_change_pct, volume_change_pct,
                call_oi_trend, put_oi_trend, current_price, max_pain,
                f"Futures: Price↓ OI↑ + Options: Put OI↑ + Below Max Pain | Data: {data_source}",
                "Medium", data_quality, has_historical
            )
        
        return opportunity
    
    def _create_opportunity(self, symbol, setup, recommendation, 
                          price_change_pct, oi_change_pct, volume_change_pct,
                          call_oi_trend, put_oi_trend, current_price, max_pain,
                          reason, confidence, data_quality, has_historical):
        """Create enhanced opportunity dictionary with historical context"""
        
        opportunity = {
            'symbol': symbol,
            'setup': setup,
            'recommendation': recommendation,
            'current_price': current_price,
            'price_change_pct': price_change_pct,
            'oi_change_pct': oi_change_pct,
            'volume_change_pct': volume_change_pct,
            'call_oi_change': call_oi_trend,
            'put_oi_change': put_oi_trend,
            'max_pain': max_pain,
            'reason': reason,
            'confidence': confidence,
            'data_quality': data_quality,
            'has_historical_data': has_historical,
            'analysis_type': 'ENHANCED_COMBINED',
            'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Add strike guidance based on recommendation and confidence
        if 'CALL' in recommendation:
            if confidence == 'High':
                opportunity['strike_guidance'] = f"ATM to slightly OTM ({current_price:.2f} to {current_price*1.05:.2f})"
            else:
                opportunity['strike_guidance'] = f"ATM around {current_price:.2f}"
        else:  # PUT
            if confidence == 'High':
                opportunity['strike_guidance'] = f"ATM to slightly OTM ({current_price:.2f} to {current_price*0.95:.2f})"
            else:
                opportunity['strike_guidance'] = f"ATM around {current_price:.2f}"
        
        return opportunity
