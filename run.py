#!/usr/bin/env python3
"""
Enhanced NSE Combined Futures & Options Analysis
Main execution script
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from data_fetcher import DataFetcher
from data_processor import DataProcessor
from combined_analyzer import CombinedAnalyzer
from report_generator import ReportGenerator
from config.settings import STRATEGY_CONFIG, ANALYSIS_CONFIG

def ensure_directories():
    """Create all necessary directories"""
    directories = [
        'data/raw',
        'data/processed',
        'data/comparison',
        'outputs/signals',
        'outputs/reports',
        'logs'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def main():
    print("Run Combined Futures & Options Analysis")
    print("=" * 50)
    
    try:
        # Step 0: Ensure directories exist
        print("\nStep 0: Ensuring directories...")
        ensure_directories()
        
        # Step 1: Fetch data
        print("\nStep 1: Fetching bhavcopy...")
        fetcher = DataFetcher()
        csv_file = fetcher.fetch_latest_bhavcopy()
        
        if not csv_file:
            print("❌ Failed to fetch bhavcopy data")
            return
        
        # Step 2: Process data
        print("\nStep 2: Processing data...")
        processor = DataProcessor()
        futures_data, options_data = processor.process_bhavcopy_data(csv_file)
        
        # Get current date
        current_date = fetcher.current_date.strftime('%Y-%m-%d')
        
        # Step 3: Save current data with date in filename
        print(f"\nStep 3: Saving current data for {current_date}...")
        
        # Save futures data
        if futures_data is not None and not futures_data.empty:
            futures_file = f"data/processed/futures_{current_date}.csv"
            futures_data.to_csv(futures_file, index=False)
            print(f"✅ Saved futures data: {futures_file}")
        
        # Save options data
        if options_data is not None and not options_data.empty:
            options_file = f"data/processed/options_{current_date}.csv"
            options_data.to_csv(options_file, index=False)
            print(f"✅ Saved options data: {options_file}")
        
        # Step 4: Load previous data
        print("\nStep 4: Loading PREVIOUS day data for comparison...")
        previous_futures, previous_options, prev_date = processor.load_previous_data()
        
        # Step 5: Run combined analysis
        print("\nStep 5: Running ENHANCED COMBINED ANALYSIS...")
        analyzer = CombinedAnalyzer()
        opportunities = analyzer.identify_combined_opportunities(
            futures_data, options_data, previous_futures, previous_options
        )
        
        # Step 6: Generate enhanced reports
        print("\nStep 6: Generating enhanced reports...")
        reporter = ReportGenerator()
        
        # Determine historical status
        historical_status = "AVAILABLE" if previous_futures is not None else "UNAVAILABLE"
        
        reporter.generate_enhanced_reports(
            opportunities, futures_data, options_data, 
            current_date, prev_date, historical_status
        )
        
        print("\n" + "=" * 50)
        print("ENHANCED ANALYSIS COMPLETED SUCCESSFULLY!")
        print(f"Historical Data: {historical_status}")
        
        if len(opportunities) > 0:
            print(f"FINAL VERDICT: {len(opportunities)} opportunities identified")
        else:
            print("FINAL VERDICT: No strong opportunities identified today")
            if historical_status == "UNAVAILABLE":
                print("Tip: Run again tomorrow for historical data comparison")
                
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
