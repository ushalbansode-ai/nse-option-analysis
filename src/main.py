import os, json
from datetime import datetime
from src.config import AppConfig
from src.utils.logger import get_logger
from src.data.nse_data_fetcher import NSEDataFetcher
from src.data.data_processor import DataProcessor
from src.analysis.underrated_factors import UnderratedFactors
from src.analysis.scoring_engine import ScoringEngine
from src.strategies.option_buying_strategies import OptionBuyingStrategies

def run_for_symbol(symbol: str, config: AppConfig):
    logger = get_logger(__name__)
    fetcher = NSEDataFetcher(config)
    processor = DataProcessor()
    factors = UnderratedFactors()
    scorer = ScoringEngine()
    strategies = OptionBuyingStrategies()

    raw = fetcher.fetch_option_chain(symbol)
    processed = processor.process_option_chain(raw)
    if not processed:
        return {}

    calls, puts = processed["calls"], processed["puts"]
    under_price = processed["underlying_value"]

    for df in (calls, puts):
        df = factors.analyze_oi_volume_ratio(df)
        df = factors.analyze_bid_ask_spread(df)
        df = factors.analyze_oi_concentration(df)
        df = scorer.compute_scores(df)

    opp_calls = strategies.find_breakout_opportunities(calls, under_price)
    opp_puts = strategies.find_breakout_opportunities(puts, under_price)
    opportunities = sorted(opp_calls + opp_puts, key=lambda x: x["score"], reverse=True)

    return {
        "symbol": symbol,
        "timestamp": processed["timestamp"],
        "underlying_price": under_price,
        "opportunities": opportunities[:20],
    }

if __name__ == "__main__":
    os.makedirs("results", exist_ok=True)
    config = AppConfig()
    results = [run_for_symbol(sym, config) for sym in config.symbols]
    filename = f"results/analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Results saved to {filename}")
    
