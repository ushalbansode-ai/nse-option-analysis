import requests, time
from typing import Dict
from src.config import AppConfig
from src.utils.logger import get_logger
from src.data.cache_manager import CacheManager
from src.utils.helpers import throttle

class NSEDataFetcher:
    INDEX_URL = "https://www.nseindia.com/api/option-chain-indices"

    def __init__(self, config: AppConfig = AppConfig()):
        self.config = config
        self.logger = get_logger(__name__)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.config.user_agent,
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/"
        })
        self.cache = CacheManager(ttl_seconds=config.cache_ttl_seconds)

    def fetch_option_chain(self, symbol: str) -> Dict:
        cache_key = f"option_chain_{symbol}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        url = f"{self.INDEX_URL}?symbol={symbol}"
        throttle(self.config.throttle_seconds)
        for attempt in range(1, self.config.max_retries + 1):
            try:
                resp = self.session.get(url, timeout=self.config.request_timeout)
                resp.raise_for_status()
                data = resp.json()
                self.cache.set(cache_key, data)
                return data
            except Exception as e:
                self.logger.warning(f"Attempt {attempt} failed: {e}")
                time.sleep(self.config.retry_backoff_seconds)
        return {}
        
