from dataclasses import dataclass

@dataclass
class AppConfig:
    symbols: tuple[str, ...] = ("NIFTY", "BANKNIFTY")
    request_timeout: int = 10
    max_retries: int = 3
    retry_backoff_seconds: int = 3
    throttle_seconds: float = 1.5
    cache_ttl_seconds: int = 120
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
  
