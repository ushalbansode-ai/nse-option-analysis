import json
import os
import time
from typing import Any

class CacheManager:
    def __init__(self, cache_dir: str = ".cache", ttl_seconds: int = 120):
        self.cache_dir = cache_dir
        self.ttl_seconds = ttl_seconds
        os.makedirs(self.cache_dir, exist_ok=True)

    def _path(self, key: str) -> str:
        return os.path.join(self.cache_dir, f"{key}.json")

    def get(self, key: str) -> Any | None:
        path = self._path(key)
        if not os.path.exists(path):
            return None
        if time.time() - os.path.getmtime(path) > self.ttl_seconds:
            return None
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return None

    def set(self, key: str, data: Any) -> None:
        path = self._path(key)
        try:
            with open(path, "w") as f:
                json.dump(data, f)
        except Exception:
            pass
          
