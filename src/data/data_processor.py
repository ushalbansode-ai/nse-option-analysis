import pandas as pd
from typing import Dict
from src.utils.logger import get_logger

class DataProcessor:
    def __init__(self):
        self.logger = get_logger(__name__)

    def process_option_chain(self, raw_data: Dict) -> Dict:
        if not raw_data or "filtered" not in raw_data:
            self.logger.error("Invalid raw_data: missing 'filtered'")
            return {}

        filtered = raw_data["filtered"]
        records = filtered.get("data", [])
        if not isinstance(records, list):
            self.logger.error("Invalid 'data' format in raw_data")
            return {}

        call_rows, put_rows = [], []
        for rec in records:
            expiry = rec.get("expiryDate")
            if "CE" in rec:
                ce = dict(rec["CE"])
                ce["optionType"] = "CE"
                ce["expiryDate"] = expiry
                call_rows.append(ce)
            if "PE" in rec:
                pe = dict(rec["PE"])
                pe["optionType"] = "PE"
                pe["expiryDate"] = expiry
                put_rows.append(pe)

        calls = pd.DataFrame(call_rows)
        puts = pd.DataFrame(put_rows)

        return {
            "calls": calls,
            "puts": puts,
            "timestamp": raw_data.get("records", {}).get("timestamp") or raw_data.get("timestamp"),
            "underlying_value": raw_data.get("records", {}).get("underlyingValue") or raw_data.get("underlyingValue", 0.0),
        }
        
