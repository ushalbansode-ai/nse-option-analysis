import pandas as pd




def pct_change(a, b):
try:
if pd.isna(a) or pd.isna(b):
return None
return (a - b) / b * 100.0
except Exception:
return None
