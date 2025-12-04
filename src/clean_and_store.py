# src/clean_and_store.py
import pandas as pd
from pathlib import Path
from datetime import datetime


HISTORY_CLEAN = Path('data/history/cleaned')
HISTORY_CLEAN.mkdir(parents=True, exist_ok=True)
MASTER = Path('data/history/merged/all_days.parquet')


COLUMN_MAP = {
'TckrSymb': 'symbol',
'SctySrs': 'series',
'XpryDt': 'expiry',
'StrkPric': 'strike',
'OptnTp': 'option_type',
'LastPric': 'ltp',
'OpnIntrst': 'oi',
'ChngInOpnIntrst': 'oi_change',
'TtlTradgVol': 'volume',
'UndrlygPric': 'underlying',
'FinInstrmTp': 'instrument_type',
'TradDt': 'trade_date'
}
def clean_csv(path: Path):
df = pd.read_csv(path, low_memory=False)
df = df.rename(columns=lambda c: c.strip())
present_map = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
df = df.rename(columns=present_map)


for c in ['strike','ltp','oi','oi_change','volume','underlying']:
if c in df.columns:
df[c] = pd.to_numeric(df[c], errors='coerce')


if 'instrument_type' in df.columns:
df['instrument_type'] = df['instrument_type'].str.strip().str.upper()


date_str = None
if 'trade_date' in df.columns:
try:
date_str = pd.to_datetime(df['trade_date'].iloc[0]).strftime('%Y-%m-%d')
except Exception:
date_str = datetime.utcnow().strftime('%Y-%m-%d')
else:
date_str = datetime.utcnow().strftime('%Y-%m-%d')


out_path = HISTORY_CLEAN / f'{date_str}.csv'
df.to_csv(out_path, index=False)
if MASTER.exists():
master = pd.read_parquet(MASTER)
merged = pd.concat([master, df], ignore_index=True)
merged = merged.drop_duplicates(subset=['symbol','expiry','strike','option_type','trade_date','instrument_type'], keep='last')
merged.to_parquet(MASTER, index=False)
else:
df.to_parquet(MASTER, index=False)


return out_path




if __name__ == '__main__':
import sys
p = Path(sys.argv[1]) if len(sys.argv)>1 else None
if p is None:
print('Usage: clean_and_store.py path/to/extracted.csv')
else:
print('Cleaning', p)
out = clean_csv(p)
print('Saved cleaned', out)
