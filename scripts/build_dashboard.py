import json
import pandas as pd

def build_dashboard():
    with open("data/signals/latest_signals.json") as f:
        data = json.load(f)

    html = "<html><body><h1>NSE Option Signals</h1><table border='1'>"
    html += "<tr><th>Symbol</th><th>Type</th><th>Reason</th><th>Price%</th><th>OI%</th></tr>"

    for r in data:
        html += f"<tr><td>{r['symbol']}</td><td>{r['type']}</td><td>{r['reason']}</td><td>{r['price_change']}</td><td>{r['oi_change']}</td></tr>"

    html += "</table></body></html>"

    with open("dashboards/index.html", "w") as f:
        f.write(html)

    print("[INFO] Dashboard updated")
  
