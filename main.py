import json
import pandas as pd
from pathlib import Path

# ----------------------------
# Import core modules
# ----------------------------
from src.fetch_bhavcopy import fetch_bhavcopy
from src.compute_features import compute_features
from src.generate_signals import generate_signals


def main():
    print("Fetching NSE Futures DAT bhavcopy...\n")

    # ---- Step 1: Download & parse DAT file ----
    raw_df = fetch_bhavcopy()          # <-- Updated name
    print(f"Fetched rows: {len(raw_df)}")

    # ---- Step 2: Compute features ----
    print("\nComputing features...")
    feat_df = compute_features(raw_df)
    print(f"Feature rows: {len(feat_df)}")

    # ---- Step 3: Generate signals ----
    print("\nGenerating signals...")
    sig_df = generate_signals(feat_df)
    print(f"Signals generated: {len(sig_df)}")

    # ---- Step 4: Save output ----
    output_path = Path("data/signals/latest_signals.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output_path.write_text(
        json.dumps(sig_df.to_dict(orient="records"), indent=2)
    )

    print("\nSaved signals → data/signals/latest_signals.json")
    print("Done.")


if __name__ == "__main__":
    main()
    
