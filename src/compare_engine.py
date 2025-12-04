import pandas as pd


def detect_signals(prev_df, today_df):
    signals = []

    for symbol in today_df["H"].unique():
        t = today_df[today_df["H"] == symbol]
        p = prev_df[prev_df["H"] == symbol]

        if t.empty or p.empty:
            continue

        # FUTURES ONLY (Instrument Type = FUTIDX / FUTSTK)
        t_fut = t[t["FinInstrmTp"] == "FUTIDX"]
        p_fut = p[p["FinInstrmTp"] == "FUTIDX"]

        if t_fut.empty or p_fut.empty:
            continue

        price_change = (t_fut["LastPric"].iloc[0] - p_fut["LastPric"].iloc[0]) / p_fut["LastPric"].iloc[0] * 100
        oi_change = (t_fut["OpnIntrst"].iloc[0] - p_fut["OpnIntrst"].iloc[0]) / max(p_fut["OpnIntrst"].iloc[0], 1) * 100

        # CALL and PUT tables
        t_calls = t[t["OptnTp"] == "CE"]
        t_puts = t[t["OptnTp"] == "PE"]
        p_calls = p[p["OptnTp"] == "CE"]
        p_puts = p[p["OptnTp"] == "PE"]

        # Compute OI changes ATM
        def get_oi_change(df_t, df_p):
            if df_t.empty or df_p.empty:
                return 0
            return (df_t["OpnIntrst"].sum() - df_p["OpnIntrst"].sum()) / max(df_p["OpnIntrst"].sum(), 1) * 100

        call_oi_change = get_oi_change(t_calls, p_calls)
        put_oi_change  = get_oi_change(t_puts, p_puts)

        # ---- Your Rules ----

        # 1. Long CALL setup
        if price_change > 1.5 and oi_change > 5 and call_oi_change < -5 and put_oi_change > 5:
            signals.append({
                "symbol": symbol,
                "type": "BUY CALL",
                "reason": "Long buildup + Call unwinding + Put writing",
                "price_change": round(price_change, 2),
                "oi_change": round(oi_change, 2),
                "call_oi_change": round(call_oi_change, 2),
                "put_oi_change": round(put_oi_change, 2),
            })

        # 2. Long PUT setup
        if price_change < -1.5 and oi_change > 5 and put_oi_change < -5 and call_oi_change > 5:
            signals.append({
                "symbol": symbol,
                "type": "BUY PUT",
                "reason": "Short buildup + Put unwinding + Call writing",
                "price_change": round(price_change, 2),
                "oi_change": round(oi_change, 2),
                "call_oi_change": round(call_oi_change, 2),
                "put_oi_change": round(put_oi_change, 2),
            })

    return pd.DataFrame(signals)
  
