# tests/test_portfolio.py
import json
import numpy as np
import pandas as pd
from portfolio import aggregate_portfolio_sequential, aggregate_portfolio_parallel

def test_portfolio_seq_equals_parallel(df_pandas: pd.DataFrame, portfolio_dict: dict):
    seq = aggregate_portfolio_sequential(portfolio_dict, df_pandas, vol_window=20)
    par = aggregate_portfolio_parallel(portfolio_dict, df_pandas, vol_window=20, max_workers=None)

    # totals must match
    assert abs(seq["total_value"] - par["total_value"]) < 1e-6
    assert np.isfinite(seq["aggregate_volatility"])
    assert np.isfinite(par["aggregate_volatility"])

def test_portfolio_position_values_match_latest_price(df_pandas: pd.DataFrame, portfolio_dict: dict):
    # Spot-check: value = qty * latest price in df for first position
    if not portfolio_dict.get("positions"):
        return
    pos0 = portfolio_dict["positions"][0]
    sym, qty = pos0["symbol"], float(pos0["quantity"])

    # latest price from df
    s = df_pandas[df_pandas["symbol"] == sym].sort_index()
    assert len(s) > 0, f"No data for symbol {sym}"
    last_px = float(s["price"].iloc[-1])

    seq = aggregate_portfolio_sequential(portfolio_dict, df_pandas, vol_window=20)
    # find matching position result
    match = [p for p in seq["positions"] if p["symbol"] == sym]
    assert match, f"No aggregated position for {sym}"
    computed_val = match[0]["value"]
    assert abs(computed_val - qty * last_px) < 1e-6
