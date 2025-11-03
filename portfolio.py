from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
import numpy as np
import pandas as pd

# ---------- core helpers (self-contained; no changes to metrics.py needed) ----------
def _pct_returns(price: pd.Series) -> pd.Series:
    return price.pct_change().fillna(0.0)

def _rolling_vol_pct(price: pd.Series, window: int = 20) -> float:
    """Rolling std of returns over window (percent volatility)."""
    r = _pct_returns(price)
    v = r.rolling(window).std(ddof=0)
    return float(v.iloc[-1]) if v.notna().any() else float("nan")

def _max_drawdown_pct(price: pd.Series) -> float:
    
    if price.empty:
        return float("nan")
    peak = price.cummax()
    dd = price / peak - 1.0
    return float(dd.min())

def _series_for_symbol(panel: pd.DataFrame, symbol: str) -> pd.Series:
    sub = panel[panel["symbol"] == symbol]


    sub = sub.sort_index()
    return sub["price"].astype(float)


@dataclass
class PositionOut:
    symbol: str
    value: float
    volatility: float
    drawdown: float
    


# Compute metrics for a single position
def _compute_position(symbol: str, quantity: float, price_series: pd.Series, vol_window: int = 20) -> Dict[str, Any]:
    
    value_now = float(price_series.iloc[-1]) * float(quantity) if not price_series.empty else np.nan
    vol_pct   = _rolling_vol_pct(price_series, window=vol_window)     
    dd_pct    = _max_drawdown_pct(price_series)                       
    return {
        "symbol": symbol,
        "value": round(value_now, 2),
        "volatility": float(vol_pct) if np.isfinite(vol_pct) else np.nan,
        "drawdown": float(dd_pct) if np.isfinite(dd_pct) else np.nan,
    }

def _aggregate_children(children: List[Dict[str, Any]]) -> Tuple[float, float, float]:
    """
    Returns: (total_value, aggregate_volatility, max_drawdown)
      - total_value: sum of child 'value' or 'total_value'
      - aggregate_volatility: value-weighted average of volatilities
      - max_drawdown: worst (most negative) drawdown among children
    """
    total = float(sum(c.get("total_value", c.get("value", 0.0)) for c in children))

    vol_num = 0.0
    for c in children:
        v = float(c.get("total_value", c.get("value", 0.0)) or 0.0)
        vol = float(c.get("aggregate_volatility", c.get("volatility", 0.0)) or 0.0)
        vol_num += v * vol
    agg_vol = (vol_num / total) if total > 0 else 0.0

    worst_dd = 0.0
    for c in children:
        d = c.get("max_drawdown", c.get("drawdown", 0.0))
        worst_dd = min(worst_dd, float(d or 0.0))
    return total, agg_vol, worst_dd


# Main function: aggregate portfolio metrics sequentially for json
def aggregate_portfolio_sequential(portfolio: Dict[str, Any],
                                   price_panel: pd.DataFrame,
                                   vol_window: int = 20) -> Dict[str, Any]:
   
    out: Dict[str, Any] = {"name": portfolio.get("name", "Portfolio")}
    
    pos_out: List[Dict[str, Any]] = []
    for p in portfolio.get("positions", []):
        sym, qty = p["symbol"], float(p["quantity"])
        series = _series_for_symbol(price_panel, sym)
        pos_out.append(_compute_position(sym, qty, series, vol_window))

    subs_out: List[Dict[str, Any]] = []
    for sp in portfolio.get("sub_portfolios", []):
        subs_out.append(aggregate_portfolio_sequential(sp, price_panel, vol_window))

    tv_pos, av_pos, dd_pos = _aggregate_children(pos_out)
    tv_sub, av_sub, dd_sub = _aggregate_children(subs_out)
    total_value = round(tv_pos + tv_sub, 2)
    agg_vol = ((av_pos * tv_pos + av_sub * tv_sub) / total_value) if total_value > 0 else 0.0
    max_dd = min(dd_pos, dd_sub)

    out.update({
        "total_value": total_value,
        "aggregate_volatility": float(agg_vol),
        "max_drawdown": float(max_dd),
        "positions": pos_out,
    })
    if subs_out:
        out["sub_portfolios"] = subs_out
    return out


# Main function: aggregate portfolio metrics in parallel for json
def aggregate_portfolio_parallel(portfolio: Dict[str, Any],
                                 price_panel: pd.DataFrame,
                                 vol_window: int = 20,
                                 max_workers: int | None = None) -> Dict[str, Any]:
   
    out: Dict[str, Any] = {"name": portfolio.get("name", "Portfolio")}
    positions = portfolio.get("positions", [])
    subs = portfolio.get("sub_portfolios", [])

    # parallel over this node's positions
    pos_out: List[Dict[str, Any]] = []
    if positions:
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            futs = []
            for p in positions:
                sym, qty = p["symbol"], float(p["quantity"])
                series = _series_for_symbol(price_panel, sym)  # slice before sending to process
                futs.append(ex.submit(_compute_position, sym, qty, series, vol_window))
            for f in as_completed(futs):
                pos_out.append(f.result())

    # sequential recursion for sub-portfolios (keeps structure simple & avoids nested pools)
    subs_out: List[Dict[str, Any]] = []
    for sp in subs:
        subs_out.append(aggregate_portfolio_parallel(sp, price_panel, vol_window, max_workers))

    # aggregate
    tv_pos, av_pos, dd_pos = _aggregate_children(pos_out)
    tv_sub, av_sub, dd_sub = _aggregate_children(subs_out)
    total_value = round(tv_pos + tv_sub, 2)
    agg_vol = ((av_pos * tv_pos + av_sub * tv_sub) / total_value) if total_value > 0 else 0.0
    max_dd = min(dd_pos, dd_sub)

    out.update({
        "total_value": total_value,
        "aggregate_volatility": float(agg_vol),
        "max_drawdown": float(max_dd),
        "positions": pos_out,
    })
    if subs_out:
        out["sub_portfolios"] = subs_out
    return out