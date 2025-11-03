import datetime as dt
import pandas as pd
import polars as pl
from metrics import rolling_pandas, rolling_polars

def test_rolling_pandas_basic():
    idx = pd.date_range("2024-01-01", periods=25, freq="D", tz="UTC")
    df = pd.DataFrame({
        "timestamp": list(idx)*2,
        "symbol": ["AAPL"]*25 + ["MSFT"]*25,
        "price": list(range(100,125)) + list(range(200,225)),
    })
    r = rolling_pandas(df, window=5)
    assert all(c in r.columns for c in ["ma","std","sharpe"])
    assert len(r) == len(df)

def test_rolling_polars_basic():
    start = dt.datetime(2024,1,1)
    end = dt.datetime(2024,1,25)
    ts = pl.date_range(start=start, end=end, interval="1d", eager=True)
    df = pl.DataFrame({
        "timestamp": ts.to_list() + ts.to_list(),
        "symbol": ["AAPL"]*25 + ["MSFT"]*25,
        "price": list(range(100,125)) + list(range(200,225)),
    })
    r = rolling_polars(df, window=5)
    assert all(c in r.columns for c in ["ma","std","sharpe"])
    assert r.height == df.height