import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import polars as pl
from data_loader import load_pandas, load_polars


def test_load_pandas(tmp_path):
    f = tmp_path / "market_data-1.csv"
    f.write_text("timestamp,symbol,price\n2024-01-01 09:30:00,AAPL,170.5\n2024-01-01 09:31:00,MSFT,310.2\n")
    df = load_pandas(f)
    assert isinstance(df, pd.DataFrame)
    assert {"symbol", "price"}.issubset(df.columns)
    assert pd.api.types.is_datetime64_any_dtype(df.index)
    assert len(df) == 2

def test_load_polars(tmp_path):
    f = tmp_path / "market_data-1.csv"
    f.write_text("timestamp,symbol,price\n2024-01-01 09:30:00,AAPL,170.5\n2024-01-01 09:31:00,MSFT,310.2\n")
    df = load_polars(f)
    assert isinstance(df, pl.DataFrame)
    assert {"timestamp", "symbol", "price"}.issubset(df.columns)
    assert df.height == 2