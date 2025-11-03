# tests/conftest.py
import json
import os
import pytest
import pandas as pd
import polars as pl

from data_loader import load_pandas, load_polars

CSV_PATH = os.environ.get("MARKET_CSV", "market_data-1.csv")
PORTFOLIO_JSON = os.environ.get("PORTFOLIO_JSON", "portfolio_structure-1.json")

@pytest.fixture(scope="session")
def df_pandas() -> pd.DataFrame:
    df = load_pandas(CSV_PATH)
    # keep test runtime small: use first N per symbol if very large
    if len(df) > 100_000:
        df = df.groupby("symbol", group_keys=False).head(5000)
    return df

@pytest.fixture(scope="session")
def df_polars() -> pl.DataFrame:
    df = load_polars(CSV_PATH)
    # keep runtime small
    if df.height > 100_000:
        syms = df.select(pl.col("symbol").unique()).to_series().to_list()
        small = []
        for s in syms:
            small.append(df.filter(pl.col("symbol")==s).head(5000))
        df = pl.concat(small)
    return df

@pytest.fixture(scope="session")
def sample_symbol(df_pandas) -> str:
    return df_pandas["symbol"].iloc[0]

@pytest.fixture(scope="session")
def portfolio_dict():
    with open(PORTFOLIO_JSON, "r", encoding="utf-8") as f:
        return json.load(f)
