import pandas as pd
import polars as pl

def load_pandas(path):
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    return df

def load_polars(path):
    df = pl.read_csv(path)
    df = df.with_columns(pl.col("timestamp").str.to_datetime())
    return df