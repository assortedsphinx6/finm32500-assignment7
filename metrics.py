import pandas as pd
import polars as pl

def rolling_pandas(df, window=20):
    df = df.reset_index(drop=False)
    out = df.copy()
    g = out.groupby("symbol", group_keys=False)
    out["ret"] = g["price"].pct_change()
    out["ma"] = g["price"].transform(lambda x: x.rolling(window).mean())
    out["std"] = g["price"].transform(lambda x: x.rolling(window).std())
    out["sharpe"] = g["ret"].transform(lambda x: x.rolling(window).mean() / x.rolling(window).std())
    return out

def rolling_polars(df, window=20):
    df = df.sort(["symbol", "timestamp"])
    df = df.with_columns((pl.col("price") / pl.col("price").shift(1) - 1).over("symbol").alias("ret"))
    df = df.with_columns([
        pl.col("price").rolling_mean(window_size=window).over("symbol").alias("ma"),
        pl.col("price").rolling_std(window_size=window).over("symbol").alias("std"),
        pl.col("ret").rolling_mean(window_size=window).over("symbol").alias("m"),
        pl.col("ret").rolling_std(window_size=window).over("symbol").alias("s"),
    ])
    df = df.with_columns((pl.col("m") / pl.col("s")).alias("sharpe")).drop(["m","s"])
    return df