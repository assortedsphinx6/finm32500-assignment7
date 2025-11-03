import pandas as pd
import polars as pl
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp

# Pandas metrics
def compute_metrics_for_symbol(df_symbol: pd.DataFrame, window=20):
    df_symbol = df_symbol.copy()
    df_symbol["return"] = df_symbol["price"].pct_change()
    df_symbol["ma"] = df_symbol["price"].rolling(window).mean()
    df_symbol["std"] = df_symbol["return"].rolling(window).std()
    df_symbol["sharpe"] = df_symbol["return"] / df_symbol["std"]
    return df_symbol

# Polars metrics
def polars_compute_metrics(df_symbol: pl.DataFrame, window=20):
    # compute return first
    df_symbol = df_symbol.with_columns([
        pl.col("price").pct_change().alias("return")
    ])
    # compute rolling metrics
    df_symbol = df_symbol.with_columns([
        pl.col("price").rolling_mean(window).alias("ma"),
        pl.col("return").rolling_std(window).alias("std"),
        (pl.col("return") / pl.col("return").rolling_std(window)).alias("sharpe")
    ])
    return df_symbol


# Threading
def compute_threading(df, library="pandas", window=20):
    results = []

    # split into groups
    if library == "pandas":
        func = compute_metrics_for_symbol
        groups = [df_symbol for _, df_symbol in df.groupby("symbol")]
    elif library == "polars":
        func = polars_compute_metrics
        groups = [df.filter(pl.col("symbol") == s) for s in df["symbol"].unique().to_list()]
    else:
        raise ValueError("library must be 'pandas' or 'polars'")

    # submit to threads
    with ThreadPoolExecutor() as executor:
        for df_symbol in groups:
            results.append(executor.submit(func, df_symbol, window))

    # collect results
    if library == "pandas":
        final_df = pd.concat([f.result() for f in results])
    else:
        final_df = pl.concat([f.result() for f in results])

    return final_df


# Multiprocessing
def compute_multiprocessing(df, library="pandas", window=20):
    # split into groups
    if library == "pandas":
        func = compute_metrics_for_symbol
        groups = [df_symbol for _, df_symbol in df.groupby("symbol")]
    elif library == "polars":
        func = polars_compute_metrics
        groups = [df.filter(pl.col("symbol") == s) for s in df["symbol"].unique().to_list()]
    else:
        raise ValueError("library must be 'pandas' or 'polars'")

    # map to processes
    with ProcessPoolExecutor(mp.cpu_count()) as executor:
        results = list(executor.map(func, groups, [window]*len(groups)))

    # collect results
    if library == "pandas":
        final_df = pd.concat(results)
    else:
        final_df = pl.concat(results)

    return final_df

