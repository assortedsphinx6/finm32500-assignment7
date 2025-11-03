import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import time

def compute_metrics_for_symbol(df_symbol, window=20):
    df_symbol = df_symbol.copy()
    df_symbol["return"] = df_symbol["price"].pct_change()
    df_symbol["ma"] = df_symbol["price"].rolling(window).mean()
    df_symbol["std"] = df_symbol["return"].rolling(window).std()
    df_symbol["sharpe"] = df_symbol["return"] / df_symbol["std"]
    return df_symbol


def compute_threading(df):
    start = time.time()
    results = []

    with ThreadPoolExecutor() as executor:
        for _, df_symbol in df.groupby("symbol"):
            results.append(executor.submit(compute_metrics_for_symbol, df_symbol))

    final_df = pd.concat([f.result() for f in results])
    return final_df, time.time() - start

def compute_multiprocessing(df):
    start = time.time()
    results = []
    with ProcessPoolExecutor(mp.cpu_count()) as executor:
        for _, df_symbol in df.groupby("symbol"):
            results.append(executor.submit(compute_metrics_for_symbol, df_symbol))

    final_df = pd.concat(f.result() for f in results)
    return final_df, time.time() - start
