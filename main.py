import time
import matplotlib.pyplot as plt
import polars as pl
import psutil
from data_loader import load_pandas, load_polars
from metrics import rolling_pandas, rolling_polars
import parallel

if __name__ == "__main__":
    # Load CSV
    t0 = time.time()
    dfp = load_pandas("market_data-1.csv")
    t1 = time.time()

    t2 = time.time()
    dfpl = load_polars("market_data-1.csv")
    t3 = time.time()

    print("CSV Load Times:")
    print("Pandas:", round(t1 - t0, 4), "s", dfp.shape)
    print("Polars:", round(t3 - t2, 4), "s", dfpl.shape)

    # Rolling Metrics (single-threaded)
    t0 = time.time()
    rp = rolling_pandas(dfp)
    t1 = time.time()

    t2 = time.time()
    rpl = rolling_polars(dfpl)
    t3 = time.time()

    print("\nRolling Computation Times (Single-threaded):")
    print("Pandas:", round(t1 - t0, 4), "s")
    print("Polars:", round(t3 - t2, 4), "s")

    # Visualize one symbol
    s = "AAPL"
    sub_pandas = rp[rp["symbol"] == s]
    sub_polars = rpl.filter(pl.col("symbol") == s).to_pandas()

    plt.figure(figsize=(10,5))
    plt.plot(sub_pandas.index, sub_pandas["price"], label="Price (Pandas)")
    plt.plot(sub_pandas.index, sub_pandas["ma"], label="MA (Pandas)")
    plt.plot(sub_polars.index, sub_polars["price"], '--', label="Price (Polars)")
    plt.plot(sub_polars.index, sub_polars["ma"], '--', label="MA (Polars)")
    plt.title(f"{s} Price & 20-period MA (Pandas vs Polars)")
    plt.legend()
    plt.show()

    # CPU/Memory helper
    def run_with_metrics(func, df, **kwargs):
        process = psutil.Process()
        cpu_before = psutil.cpu_percent(interval=None)
        mem_before = process.memory_info().rss / 1024**2
        start_time = time.time()
        result = func(df, **kwargs)
        elapsed = time.time() - start_time
        cpu_after = psutil.cpu_percent(interval=None)
        mem_after = process.memory_info().rss / 1024**2
        return result, elapsed, cpu_after - cpu_before, mem_after - mem_before

    # Threading - Pandas
    thread_df, t_time, cpu, mem = run_with_metrics(parallel.compute_threading, dfp)
    print("\nThreading (Pandas):")
    print(f"Time: {t_time:.4f}s, CPU change: {cpu:.1f}%, Memory change: {mem:.2f} MB")

    # Multiprocessing - Pandas
    process_df, p_time, cpu, mem = run_with_metrics(parallel.compute_multiprocessing, dfp)
    print("\nMultiprocessing (Pandas):")
    print(f"Time: {p_time:.4f}s, CPU change: {cpu:.1f}%, Memory change: {mem:.2f} MB")

    # Threading - Polars
    thread_df_pl, t_time, cpu, mem = run_with_metrics(parallel.compute_threading, dfpl, library="polars")
    print("\nThreading (Polars):")
    print(f"Time: {t_time:.4f}s, CPU change: {cpu:.1f}%, Memory change: {mem:.2f} MB")

    # Multiprocessing - Polars
    process_df_pl, p_time, cpu, mem = run_with_metrics(parallel.compute_multiprocessing, dfpl, library="polars")
    print("\nMultiprocessing (Polars):")
    print(f"Time: {p_time:.4f}s, CPU change: {cpu:.1f}%, Memory change: {mem:.2f} MB")