import time
import matplotlib.pyplot as plt
import polars as pl
import psutil
from data_loader import load_pandas, load_polars
from metrics import rolling_pandas, rolling_polars
import parallel
import psutil
import pandas as pd



import json
from portfolio import (
    aggregate_portfolio_sequential,
    aggregate_portfolio_parallel,
)

def _rss_mb():
    return psutil.Process().memory_info().rss / (1024**2)


if __name__ == "__main__":

    rows = []

    rss0 = _rss_mb() 
    t0 = time.perf_counter()
    df1 = load_pandas("market_data-1.csv")
    t1 = time.perf_counter()
    rss1 = _rss_mb()

    rows.append({
    "section": "ingestion",
    "lib": "pandas",
    "seconds": round(t1 - t0, 6),
    "rss_mb_delta": round(max(rss1 - rss0, 0.0), 2),
    "rows": len(df1),
    "symbols": df1["symbol"].nunique()
    })
    
    rss2 = _rss_mb()
    t2 = time.perf_counter()
    df2 = load_polars("market_data-1.csv")
    t3 = time.perf_counter()
    rss3 = _rss_mb()

    rows.append({
    "section": "ingestion",
    "lib": "polars",
    "seconds": round(t3 - t2, 6),
    "rss_mb_delta": round(max(rss3 - rss2, 0.0), 2),
    "rows": len(df2),
    "symbols": df2["symbol"].n_unique()
    })

    

    # Rolling Metrics (single-threaded)
    t0 = time.time()
    rp = rolling_pandas(dfp)
    t1 = time.time()

    t0 = time.time()
    rss0 = _rss_mb()
    rp = rolling_pandas(dfp)
    t1 = time.time()
    rss1 = _rss_mb()
    rows.append({
        "section": "rolling",
        "lib": "pandas",
        "seconds": round(t1 - t0, 6),
        "rss_mb_delta": round(max(rss1 - rss0, 0.0), 2),
        "rows": len(rp),
        "symbols": rp["symbol"].nunique()
    })

    rss2 = _rss_mb()
    t2 = time.time()
    rpl = rolling_polars(dfpl)
    t3 = time.time()
    rss3 = _rss_mb()
    rows.append({
        "section": "rolling",
        "lib": "polars",
        "seconds": round(t3 - t2, 6),
        "rss_mb_delta": round(max(rss3 - rss2, 0.0), 2),
        "rows": len(rpl),
        "symbols": rpl["symbol"].n_unique()
    })

  
    t0 = time.time()
    rss0 = _rss_mb()
    thread_df, thread_time = parallel.compute_threading(dfp)
    t1 = time.time()
    rss1 = _rss_mb()
    rows.append({
        "section": "concurrency",
        "lib": "threading",
        "seconds": round(t1 - t0, 6),
        "rss_mb_delta": round(max(rss1 - rss0, 0.0), 2),
        "rows": len(thread_df),
        "symbols": thread_df["symbol"].nunique()
    })
    print(f"Threading time: {thread_time:.4f}s")

    rss2 = _rss_mb()
    t2 = time.time()
    process_df, process_time = parallel.compute_multiprocessing(dfp)
    t3 = time.time()
    rss3 = _rss_mb()
    rows.append({
        "section": "concurrency",
        "lib": "multiprocessing",
        "seconds": round(t3 - t2, 6),
        "rss_mb_delta": round(max(rss3 - rss2, 0.0), 2),
        "rows": len(process_df),
        "symbols": process_df["symbol"].nunique()
    })
    print(f"Multiprocessing time: {process_time:.4f}s")

    # print summary table
    perf_df = pd.DataFrame(rows)
    print("\n=== Performance Summary ===")
    print(perf_df)

    perf_df.to_csv("performance_summary.csv", index=False)

    with open("performance_report.md", "w", encoding="utf-8") as f:
        f.write("# Performance Report\n\n")
        f.write(perf_df.to_markdown(index=False))
        f.write("\n")

    #portfolio aggregation test
    with open("portfolio_structure-1.json","r",encoding="utf-8") as f:
        port = json.load(f)

    snap_seq = aggregate_portfolio_sequential(port, dfp, vol_window=20)
    snap_par = aggregate_portfolio_parallel(port, dfp, vol_window=20, max_workers=None)
    assert abs(snap_seq["total_value"] - snap_par["total_value"]) < 1e-6
    #print(json.dumps(snap_par, indent=2))
    #save json output
    with open("portfolio_snapshot_output.json","w",encoding="utf-8") as f:
        json.dump(snap_par, f, indent=2)



    s = "AAPL"
    sub = rp[rp["symbol"]==s]
    plt.plot(sub.index, sub["price"], label="price")
    plt.plot(sub.index, sub["ma"], label="ma")
    plt.legend()
    plt.show()
