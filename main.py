import time
import matplotlib.pyplot as plt
import polars as pl
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

def bar(values, ylabel, title, outfile):
    plt.figure()
    plt.bar(range(len(labels)), list(values))
    plt.xticks(range(len(labels)), labels, rotation=45, ha="right")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(outfile, dpi=140)
    plt.close()

def _rss_mb():
    return psutil.Process().memory_info().rss / (1024**2)

def _cpu_mb():
    try:
        return psutil.cpu_percent(interval=0.2)  # brief sampling window
    except Exception:
        return None

if __name__ == "__main__":

    rows = []

    cpu0 = _cpu_mb()
    rss0 = _rss_mb() 
    t0 = time.perf_counter()
    df1 = load_pandas("market_data-1.csv")
    t1 = time.perf_counter()
    rss1 = _rss_mb()
    cpu1 = _cpu_mb()

    rows.append({
    "section": "ingestion",
    "lib": "pandas",
    "seconds": round(t1 - t0, 6),
    "rss_mb_delta": round(max(rss1 - rss0, 0.0), 2),
    "cpu_mb_delta": round(max(cpu1 - cpu0, 0.0), 2),
    "rows": len(df1),
    "symbols": df1["symbol"].nunique()
    })
    
    cpu0 = _cpu_mb()
    rss2 = _rss_mb()
    t2 = time.perf_counter()
    df2 = load_polars("market_data-1.csv")
    t3 = time.perf_counter()
    rss3 = _rss_mb()
    cpu1 = _cpu_mb()

    rows.append({
    "section": "ingestion",
    "lib": "polars",
    "seconds": round(t3 - t2, 6),
    "rss_mb_delta": round(max(rss3 - rss2, 0.0), 2),
    "cpu_mb_delta": round(max(cpu1 - cpu0, 0.0), 2),
    "rows": len(df2),
    "symbols": df2["symbol"].n_unique()
    })

    

    cpu0 = _cpu_mb()
    t0 = time.time()
    rss0 = _rss_mb()
    rp = rolling_pandas(df1)
    t1 = time.time()
    rss1 = _rss_mb()
    cpu1 = _cpu_mb()
    rows.append({
        "section": "rolling",
        "lib": "pandas",
        "seconds": round(t1 - t0, 6),
        "rss_mb_delta": round(max(rss1 - rss0, 0.0), 2),
        "cpu_mb_delta": round(max(cpu1 - cpu0, 0.0), 2),
        "rows": len(rp),
        "symbols": rp["symbol"].nunique()
    })

    cpu0 = _cpu_mb()
    rss2 = _rss_mb()
    t2 = time.time()
    rpl = rolling_polars(df2)
    t3 = time.time()
    rss3 = _rss_mb()
    cpu1 = _cpu_mb()
    rows.append({
        "section": "rolling",
        "lib": "polars",
        "seconds": round(t3 - t2, 6),
        "rss_mb_delta": round(max(rss3 - rss2, 0.0), 2),
        "cpu_mb_delta": round(max(cpu1 - cpu0, 0.0), 2),
        "rows": len(rpl),
        "symbols": rpl["symbol"].n_unique()
    })

    cpu0 = _cpu_mb()
    t0 = time.time()
    rss0 = _rss_mb()
    thread_df = parallel.compute_threading(df1,library="pandas")
    t1 = time.time()
    rss1 = _rss_mb()
    cpu1 = _cpu_mb()
    rows.append({
        "section": "threading",
        "lib": "pandas",
        "seconds": round(t1 - t0, 6),
        "rss_mb_delta": round(max(rss1 - rss0, 0.0), 2),
        "cpu_mb_delta": round(max(cpu1 - cpu0, 0.0), 2),
        "rows": len(thread_df),
        "symbols": thread_df["symbol"].nunique()
    })

    cpu0 = _cpu_mb()
    t0 = time.time()
    rss0 = _rss_mb()
    thread_df = parallel.compute_threading(df2,library="polars")
    t1 = time.time()
    rss1 = _rss_mb()
    cpu1 = _cpu_mb()
    rows.append({
        "section": "threading",
        "lib": "polares",
        "seconds": round(t1 - t0, 6),
        "rss_mb_delta": round(max(rss1 - rss0, 0.0), 2),
        "cpu_mb_delta": round(max(cpu1 - cpu0, 0.0), 2),
        "rows": len(thread_df),
        "symbols": thread_df["symbol"].n_unique()
    })

    
    cpu0 = _cpu_mb()
    rss2 = _rss_mb()
    t2 = time.time()
    process_df = parallel.compute_multiprocessing(df1,library="pandas")
    t3 = time.time()
    rss3 = _rss_mb()
    cpu1 = _cpu_mb()
    rows.append({
        "section": "multiprocessing",
        "lib": "pandas",
        "seconds": round(t3 - t2, 6),
        "rss_mb_delta": round(max(rss3 - rss2, 0.0), 2),
        "cpu_mb_delta": round(max(cpu1 - cpu0, 0.0), 2),
        "rows": len(process_df),
        "symbols": process_df["symbol"].nunique()
    })

    cpu0 = _cpu_mb()
    rss2 = _rss_mb()
    t2 = time.time()
    process_df = parallel.compute_multiprocessing(df2,library="polars")
    t3 = time.time()
    rss3 = _rss_mb()
    cpu1 = _cpu_mb()
    rows.append({
        "section": "multiprocessing",
        "lib": "polares",
        "seconds": round(t3 - t2, 6),
        "rss_mb_delta": round(max(rss3 - rss2, 0.0), 2),
        "cpu_mb_delta": round(max(cpu1 - cpu0, 0.0), 2),
        "rows": len(process_df),
        "symbols": process_df["symbol"].n_unique()
    })

    


    # json report file for portfolio


    #portfolio aggregation test
    with open("portfolio_structure-1.json","r",encoding="utf-8") as f:
        port = json.load(f)

    snap_seq = aggregate_portfolio_sequential(port, df1, vol_window=20)
    snap_par = aggregate_portfolio_parallel(port, df1, vol_window=20, max_workers=None)
    assert abs(snap_seq["total_value"] - snap_par["total_value"]) < 1e-6
    #print(json.dumps(snap_par, indent=2))
    #save json output
    with open("portfolio_snapshot_output.json","w",encoding="utf-8") as f:
        json.dump(snap_par, f, indent=2)



    # print summary table
    perf_df = pd.DataFrame(rows)
    print("\n=== Performance Summary ===")
    print(perf_df)

    perf_df.to_csv("performance_summary.csv", index=False)
        # bar plots

    with open("performance_report.md", "w", encoding="utf-8") as f:
        f.write("# Performance Report\n\n")
        f.write(perf_df.to_markdown(index=False))
        f.write("\n")

    labels = [f"{a}|{b}" for a,b in zip(perf_df["section"].astype(str),
                                    perf_df["lib"].astype(str))]

    bar(perf_df["seconds"].astype(float).fillna(0.0),
        "seconds", "Performance: seconds", "performance_seconds.png")

    if "rss_mb_delta" in perf_df.columns:
        bar(perf_df["rss_mb_delta"].astype(float).fillna(0.0),
            "MB", "Performance: RSS delta (MB)", "performance_rss_delta.png")

    vms_col = "vms_mb_delta" if "vms_mb_delta" in perf_df.columns else (
            "cpu_mb_delta" if "cpu_mb_delta" in perf_df.columns else None)
    if vms_col:
        bar(perf_df[vms_col].astype(float).fillna(0.0),
            "MB", f"Performance: {vms_col}", "performance_vms_delta.png")

    print("Saved: performance_seconds.png, performance_rss_delta.png, performance_vms_delta.png")# plot example for pandas rolling

    s = "AAPL"
    sub = rp[rp["symbol"]==s]
    plt.plot(sub.index, sub["price"], label="price")
    plt.plot(sub.index, sub["ma"], label="ma")
    plt.legend()
    plt.show()
