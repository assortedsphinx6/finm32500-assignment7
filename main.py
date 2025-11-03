import time, matplotlib.pyplot as plt
from data_loader import load_pandas, load_polars
from metrics import rolling_pandas, rolling_polars
import parallel

import json
from portfolio import (
    aggregate_portfolio_sequential,
    aggregate_portfolio_parallel,
)

if __name__ == "__main__":
    t0 = time.time()
    df1 = load_pandas("market_data-1.csv")
    t1 = time.time()

    t2 = time.time()
    df2 = load_polars("market_data-1.csv")
    t3 = time.time()

    print("pandas:", round(t1 - t0, 4), "s", df1.shape)
    print("polars:", round(t3 - t2, 4), "s", df2.shape)

    dfp = load_pandas("market_data-1.csv")
    dfpl = load_polars("market_data-1.csv")

    t0 = time.time(); rp = rolling_pandas(dfp); t1 = time.time()
    t2 = time.time(); rpl = rolling_polars(dfpl); t3 = time.time()

    print("pandas:", round(t1 - t0,4), "s")
    print("polars:", round(t3 - t2,4), "s")

    s = "AAPL"
    sub = rp[rp["symbol"]==s]
    plt.plot(sub.index, sub["price"], label="price")
    plt.plot(sub.index, sub["ma"], label="ma")
    plt.legend()
    plt.show()

    thread_df, thread_time = parallel.compute_threading(dfp)
    print(f"Threading time: {thread_time:.4f}s")

    process_df, process_time = parallel.compute_multiprocessing(dfp)
    print(f"Multiprocessing time: {process_time:.4f}s")


    #portfolio aggregation test
    with open("portfolio_structure-1.json","r",encoding="utf-8") as f:
        port = json.load(f)

    snap_seq = aggregate_portfolio_sequential(port, dfp, vol_window=20)
    snap_par = aggregate_portfolio_parallel(port, dfp, vol_window=20, max_workers=None)
    assert abs(snap_seq["total_value"] - snap_par["total_value"]) < 1e-6
    print(json.dumps(snap_par, indent=2))
    #save json output
    with open("portfolio_snapshot_output.json","w",encoding="utf-8") as f:
        json.dump(snap_par, f, indent=2)