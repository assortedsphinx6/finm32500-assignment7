import time, matplotlib.pyplot as plt
from data_loader import load_pandas, load_polars
from metrics import rolling_pandas, rolling_polars
import parallel

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


