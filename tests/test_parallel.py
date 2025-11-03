# tests/test_parallel.py
import numpy as np
import pandas as pd
import polars as pl
import parallel

def _as_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize index and sort for deterministic comparisons.
    Works whether 'timestamp' is a column or the index.
    """
    out = df.copy()
    # If timestamp is a column, sort by it; otherwise rely on index ordering.
    if "timestamp" in out.columns:
        out = out.sort_values(["symbol", "timestamp"])
    else:
        # If index is DatetimeIndex, keep; then sort by symbol
        if not isinstance(out.index, pd.DatetimeIndex):
            # try to promote from column if present
            if "timestamp" in out.columns:
                out = out.set_index("timestamp")
        out = out.sort_index()
        if "symbol" in out.columns:
            out = out.sort_values(["symbol"])
    return out

def test_threading_vs_multiprocessing_pandas(df_pandas: pd.DataFrame):
    # Use a small slice for speed but keep multiple symbols
    small = df_pandas.groupby("symbol", group_keys=False).head(2000).copy()

    thr = parallel.compute_threading(small, library="pandas", window=20)
    pro = parallel.compute_multiprocessing(small, library="pandas", window=20)

    thr = _as_key(thr)
    pro = _as_key(pro)

    # Compare overlapping columns produced by parallel.py
    cols = [c for c in ["return", "ma", "std", "sharpe", "price", "symbol"]
            if c in thr.columns and c in pro.columns]

    # Must have same number of rows once normalized
    assert len(thr) == len(pro)

    for c in cols:
        a = thr[c].to_numpy()
        b = pro[c].to_numpy()

        # numeric vs non-numeric handling
        a_is_num = np.issubdtype(a.dtype, np.number)
        b_is_num = np.issubdtype(b.dtype, np.number)

        if a_is_num and b_is_num:
            # numeric: compare with tolerance, ignoring NaNs
            mask = ~(np.isnan(a) | np.isnan(b))
            if mask.any():
                assert np.allclose(a[mask], b[mask], rtol=1e-7, atol=1e-10)
        else:
            # non-numeric (e.g., 'symbol'): exact match
            assert (a == b).all()

def test_parallel_polars_runs(df_polars: pl.DataFrame):
    # Sanity check: both threading/multiprocessing paths for polars return pl.DataFrame with expected columns
    thr = parallel.compute_threading(df_polars, library="polars", window=20)
    pro = parallel.compute_multiprocessing(df_polars, library="polars", window=20)

    assert isinstance(thr, pl.DataFrame)
    assert isinstance(pro, pl.DataFrame)

    for c in ["return", "ma", "std", "sharpe", "price", "symbol"]:
        assert c in thr.columns
        assert c in pro.columns
