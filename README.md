# finm32500-assignment7
Parallel Computing for Financial Data Processing

part 2 
syntax discussion 
##
Pandas vs Polars (Rolling Analytics)
#
pandas: index-centric; groupby("symbol") + rolling(20) on a DatetimeIndex. Super simple for quick analysis/plots.
#
polars: expression-centric; with_columns(...) + window ops .over("symbol"). Use .lazy().collect() for optimizer benefits.
##
Performance
#
pandas: mostly single-threaded; speed up CPU-bound work with multiprocessing per symbol (some pickling/IPC overhead).
#
polars: Rust engine, multi-threaded by default + lazy optimizer; usually faster and more memory-efficient on large, multi-symbol data.
##
IO & Memory
#
pandas: fine for small/medium; can create heavier intermediates with groupby().rolling().
#
polars: faster multi-threaded CSV/Parquet; Arrow columnar memory is leaner.
##
When to choose
#
pandas: small–medium datasets, rapid iteration, plotting, rich PyData ecosystem.
#
polars: large pipelines, many symbols, need parallelism/low memory. Prefer one lazy pipeline over manual Python threading.

part 3 
GIL limitations 
#
Python’s Global Interpreter Lock (GIL) allows only one thread to execute Python bytecode at a time
#However, threading still helps for I/O-bound tasks or when using libraries like NumPy or Polars that release the GIL during heavy computations. 
#Multiprocessing, on the other hand, spawns separate Python processes, each with its own GIL and memory space, enabling true parallelism. 
#
As we can see threading provided fast speedups for Polars and moderate improvements for Pandas due to vectorized operations releasing the GIL. 
#Multiprocessing showed higher CPU utilization but was slower for smaller datasets because of process startup and data serialization overhead. 
#
Thus, threading is preferred for lightweight or GIL-releasing operations, while multiprocessing is best suited for CPU-bound tasks with large computations for which the benefits of multiprocessing outweigh the overhead of process creation.



part 5 
Discuss tradeoffs in syntax, ecosystem, and scalability.

Syntax. Pandas is index-centric and imperative: you set a DatetimeIndex, chain groupby → rolling → assign, and see results immediately. That style is great for ad-hoc exploration and plots because it mirrors how you think in a notebook. Polars is expression-centric and more declarative: you build transformations with with_columns(...) and window expressions like .over("symbol"), ideally in lazy mode and then .collect(). It feels closer to SQL/functional pipelines, which can make complex, multi-step jobs cleaner and easier to optimize, but it’s a small mental shift if you’re used to pandas.

Ecosystem. Pandas sits at the center of PyData, so it plugs straight into NumPy, SciPy, scikit-learn, statsmodels, matplotlib/plotly, and countless recipes, blog posts, and StackOverflow answers. Polars’ ecosystem is newer but growing quickly, with strong Arrow/DuckDB/Rust interop and excellent file IO; however, for many downstream tasks (advanced plotting, ML libraries, econometrics) you’ll still often convert to pandas. In practice, teams frequently use both: Polars to ingest/transform at speed, then .to_pandas() for modeling/visuals.

Scalability & performance. Pandas is mostly single-threaded at the Python layer, so it scales by vectorization and, for CPU-bound workloads, by multiprocessing (e.g., per-symbol chunks)—which works but incurs pickling/IPC overhead and can inflate memory with groupby().rolling() intermediates. Polars brings a Rust engine with built-in multithreading and a query optimizer that fuses operations, pushes down projections/filters, and exploits columnar (Arrow) memory—so large multi-symbol pipelines and heavy IO typically run faster and leaner. For pandas, you parallelize at the Python level; for Polars, you usually get better results by expressing one lazy pipeline and letting the engine handle parallelism.
