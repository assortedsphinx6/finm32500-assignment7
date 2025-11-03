# Performance Report

| section     | lib             |   seconds |   rss_mb_delta |   rows |   symbols |
|:------------|:----------------|----------:|---------------:|-------:|----------:|
| ingestion   | pandas          |  0.197838 |          11.68 | 300000 |         3 |
| ingestion   | polars          |  0.144853 |          30.16 | 300000 |         3 |
| rolling     | pandas          |  0.164945 |          16.47 | 300000 |         3 |
| rolling     | polars          |  0.048029 |          70.64 | 300000 |         3 |
| concurrency | threading       |  0.037337 |          15.2  | 300000 |         3 |
| concurrency | multiprocessing |  1.33157  |          15.45 | 300000 |         3 |
