# Performance Report

| section         | lib     |   seconds |   rss_mb_delta |   cpu_mb_delta |   rows |   symbols |
|:----------------|:--------|----------:|---------------:|---------------:|-------:|----------:|
| ingestion       | pandas  |  0.200598 |          11.12 |            3.8 | 300000 |         3 |
| ingestion       | polars  |  0.169497 |          32.07 |           10.6 | 300000 |         3 |
| rolling         | pandas  |  0.159495 |          16.62 |            0   | 300000 |         3 |
| rolling         | polars  |  0.050694 |          90.84 |            0   | 300000 |         3 |
| threading       | pandas  |  0.0547   |          17.76 |            0   | 300000 |         3 |
| threading       | polares |  0.018049 |           2.05 |            2.1 | 300000 |         3 |
| multiprocessing | pandas  |  1.47749  |          16.1  |           11.8 | 300000 |         3 |
| multiprocessing | polares |  1.38959  |           5.01 |            0   | 300000 |         3 |
