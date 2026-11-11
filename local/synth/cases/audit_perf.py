"""Audit 1.3, theme perf (ids F..): DV-46, DV-50, DV-53, DV-54, DV-55.

No cases on purpose. All five candidates are about load (driver memory, YARN applications, extra scans, cache
retention, Spark config), not about table content: on any input the 3.0 and 4.0 rows are the same, so a case here
would PASS on both sides whether the candidate is real or not. The stand cannot observe the difference
(spark.ui.enabled=false, no event log / SparkListener, local[2] in one JVM, hand-set Spark config, thousands of rows;
SVOD "stand limitations" 16, 19, 21). What each candidate needs instead is in the 1.3 report (per_dv):

  DV-46  broadcast(cand) in PeriodRewriteCompatCalculator:209-218; 3.0/ESSA has no broadcast hint at all.
         Content of the cand path is already checked by C11 (new key), C06 / C10 (div_type returner).
  DV-50  one spawned app + thread pool vs ~16 YARN apps: IFT only.
  DV-53  needsLaunch reads diff/snp/stg on every INCR (PublishPreconditions:44-79): runs in every INCR of every
         chain; content side of its branches - R01 (rerun); the extra reads need job/byte metrics.
  DV-54  readFrozenDraft(...).cache() without unpersist (BaseTransformation:203-206): needs a SparkListener /
         getPersistentRDDs probe; executors under dynamicAllocation - IFT.
  DV-55  Spark parameters: Environment tab 3.0 vs 4.0 on IFT/prom; gzip can be read from parquet footers.
"""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case  # noqa: F401

for _f in []:
    _f()
