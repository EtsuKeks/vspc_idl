"""DIRTY case catalog: patterns found in real DEV data (dev_data_anomalies_20261109.txt), grouped by the audit's
classes. Purpose: DIAGNOSE whether 3.0 and 4.0 diverge on dirty data - the cases carry no 3.0 expectations
(3.0 vs 4.0 parity is the check); `./stand.sh explain <case>` shows the source rows / seeded SQL per load.

  b_dups    B1-B4  duplicate keys: twins in div_, double open div_h versions, overlapping intervals, junk diff legs
  c_nulls   C1     mass NULL foreign keys
  d_fk      D1-D2  dangling parent / time zone references
  e_hist    E1-E2  garbage dates, deep / spread / duplicate d_ history
  f_values  F1-F4  spaces, long strings, shared and literal 'null' names, p1month layouts
  g_stg     G1     staging garbage, stale ctl_log, emptied loader cache

Chains: dsrc (source-side dirt, A0..A3) and dtgt (target-side dirt seeded by SQL before T1/T3, T0..T3).
Keys contain the case id (B11_1, 'F21 ...'); keys without it are declared with Case.owns().
"""
import importlib

from model import Case

GROUPS = ["b_dups", "c_nulls", "d_fk", "e_hist", "f_values", "g_stg", "h_cut"]
CASES = []
_group = [None]


def case(**kw):
    c = Case(**kw)
    c.group = _group[0]
    CASES.append(c)
    return c


for _g in GROUPS:
    _group[0] = _g
    importlib.import_module("cases." + _g)
