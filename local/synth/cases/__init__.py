"""CASE CATALOG, one file per theme (GROUPS below, in catalog order). One function per case; each builds the
source rows and the EXPECTED 3.0 outcome. Rendered as synth/CASES.md; `./stand.sh explain <case>` shows details.

Conventions
  * Every natural key starts with the case id (fulldivcode 'C03_1', address text 'C13 fact', type 'C26_A', ...),
    so any target row can be traced back to its case. Keys without the id are declared with Case.owns().
  * Expectations are written in 3.0 terms (3.0 is the reference). Values:
      dates 'yyyy-MM-dd'; MIN=1900-01-01, MAX=9999-12-31
      ctl_validfrom class: START (= start_dt 00:00), RUN@Lk (run timestamp of load Lk), NULL
      ctl_loading: load label (Lk); *_sk: natural key of the referenced row (or NOTNULL / None)
  * An expectation lists ALL rows of the key in that table at that load (snp: whole table state,
    diff: only rows with ctl_loading = that load), projected to the listed columns. [] means "no rows".
  * Dates: d('L3') = business date of L3, d('L3', -1) = D-1, d('L3', -2) = D-2.
"""
import importlib

from model import Case

GROUPS = ["history", "hubs", "addr", "hash", "sources", "links", "keys", "modes",
          "audit_hubs1", "audit_hubs2", "audit_hist", "audit_stgwrite", "audit_run", "audit_perf", "audit_cross", "audit_cover",
          "audit_dirty_hubs", "audit_dirty_hist", "audit_dirty_stgrun", "audit_dirty_migrate", "audit_dirty_perf"]
DV77 = "недетерминизм 3.0: ничья окна (DV-77)"
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
