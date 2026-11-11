"""Same metrics as hist_layout.py, but straight from the raw parquet exports exp<v>/<load>/snp_<table>
(before the stand's compare step). div_sk -> sid via snp_div_. usage: raw_hist.py <runDir> <v> <load> [prefix]"""
import os, sys, glob
from collections import Counter, defaultdict
import pyarrow.parquet as pq

run, v, ld = sys.argv[1:4]
pref = sys.argv[4] if len(sys.argv) > 4 else "HL"
base = os.path.join(run, "exp" + v, ld)


def rows(t):
    fs = glob.glob(os.path.join(base, t, "**", "*.parquet"), recursive=True)
    out = []
    for f in fs:
        if "_temporary" in f:
            continue
        out += pq.read_table(f).to_pylist()
    return out


sid = {r["div_sk"]: r["sid"] for r in rows("snp_div_")}
for t in ["snp_div_h", "snp_div_type", "snp_addr_h", "snp_div_div_relation", "diff_div_h"]:
    rs = rows(t)
    if not rs:
        continue
    if "div_sk" in rs[0]:
        for r in rs:
            r["_key"] = str(sid.get(r["div_sk"], r["div_sk"]))
        rs = [r for r in rs if r["_key"].startswith(pref)]
    else:
        for r in rs:
            r["_key"] = str(r.get("addr_sk"))
    by = defaultdict(list)
    for r in rs:
        by[r["_key"]].append(r)
    audit = real = one = dopen = 0
    ov = []
    for k, vs in by.items():
        if t.startswith("diff"):
            continue
        vs.sort(key=lambda r: (str(r["start_dt"]), str(r["end_dt"])))
        dopen += sum(1 for r in vs if str(r["end_dt"]) == "9999-12-31") >= 2
        for p, c in zip(vs, vs[1:]):
            if str(p["start_dt"]) <= str(c["end_dt"]): audit += 1
            if str(c["start_dt"]) <= str(p["end_dt"]):
                real += 1; ov.append("%s [%s..%s] x [%s..%s]" % (k, p["start_dt"], p["end_dt"], c["start_dt"], c["end_dt"]))
            if str(c["start_dt"]) == str(p["end_dt"]): one += 1
    print("%s %s %-22s rows=%d keys=%d rows-keys=%d audit_B3=%d real_overlap=%d one_day=%d double_open=%d"
          % (v, ld, t, len(rs), len(by), len(rs) - len(by), audit, real, one, dopen))
    for o in ov: print("     overlap", o)
    h = Counter((r.get("p1month"), r.get("ctl_loading"), str(r["end_dt"])) for r in rs)
    for k2, n in sorted(h.items(), key=str): print("     p1month=%s loading=%s end=%s : %d" % (k2 + (n,)))
    if t in ("snp_div_h", "diff_div_h"):
        for r in sorted(rs, key=lambda r: (r["_key"], str(r["start_dt"]), str(r.get("ctl_do")))):
            print("       %-6s %s..%s p=%s ld=%s vf=%s %s %s %s" % (r["_key"], r["start_dt"], r["end_dt"], r.get("p1month"),
                  r["ctl_loading"], r["ctl_validfrom"], r.get("ctl_do", ""), r.get("ctl_action", ""), r["div_name"]))
