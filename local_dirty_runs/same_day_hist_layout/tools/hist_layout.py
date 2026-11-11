"""Interval/layout metrics over stand dumps (dump/<v>/<load>/snp.<table>.tsv) of historical tables.
usage: python3 -I hist_layout.py <runDir> [case_prefix]"""
import csv, os, sys
from collections import Counter, defaultdict

MAX = "9999-12-31"
TABLES = ["snp.div_h", "snp.div_type", "snp.addr_h", "snp.div_div_relation"]


def read(p):
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))


def metrics(rows):
    by = defaultdict(list)
    for r in rows:
        by[r["_key"]].append(r)
    audit = real = oneday = dopen = 0
    over_keys = []
    for k, vs in by.items():
        vs.sort(key=lambda r: (r["start_dt"], r["end_dt"]))
        if sum(1 for r in vs if r["end_dt"] in (MAX, "\\N")) >= 2:
            dopen += 1
        for p, c in zip(vs, vs[1:]):
            pe = MAX if p["end_dt"] == "\\N" else p["end_dt"]
            ce = MAX if c["end_dt"] == "\\N" else c["end_dt"]
            if p["start_dt"] <= ce:           # audit B3 predicate: LAG(start_dt) <= COALESCE(end_dt, MAX)
                audit += 1
            if c["start_dt"] <= pe:           # real overlap: start <= LAG(end_dt)
                real += 1
                over_keys.append("%s[%s..%s]x[%s..%s]" % (k, p["start_dt"], pe, c["start_dt"], ce))
            if c["start_dt"] == pe:
                oneday += 1
    return dict(rows=len(rows), keys=len(by), audit=audit, real=real, oneday=oneday, dopen=dopen), over_keys


def main():
    run = sys.argv[1]
    pref = sys.argv[2] if len(sys.argv) > 2 else ""
    for v in ("30", "40"):
        d = os.path.join(run, "dump", v)
        if not os.path.isdir(d):
            continue
        for ld in sorted(os.listdir(d)):
            print("=" * 100)
            print("version %s  load %s" % (v, ld))
            for t in TABLES:
                rows = read(os.path.join(d, ld, t + ".tsv"))
                if rows is None:
                    continue
                rows = [r for r in rows if r["_case"].startswith(pref)]
                m, ov = metrics(rows)
                print("  %-22s rows=%d keys=%d rows-keys=%d audit_B3=%d real_overlap=%d one_day=%d double_open_keys=%d"
                      % (t, m["rows"], m["keys"], m["rows"] - m["keys"], m["audit"], m["real"], m["oneday"], m["dopen"]))
                for o in ov:
                    print("      overlap " + o)
                h = Counter((r["p1month"], r["ctl_loading"], r["end_dt"]) for r in rows)
                for (pm, cl, ed), n in sorted(h.items()):
                    print("      p1month=%s ctl_loading=%s end_dt=%s : %d" % (pm, cl, ed, n))
                init = [r for r in rows if r["ctl_loading"] == "D0"]
                if init:
                    print("      D0 rows: %d, ctl_validfrom=START(start_dt) %d, other %s"
                          % (len(init), sum(1 for r in init if r["ctl_validfrom"] == "START"),
                             dict(Counter(r["ctl_validfrom"] for r in init if r["ctl_validfrom"] != "START"))))
            # per key listing of div_h
            rows = read(os.path.join(d, ld, "snp.div_h.tsv")) or []
            rows = [r for r in rows if r["_case"].startswith(pref)]
            print("  div_h versions:")
            for r in sorted(rows, key=lambda r: (r["_key"], r["start_dt"], r["end_dt"])):
                print("    %-6s %s..%s p=%s ld=%s vf=%s %s" % (r["_key"], r["start_dt"], r["end_dt"], r["p1month"],
                                                         r["ctl_loading"], r["ctl_validfrom"], r["div_name"]))


main()
