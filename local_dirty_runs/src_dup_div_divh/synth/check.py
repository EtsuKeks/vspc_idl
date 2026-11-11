#!/usr/bin/env python3
"""Check the expectations of the case catalog against the normalized dumps of a parity run.

    python3 -I synth/check.py <runDir> [--version 30|40|both]

Reads <runDir>/synth/expect.json (written by gen.py) and <runDir>/dump/<30|40>/<load>/<layer>.<table>.tsv
(written by the comparator). Writes <runDir>/expect_check.txt and prints the per-case table.

Matching rule: for each expectation (case, load, table, key) the rows of that key are taken from the dump
(diff tables: only rows with ctl_loading = the load), projected to the columns the expectation lists, and
compared as a multiset. '<notnull>' matches any non-NULL value. 'PASS' means 3.0 (or 4.0) produced exactly
the rows the catalog predicts.
"""
import json
import os
import sys

sys.dont_write_bytecode = True  # no __pycache__ next to the catalog

NUL = "\\N"


def read_tsv(path):
    with open(path, encoding="utf-8") as f:
        lines = f.read().split("\n")
    head = lines[0].split("\t")
    rows = []
    for l in lines[1:]:
        if not l:
            continue
        vals = [v.replace("\\t", "\t").replace("\\n", "\n") for v in l.split("\t")]
        rows.append(dict(zip(head, vals)))
    return head, rows


def norm(v):
    if v is None:
        return NUL
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def match(exp_rows, act_rows):
    """Greedy multiset match; expected rows with fewer wildcards first."""
    left = list(act_rows)
    order = sorted(range(len(exp_rows)), key=lambda i: sum(1 for v in exp_rows[i].values() if v == "<notnull>"))
    missing = []
    for i in order:
        e = exp_rows[i]
        hit = None
        for j, a in enumerate(left):
            if all((a.get(k) != NUL) if v == "<notnull>" else (a.get(k) == norm(v)) for k, v in e.items()):
                hit = j
                break
        if hit is None:
            missing.append(e)
        else:
            left.pop(hit)
    return missing, left


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    run = os.path.abspath(sys.argv[1])
    versions = ["30", "40"]
    if "--version" in sys.argv:
        v = sys.argv[sys.argv.index("--version") + 1]
        versions = ["30", "40"] if v == "both" else [v]
    info = open(os.path.join(run, "run.info")).read()
    chain = info.split("chain=")[1].split()[0]
    spec = json.load(open(os.path.join(run, "synth", "expect.json"), encoding="utf-8"))
    cases = [c for c in spec["cases"] if c["chain"] == chain]
    exps = [e for e in spec["expects"] if e["chain"] == chain]
    cache = {}
    # ctl_loading is dumped as the label of the FIRST load with that loadingId (reruns share the id)
    canon, first = {}, {}
    for l in open(os.path.join(run, "synth", chain, "timeline.tsv"), encoding="utf-8").read().split("\n")[1:]:
        if l.strip():
            lab, lid = l.split("\t")[0], l.split("\t")[3]
            canon[lab] = first.setdefault(lid, lab)

    def table(ver, load, tn):
        k = (ver, load, tn)
        if k not in cache:
            p = os.path.join(run, "dump", ver, load, tn + ".tsv")
            cache[k] = read_tsv(p) if os.path.exists(p) else None
        return cache[k]

    status = {}   # (case, ver) -> [pass, fail, nodata]
    details = []
    for e in exps:
        for ver in versions:
            st = status.setdefault((e["case"], ver), [0, 0, 0])
            t = table(ver, e["load"], e["table"])
            if t is None:
                st[2] += 1
                continue
            head, rows = t
            key = NUL if e["key"] is None else e["key"]
            act = [r for r in rows if r["_key"] == key]
            if e["table"].startswith("diff."):
                act = [r for r in act if r.get("ctl_loading") == canon.get(e["load"], e["load"])]
            cols = list(e["rows"][0].keys()) if e["rows"] else []
            unknown = [c for c in cols if c not in head]
            proj = [{c: r.get(c) for c in cols} for r in act]
            missing, extra = match(e["rows"], proj) if not unknown else (e["rows"], proj)
            if not missing and not extra:
                st[0] += 1
                continue
            st[1] += 1
            d = ["FAIL %s v%s %s %s key=%s%s" % (e["case"], ver, e["load"], e["table"], key,
                                                  (" (unknown columns %s)" % unknown) if unknown else "")]
            if e.get("note"):
                d.append("     note: " + e["note"])
            show = cols or ["start_dt", "end_dt", "ctl_validfrom", "ctl_loading"]
            d.append("     expected: " + ("(no rows)" if not e["rows"] else ""))
            for r in e["rows"]:
                d.append("        " + "  ".join("%s=%s" % (c, norm(r[c])) for c in cols))
            d.append("     actual:   " + ("(no rows)" if not act else ""))
            for r in act:
                d.append("        " + "  ".join("%s=%s" % (c, r.get(c, "?")) for c in show))
            details.append("\n".join(d))

    out = ["Expectation check, chain %s (run %s)" % (chain, run), "",
           "%-5s %-13s %-13s  %s" % ("case", "3.0", "4.0", "title")]
    for c in cases:
        cells = []
        for ver in ["30", "40"]:
            st = status.get((c["id"], ver))
            if ver not in versions or st is None:
                cells.append("-")
            elif st[0] + st[1] == 0:
                cells.append("no data")
            elif st[1] == 0:
                cells.append("PASS %d" % st[0] + ("+%dnd" % st[2] if st[2] else ""))
            else:
                cells.append("FAIL %d/%d" % (st[1], st[0] + st[1]))
        out.append("%-5s %-13s %-13s  %s%s" % (c["id"], cells[0], cells[1], c["title"][:90],
                                                "" if c["deterministic"] else "  [partly non-deterministic in 3.0]"))
    out.append("")
    out.append("PASS n = all n expectations hold; FAIL f/n = f of n do not hold; +k nd = k expectations had no dump (load not run).")
    out.append("")
    out.extend(details)
    text = "\n".join(out) + "\n"
    with open(os.path.join(run, "expect_check.txt"), "w", encoding="utf-8") as f:
        f.write(text)
    print("\n".join(out[:len(cases) + 5]))


if __name__ == "__main__":
    main()
