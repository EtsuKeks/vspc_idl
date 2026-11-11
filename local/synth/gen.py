#!/usr/bin/env python3
"""Generate seeds, timelines, expectations and the two docs from the case catalog (synth/cases/*.py).

    python3 -I synth/gen.py [--out DIR]      seeds etc. into DIR (default target/synth)
    python3 -I synth/gen.py --md             also rewrite synth/CASES.md and synth/CHAINS.md
    python3 -I synth/gen.py --explain <case|chain>   print source rows per load and expected 3.0 rows
    python3 -I synth/gen.py --chains         list the chain names

Outputs in DIR:
    <chain>/<load>.sql      full source state of that load (drop+create+insert of all 8 source tables)
    <chain>/timeline.tsv    label, mode, faketime, loadingId, by (both | 30 = 3.0 only, cutover)
    <chain>/keymap.tsv      natural keys without a case id (<sid>@<table>: only in that table) -> owning case (\\N = NULL key)
    <chain>/cases.tsv       case id, title, known 4.0 behaviour, accepted deviation and its accept area (for the comparator)
    expect.json             expectations (synth/check.py)
    ddl.sql, ddl_schemas.sql   stand databases + 3.0 target DDL / databases only
"""
import json
import os
import sys

sys.dont_write_bytecode = True  # no __pycache__ next to the catalog

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # python -I does not add the script dir

import model  # noqa: E402
from model import CHAINS, READS  # noqa: E402
from cases import CASES, GROUPS  # noqa: E402
import rules_ref  # noqa: E402

TABLES = ["addr", "addr_h", "div_", "div_h", "div_type", "div_div_relation", "terr_div_type", "time_zone_rf"]


def validate():
    model.check_reads()
    ids = [c.id for c in CASES]
    assert len(ids) == len(set(ids)), "duplicate case id"
    for c in CASES:
        labels = [ld.label for ld in CHAINS[c.chain]]
        for r in c.rows:
            for k in list(r.at) + ([r.start] if r.start else []):
                assert k in labels, "%s: load %s is not in chain %s" % (c.id, k, c.chain)
        for e in c.expects:
            assert e.load in labels, "%s: expectation load %s not in chain %s" % (c.id, e.load, c.chain)
            layer, t = e.table.split(".")
            assert layer in ("snp", "diff") and t in TABLES, "%s: bad table %s" % (c.id, e.table)
            cols = [tuple(sorted(r)) for r in e.rows]
            assert len(set(cols)) <= 1, "%s %s %s %s: rows must list the same columns" % (c.id, e.load, e.table, e.key)
        assert not c.accepted or c.accept, "%s: accepted needs accept=[\"<table>:<key>\", ...]" % c.id
        for a in c.accept:
            assert a.split(":")[0] in TABLES + ["chg_div_h"] and ":" in a and " " not in a, "%s: bad accept item %r" % (c.id, a)
        for rid in c.rules:
            assert rid in rules_ref.RULES, "%s: unknown rule id %s" % (c.id, rid)
        for cid in c.changes:
            assert cid in rules_ref.CHANGES, "%s: unknown change id %s" % (c.id, cid)


def write_chain(out, chain):
    os.makedirs(os.path.join(out, chain), exist_ok=True)
    rows = [r for c in CASES if c.chain == chain for r in c.rows]
    for ld in CHAINS[chain]:
        with open(os.path.join(out, chain, ld.label + ".sql"), "w", encoding="utf-8") as f:
            f.write(model.render_load_sql(chain, ld.label, rows))
    with open(os.path.join(out, chain, "timeline.tsv"), "w") as f:
        f.write("label\tmode\tfaketime\tloading_id\tby\n")
        for ld in CHAINS[chain]:
            f.write("%s\t%s\t%s\t%d\t%s\n" % (ld.label, ld.mode, ld.faketime, ld.loading_id, ld.by))
    with open(os.path.join(out, chain, "cases.tsv"), "w", encoding="utf-8") as f:
        f.write("id\ttitle\tknown40\taccepted\taccept\n")
        for c in CASES:
            if c.chain == chain:
                f.write("%s\t%s\t%s\t%s\t%s\n" % (c.id, c.title, c.known40.replace("\t", " ") or " ", c.accepted.replace("\t", " "),
                                                   " ".join(c.accept)))
    with open(os.path.join(out, chain, "keymap.tsv"), "w", encoding="utf-8") as f:
        f.write("sid\tcase\twhy\n")
        for c in CASES:
            if c.chain != chain:
                continue
            for (sid, table), why in c.special_keys.items():
                f.write("%s%s\t%s\t%s\n" % ("\\N" if sid is None else sid, "@" + table if table else "", c.id, why))


def write_expect(out):
    data = []
    for c in CASES:
        for e in c.expects:
            data.append(dict(case=c.id, chain=c.chain, load=e.load, table=e.table, key=e.key, rows=e.rows, note=e.note))
    with open(os.path.join(out, "expect.json"), "w", encoding="utf-8") as f:
        json.dump(dict(cases=[dict(id=c.id, chain=c.chain, title=c.title, deterministic=c.deterministic, known40=c.known40) for c in CASES],
                       expects=data), f, ensure_ascii=False, indent=1)


DIV_DEFAULTS = dict(shortdivcode="01", opendate="2020-01-01 00:00:00",
                    serving_isdisabledequip="0", ctl_loading="1")
SHORT = {"address_locationaddress": "fact_addr", "address_legaladdrdoc": "legal_addr", "address_addrfact_fiascode": "fias",
         "address_addrfact_postcode": "fact_postcode", "address_postcode": "legal_postcode",
         "address_addrfact_addrhouse_housenumbers": "house", "address_addrfact_addrcity_cityname": "city",
         "address_addrfact_timezone": "tz", "address_addrfact_okatocode": "okato", "address_addrfact_oktmocode": "oktmo",
         "requisitesdivision_registrationnum": "regnum", "requisitesdivision_bic": "bic", "serving_isdisabledequip": "disabledequip",
         "headdivisioncode": "parent", "successors_id_successor": "successor_id", "successors_successorstype": "type"}


def _v(v):
    return "NULL" if v is None else "'%s'" % v if isinstance(v, str) else str(v)


def _attrs(st, row):
    skip = {"fulldivcode", "id", "ctl_loading", "ctl_action"}
    out = []
    for k, v in st.items():
        if k in skip and not (k == "id" and v != st.get("fulldivcode")) and not (k == "fulldivcode" and v is None):
            continue
        if row.table == "m_sfs" and (DIV_DEFAULTS.get(k, object()) == v or (k == "fullname" and v == "%s name" % row.label)
                                     or (k == "divisiontype00" and v == "%s_T" % row.case.id)):
            continue
        out.append("%s=%s" % (SHORT.get(k, k), _v(v)))
    return ", ".join(out)


def describe_rows(c):
    lines = []
    for r in c.rows:
        states = r.states(c.chain)
        prev, parts = None, []
        for ld, st in states:
            if st is None and prev is not None:
                parts.append("%s gone" % ld.label)
            elif st is not None and prev is None:
                a = _attrs(st, r)
                parts.append("%s %s%s" % (ld.label, "present" if not parts else "returns", (": " + a) if a else ""))
            elif st is not None and prev is not None and st != prev:
                ch = ["%s %s->%s" % (SHORT.get(k, k), _v(prev.get(k)), _v(st.get(k))) for k in st if st.get(k) != prev.get(k)]
                parts.append("%s %s" % (ld.label, ", ".join(ch)))
            prev = st
        tname = {"m_sfs": "snapshot", "d_sfs": "d_ history", "succ": "successors", "tz": "rfzonetime",
                 "gar": "gar_fias.addr", "okato": "okato2", "oktmo": "oktmo2", "pay": "pay_sys_prtcpnt"}[r.table]
        lines.append("  %-15s %-22s %s" % (tname, r.label, "; ".join(parts) if parts else "never present"))
    return lines


def _cell(v):
    return "NULL" if v is None else str(v)


def describe_expect(e):
    out = ["  %s %s key=%s%s" % (e.load, e.table, _cell(e.key), ("   # " + e.note) if e.note else "")]
    if not e.rows:
        return out + ["      (no rows)"]
    cols = list(e.rows[0].keys())
    w = {k: max(len(k), *(len(_cell(r[k])) for r in e.rows)) for k in cols}
    out.append("      " + "  ".join(k.ljust(w[k]) for k in cols))
    for r in e.rows:
        out.append("      " + "  ".join(_cell(r[k]).ljust(w[k]) for k in cols))
    return out


def explain(what):
    sel = [c for c in CASES if c.id == what] or [c for c in CASES if c.chain == what]
    if not sel:
        sys.exit("no case or chain %s" % what)
    for c in sel:
        print("=" * 100)
        print("%s  [%s, chain %s]  %s" % (c.id, c.group, c.chain, c.title))
        print("  " + c.summary)
        if c.known40:
            print("  4.0: " + c.known40)
        print("  3.0 rules: %s; parity changes: %s" % (", ".join(c.rules) or "-", ", ".join(c.changes) or "-"))
        print("source rows (per load: present / changes / gone):")
        print("\n".join(describe_rows(c)))
        if c.special_keys:
            print("owns keys without the case id: " + ", ".join("%s%s (%s)" % (_cell(k), "@" + t if t else "", w) for (k, t), w in c.special_keys.items()))
        print("expected 3.0 rows (all rows of the key; diff = rows of that load):")
        for e in c.expects:
            print("\n".join(describe_expect(e)))


def _md(v):
    return str(v).replace("|", "\\|").replace("\n", " ")


def write_md():
    L = ["# Synthetic cases", "",
         "Generated by `./stand.sh gen` from `synth/cases/<group>.py` (one function per case). Each case owns its keys "
         "(every natural key contains the case id), so a differing target row is attributed to its case. Column "
         "*expected 3.0* is the reference outcome; the exact rows per load are in the code and in "
         "`./stand.sh explain <case>`. Chains (load sequences, run modes): [CHAINS.md](CHAINS.md).", ""]
    for g in GROUPS:
        cs = [c for c in CASES if c.group == g]
        doc = (sys.modules["cases." + g].__doc__ or "").strip().rstrip(".")
        L += ["## %s - %s (%d)" % (g, doc, len(cs)), "", "| case | tests | chain: loads checked | expected 3.0 |", "|---|---|---|---|"]
        for c in cs:
            loads = sorted({e.load for e in c.expects}, key=lambda s: (s[0], int(s[1:])))
            L.append("| %s | %s | %s: %s | %s%s |" % (c.id, _md(c.title), c.chain, " ".join(loads), _md(c.summary),
                                                      ("" if c.deterministic else " **Partly non-deterministic in 3.0.**")
                                                      + (" **Accepted 4.0 deviation:** %s (accept: `%s`)" % (_md(c.accepted), " ".join(c.accept))
                                                         if c.accepted else "")))
        L.append("")
    L += ["## Not covered (and why)", "",
          "- Duplicate fulldivcode with different attributes in an INCR batch (R16, H13): 3.0's outcome depends on row order "
          "inside a Spark window: by the audit 3.0 writes PK twins / extra diff legs non-deterministically. No expectations; "
          "checked by repeated 3.0 runs (phase 1.4).",
          "- Hash computed in the transformer's types (R03): with the real DDL every transformer column renders the same "
          "string as the target column, so the effect cannot appear.",
          "- Inverted div_type interval on vanish, runs across midnight (D-14, M2), CTL parameters (D-12, D-13, D-20..D-24), "
          "failures and kills between publications (D-02..D-04, F of the audit), CTL/KAP call multisets (D-01, D-18, D-19): "
          "not data scenarios; they need a recording CTL mock / fault injection, which this stand does not have.",
          "- Source type narrowing with overflow (bigint -> int, expected to FAIL in both versions, D-17): a failing load "
          "stops its chain, so it would need a chain of its own; not added to keep the runs short.",
          "- Partial arrival 200 -> 90 keys (D-07): covered by the smaller 150 -> 40 of P01 (same thresholds).",
          "- Performance (K of the audit): data is kept tiny on purpose.", "",
          "Source columns a case may fill (gen.py rejects others) and the 3.0/4.0 code that reads them: `READS` in "
          "`synth/model.py`. Rule / change ids: `synth/rules_ref.py`."]
    with open(os.path.join(HERE, "CASES.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    C = ["# Chains (run scenarios)", "",
         "Generated by `./stand.sh gen` from `CHAINS` in `synth/model.py`. A chain is a sequence of loads run on one "
         "stand root per version; every load = one JVM (sources of the load -> product run at the fake time -> export). "
         "Run: `./stand.sh run <chain> [--only 30|40] [--upto <load>]`, results in `target/runs/<chain>_<time>/summary.txt`.", "",
         "| chain | purpose | run modes covered | loads | cases |", "|---|---|---|---|---|"]
    for name, ch in CHAINS.items():
        cs = [c.id for c in CASES if c.chain == name]
        C.append("| [%s](#%s) | %s | %s | %d | %s |" % (name, name, _md(ch.purpose), _md(ch.modes), len(ch), " ".join(cs)))
    C.append("")
    for name, ch in CHAINS.items():
        C += ["## %s" % name, "", ch.purpose + (" Source type overrides: %s." % ch.types if ch.types else ""), "",
              "| load | mode | fake time | loadingId | D-1 | D-2 | what happens |", "|---|---|---|---|---|---|---|"]
        for ld in ch:
            C.append("| %s | %s%s | %s | %d | %s | %s | %s |" % (ld.label, ld.mode, " (3.0 only)" if ld.by == "30" else "",
                                                              ld.faketime[:16], ld.loading_id, model.d(ld.label, -1),
                                                              model.d(ld.label, -2), _md(ld.why)))
        C.append("")
    with open(os.path.join(HERE, "CHAINS.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(C) + "\n")


V30 = "/Users/timur/Desktop/ess/vspc_idl_old"   # 3.0 (read-only): owner of the target DDL
SCHEMAS = ["usl_vspc_idl_snp", "usl_vspc_idl_diff", "usl_vspc_idl_stg", "usl_vspc_idl_stgtmp",
           "usl_vspc_idl_in_snp", "usl_vspc_idl_in_diff", "usl_gar_fias_idl_snp", "usl_rdm_idl_spark_snp"]


def write_ddl(out):
    """ddl_schemas.sql (stand databases) and ddl.sql (databases + the 16 target tables and stg of 3.0, paths
    rewritten under {{ROOT}}, comments dropped; identical to the 4.0 DDL)."""
    import re
    sch = ""
    for s in SCHEMAS:
        base, layer = s.rsplit("_", 1)
        sch += "create database if not exists %s location '{{ROOT}}/data/usl/%s/%s';\n" % (s, base[len("usl_"):], layer)
    txt = ""
    for fn in ["ddl.usl.vspc_idl.v021.model.9.47.0_fw_3.9.6.sql", "ddl.usl.vspc_idl.v021.model_stg.9.47.0_fw_3.9.6.sql.sql"]:
        txt += open(V30 + "/src/main/resources/sql/ddl/" + fn, encoding="utf-8").read() + "\n"
    txt = txt.replace("{{ logical_env_prefix }}", "").replace("{{ logical_env_prefix_with_slash }}", "")
    txt = re.sub(r"location '(/data/usl/[^']*)'", lambda m: "location '{{ROOT}}%s'" % m.group(1), txt, flags=re.I)
    txt = re.sub(r"COMMENT '(?:[^'\\]|\\.)*'", "", txt)
    with open(os.path.join(out, "ddl_schemas.sql"), "w", encoding="utf-8") as f:
        f.write(sch)
    with open(os.path.join(out, "ddl.sql"), "w", encoding="utf-8") as f:
        f.write(sch + txt)


def main():
    out = os.path.join(HERE, "..", "target", "synth")
    md = False
    args = sys.argv[1:]
    while args:
        a = args.pop(0)
        if a == "--out":
            out = args.pop(0)
        elif a == "--md":
            md = True
        elif a == "--chains":
            return print(" ".join(CHAINS))
        elif a == "--explain":
            validate()
            return explain(args.pop(0))
        else:
            sys.exit(__doc__)
    out = os.path.abspath(out)
    validate()
    os.makedirs(out, exist_ok=True)
    for chain in CHAINS:
        write_chain(out, chain)
    write_expect(out)
    write_ddl(out)
    if md:
        write_md()
    print("[gen] %d cases, chains %s -> %s" % (len(CASES), ", ".join("%s:%d" % (k, len(v)) for k, v in CHAINS.items()), out))


if __name__ == "__main__":
    main()
