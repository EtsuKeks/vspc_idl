"""Audit 4.3 (dirty paths), theme migrate: the scenarios of the dirty stand `local_dirty` (chain dsrc) and of its forks
`local_dirty_runs/` (src_dup_div_divh, src_dup_divtype, same_day_hist_layout, dtgt_faithful_cutover, essa_retry_rerun,
rtfw_stg_leftovers_cutover) moved into the main catalog, with 3.0 expectations. Target-side dirt is NOT seeded by SQL
(the main stand has no Case.sql): 3.0 writes it itself from duplicated source rows on loads marked "30", and 4.0
continues from the 3.0 root (chain mcut, like `cutover`).

Not moved (need a stand-engine change, go to IFT): SQL seeds into snp/diff/stg (dtgt B11 B21 B22 B31 B41 F41 G11 G12,
dcut B14 B21L B22L, dg1 pre-step PRESQL_DIR), emptied loader cache, failure injection (-Dstand.fail, essa_retry_rerun
F1/F2a), concurrent INIT+INCR (concurrent_init_incr, not finished).

3.0 is NOT deterministic with two source rows per fulldivcode (R16, DV-77/DV-78): the order of the two NEW rows in the
window decides between "twins + lone (R,U)" and "one row + (R,U)+(U,U)". Such cases have deterministic=False and
expectations only for projections that every order gives (INIT, tables where the copies collapse under distinct,
loads after the source is clean). The outcome sets are listed in the summaries.

Paths: O = vspc_idl_old/src/main/java/ru/sberbank/bigdata/dffess/vspc/, E = essa-common-lib .../essa/common/ (SCHEMA.md).

NEW CHAINS (mday, mcut, mval, mretry; loadingId 20300-20399, labels M10-M13/M20-M24/M30-M33/M40-M43) are defined in
model.CHAINS (merged there by the coordinator of phase 4.4); NEW_CHAINS below is a view of them for _d().
"""
import datetime as _dt

from model import (Case, Chain, Load, _days, GONE, MIN, MAX, NOTNULL, START, RUN, ver, leg, hub, hleg, addr_key)  # noqa: F401
from cases import case, DV77

# The chains live in model.CHAINS (merged by the phase 4.4 coordinator); NEW_CHAINS is kept as a view for _d().
from model import CHAINS as _CHAINS  # noqa: E402
NEW_CHAINS = {k: _CHAINS[k] for k in ("mday", "mcut", "mval", "mretry")}


def _d(label, delta=0):
    """Business date of a load of NEW_CHAINS plus delta days (model.d needs the chain in model.CHAINS)."""
    for ch in NEW_CHAINS.values():
        for ld in ch:
            if ld.label == label:
                return (ld.date + _dt.timedelta(days=delta)).isoformat()
    raise KeyError(label)


_HUB = ("3.0 hub window: E/slave/loading/SnpDataLoader.java:181-205 (partition by PK, desc(ctl_loading), asc(record_status); "
        "a NEW row is compared with lead, an OLD row with lag), E/slave/loading/DataLoaderHubDiff.java:75-80 (kept: NEW, "
        "NEQ rn1, EQ rn2, OLD), :121-144 (legs)")
_HM = ("3.0 div_h: O/impl/loader/DataLoaderHistMix.java:66-69 (999912 merged; current and previous month returned as is), "
       ":75-80 (kept rows, NEW -> 1900), :84-88 (closure at vf(neighbour)-2, dropped when end < start), :94")
_HS = ("3.0 div_type: E/slave/loading/DataLoaderHistSnp.java:44-57 (input start D-1), :82-96 (kept rows; NEW -> 1900 or cached "
       "end + 1), :103-108 (OLD closes at current_date-1, NEQ rn2 at vf-2), :116 (cache), :119 (end < start dropped), :139-153 (legs)")
_DST = ("distinct in the 3.0 transformers: O/tables/div/DivTransformer.java:62,88; O/tables/divh/DivHTransformer.java:54,78; "
        "O/tables/divtype/DivTypeTransformer.java:42")
_PAIR = ("Two NEW rows of one key tie on (ctl_loading, record_status) (R16): order [x, y, old]: x NEQ rn1 kept with a (R,U)/(R,I) "
         "leg; y is compared with the old row (lead): equal -> EQ rn2 kept, no leg (twins with one PK + a lone R leg); different "
         "-> NEQ rn2 dropped or closed with (U,U); the old row is rn3 and always leaves without a leg.")


def _pair(c, k, a, b, at_b=None, start_b=None, flip=True):
    """Payload a and b for one key; with flip, on even k the b row gets the lower label (physical order flipped)."""
    odd = int(k.split("_")[1]) % 2
    la, lb = (k + "#a", k + "#b") if odd or not flip else (k + "#b", k + "#a")
    c.div(k, label=la, **a)
    c.div(k, label=lb, at=at_b, start=start_b, **b)


def _stable_divtype(c, key, loads, init):
    """div_type of a key whose copies share divisiontype00: one row [1900, MAX] from the INIT, no legs (distinct)."""
    for ld in loads:
        c.expect(ld, "snp.div_type", key, [ver(MIN, MAX, RUN(init), init)])
        if ld != init:
            c.expect(ld, "diff.div_type", key, [])


# =========================================================================================================== chain mday
MA = ["M10", "M11", "M12", "M13"]


def _mday_init(c, key, name=None):
    c.expect("M10", "snp.div_", key, [hub(RUN("M10"), "M10")])
    attrs = dict(div_name=name) if name else {}
    c.expect("M10", "snp.div_h", key, [ver(_d("M10"), MAX, START, "M10", **attrs)],
             note=None if name else "one version per fulldivcode (INIT chain, datediff >= 0); which copy's name is DV-78")


def u01():
    c = case(id="U01", accepted=DV77, accept=["div_:U01_1", "div_:U01_2", "div_:U01_3", "div_:U01_4"], chain="mday", title="Fork src_dup_div_divh SD1: two m_ rows per fulldivcode differing only in shortdivcode (div_)",
             rules=["H13-key-dedup", "R16", "H06-record-status", "H07-snp-incr", "H08-diff-legs-incr"], changes=[],
             tables=["div_", "div_h", "div_type"], deterministic=False,
             known40="DV-77 (window30 tie rule) and DV-78 (INIT tie) decide parity; outcome must be one of the 3.0 set",
             summary="U01_1..4: copies '01'/'02', same name (physical order flipped on even keys). INIT: one div_ row "
                     "(which short code is DV-78). Every INCR on the same day: div_ = 1 or 2 rows (twins with one PK) with a lone "
                     "(R,U) or (R,U)+(U,U) - not checked. " + _PAIR + " " + _HUB + ". div_h and div_type: the copies collapse "
                     "(" + _DST + "), EQUALS, no legs on any INCR - checked. DEV: B1 (12 div_ twins), B4 (R-only legs).")
    for i in range(1, 5):
        k = "U01_%d" % i
        _pair(c, k, dict(shortdivcode="01", fullname="U01"), dict(shortdivcode="02", fullname="U01"))
        _mday_init(c, k, "U01")
        for ld in MA[1:]:
            c.expect(ld, "snp.div_h", k, [ver(_d("M10"), MAX, START, "M10", div_name="U01")])
            c.expect(ld, "diff.div_h", k, [])
        _stable_divtype(c, k, MA, "M10")
        c.note("M11", "%s div_: 1 or 2 rows, legs (R,U) or (R,U)+(U,U) (R16)" % k)


def u02():
    c = case(id="U02", accepted=DV77 + "; ничья INIT stg.chg_div_h (DV-78)", accept=["div_h:U02_1", "div_h:U02_2", "div_h:U02_3", "div_h:U02_4", "chg_div_h:U02_4"], chain="mday", title="Fork src_dup_div_divh SD2: two m_ rows per fulldivcode differing only in fullname (div_h)",
             rules=["H13-key-dedup", "R16", "R07", "R15", "L9-histmix-incr"], changes=[], tables=["div_", "div_h", "div_type"],
             deterministic=False,
             summary="U02_1..4: names 'U02 A'/'U02 B'. INIT: one div_h version [INIT, MAX] (which name: DV-78). M11 (INIT day): "
                     "the INIT version is always rn3 and leaves without a leg; 1 or 2 open versions [D-1, MAX] with one PK "
                     "(twins, DEV B2 with one ctl_validfrom) - not checked. " + _HM + ". div_ and div_type: copies collapse "
                     "(" + _DST + "), no legs on any INCR - checked. DEV: B2, B4 (exact duplicate legs by (sk, start, end, "
                     "ctl_loading, ctl_do)).")
    for i in range(1, 5):
        k = "U02_%d" % i
        _pair(c, k, dict(fullname="U02 A"), dict(fullname="U02 B"))
        _mday_init(c, k)
        for ld in MA[1:]:
            c.expect(ld, "snp.div_", k, [hub(RUN("M10"), "M10")])
            c.expect(ld, "diff.div_", k, [])
        _stable_divtype(c, k, MA, "M10")


def u03():
    c = case(id="U03", accepted=DV77, accept=["div_:U03_1", "div_:U03_2", "div_:U03_3", "div_:U03_4", "div_h:U03_1", "div_h:U03_2", "div_h:U03_3", "div_h:U03_4"], chain="mday", title="Fork src_dup_div_divh SD3: two m_ rows per fulldivcode differing in shortdivcode and fullname",
             rules=["H13-key-dedup", "R16"], changes=[], tables=["div_", "div_h", "div_type"], deterministic=False,
             summary="U03_1..4: ('01', 'U03 A') / ('02', 'U03 B'). INIT checked (one div_ row, one div_h version); div_ and "
                     "div_h INCR outcomes as U01/U02 (R16, not checked); div_type collapses (" + _DST + ") - checked.")
    for i in range(1, 5):
        k = "U03_%d" % i
        _pair(c, k, dict(shortdivcode="01", fullname="U03 A"), dict(shortdivcode="02", fullname="U03 B"))
        _mday_init(c, k)
        _stable_divtype(c, k, MA, "M10")


def u04():
    c = case(id="U04", accepted=DV77, accept=["div_:U04_1", "div_:U04_3", "div_:U04_4", "div_h:U04_1", "div_h:U04_2", "div_h:U04_3", "div_h:U04_4"], chain="mday", title="Fork src_dup_div_divh SD4: clean at INIT, copy B appears at M11",
             rules=["H13-key-dedup", "R16", "H09-init"], changes=[], tables=["div_", "div_h", "div_type"], deterministic=False,
             summary="U04_1..4: INIT has only ('01', 'U04 A') - fully deterministic; from M11 the copy ('02', 'U04 B') is "
                     "there: outcomes as U03 (R16, not checked); div_type collapses - checked.")
    for i in range(1, 5):
        k = "U04_%d" % i
        _pair(c, k, dict(shortdivcode="01", fullname="U04 A"), dict(shortdivcode="02", fullname="U04 B"), start_b="M11")
        c.expect("M10", "snp.div_", k, [hub(RUN("M10"), "M10", short_div_code="01")])
        c.expect("M10", "snp.div_h", k, [ver(_d("M10"), MAX, START, "M10", div_name="U04 A")])
        _stable_divtype(c, k, MA, "M10")


def u05():
    c = case(id="U05", accepted=DV77, accept=["div_:U05_1", "div_:U05_2", "div_:U05_4", "div_h:U05_1", "div_h:U05_2", "div_h:U05_3", "div_h:U05_4"], chain="mday", title="Fork src_dup_div_divh SD7: copy B gone at M12 - 3.0 collapses the twins in one INCR",
             rules=["H13-key-dedup", "R16", "R15", "H07-snp-incr"], changes=[], tables=["div_", "div_h", "div_type"],
             deterministic=False,
             known40="DV-79/DV-03: 4.0 must also collapse the 3.0 twins once the source is clean (key with input)",
             summary="U05_1..4: ('01', 'U05 A') / ('02', 'U05 B') at M10-M11, B gone from M12. M12: input {A}; snp holds 1-2 rows "
                     "(A and/or B) with ctl_loading M11. Window [A_new, old_p, old_q]: old_p = A -> A_new EQ rn1 dropped, A_old "
                     "EQ rn2 kept, rest rn3 dropped silently; old_p = B -> A_new NEQ rn1 kept, B NEQ rn2 closed at vf-2 = "
                     "2027-01-11 < start 2027-01-12 -> dropped (O/impl/loader/DataLoaderHistMix.java:84-88). Every order: ONE "
                     "div_ row '01' and ONE div_h version [2027-01-12, MAX] 'U05 A' (ctl_* vary: not checked). M13: EQUALS, no "
                     "legs. " + _HUB + ".")
    for i in range(1, 5):
        k = "U05_%d" % i
        _pair(c, k, dict(shortdivcode="01", fullname="U05 A"), dict(shortdivcode="02", fullname="U05 B"), at_b={"M12": GONE})
        _mday_init(c, k)
        for ld in ("M12", "M13"):
            c.expect(ld, "snp.div_", k, [dict(short_div_code="01")])
            c.expect(ld, "snp.div_h", k, [dict(start_dt=_d("M11", -1), end_dt=MAX, div_name="U05 A")])
        c.expect("M13", "diff.div_", k, [])
        c.expect("M13", "diff.div_h", k, [])
        _stable_divtype(c, k, MA, "M10")


def u06():
    c = case(id="U06", accepted=DV77, accept=["div_:U06_1", "div_:U06_2"], chain="mday", title="Fork src_dup_div_divh SD5: three m_ rows per fulldivcode (short codes 01/02/03)",
             rules=["H13-key-dedup", "R16"], changes=[], tables=["div_", "div_h", "div_type"], deterministic=False,
             summary="U06_1..2: three copies, same name. div_: up to 2 kept rows per INCR (NEQ rn1 + EQ rn2), rn3/rn4 drop "
                     "(R16, not checked). div_h and div_type collapse (" + _DST + ") - checked on every load.")
    for i in (1, 2):
        k = "U06_%d" % i
        for j, sc in enumerate(("01", "02", "03")):
            c.div(k, label="%s#%d" % (k, j), shortdivcode=sc, fullname="U06")
        _mday_init(c, k, "U06")
        for ld in MA[1:]:
            c.expect(ld, "snp.div_h", k, [ver(_d("M10"), MAX, START, "M10", div_name="U06")])
            c.expect(ld, "diff.div_h", k, [])
        _stable_divtype(c, k, MA, "M10")


def u07():
    c = case(id="U07", accepted=DV77, accept=["div_type:U07_1", "div_type:U07_2", "div_type:U07_3", "div_type:U07_4", "div_type:U07_9", "div_type:U07_11"], chain="mday", title="Fork src_dup_divtype TY: two divisiontype00 per fulldivcode (div_type pair from 3.0 itself)",
             rules=["H13-key-dedup", "R16", "R12", "R13"], changes=[], tables=["div_type", "div_", "div_h"], deterministic=False,
             known40="DV-01 decision: twins as in 3.0; DV-80 when the pair later closes below the rewrite zone",
             summary="U07_1..4: types U07_TA/U07_TB (order flipped on even keys), all else equal. INIT: TWO div_type rows "
                     "[1900, MAX] with one PK (R12: INIT writes the input as is). INCR: 1 or 2 rows, [1900, ..] lost or "
                     "closed (R16, not checked). U07_9: TA only at INIT, TB from M11. U07_10: pair at INIT, TB gone at M11. "
                     "U07_11: new key at M11 with both types: no old row, so NEW-NEW: rn1 NEQ kept with start D-1, rn2 stays "
                     "NEW (lead is null) -> start 1900; both (R,I)+(U,D); which type gets which start is the tie - only dates "
                     "checked. " + _HS + ". div_ and div_h: copies collapse, no legs - checked.")
    for i in range(1, 5):
        k = "U07_%d" % i
        _pair(c, k, dict(divisiontype00="U07_TA"), dict(divisiontype00="U07_TB"))
        c.expect("M10", "snp.div_type", k, [ver(MIN, MAX, RUN("M10"), "M10", div_type_terr_div_type_sk="U07_TA"),
                                           ver(MIN, MAX, RUN("M10"), "M10", div_type_terr_div_type_sk="U07_TB")])
        _mday_init(c, k, "%s name" % k)
        for ld in MA[1:]:
            c.expect(ld, "snp.div_", k, [hub(RUN("M10"), "M10")])
            c.expect(ld, "diff.div_", k, [])
            c.expect(ld, "snp.div_h", k, [ver(_d("M10"), MAX, START, "M10")])
            c.expect(ld, "diff.div_h", k, [])
    c.div("U07_9", label="U07_9#a", divisiontype00="U07_TA")
    c.div("U07_9", label="U07_9#b", divisiontype00="U07_TB", start="M11")
    c.expect("M10", "snp.div_type", "U07_9", [ver(MIN, MAX, RUN("M10"), "M10", div_type_terr_div_type_sk="U07_TA")])
    c.div("U07_10", label="U07_10#a", divisiontype00="U07_TA")
    c.div("U07_10", label="U07_10#b", divisiontype00="U07_TB", at={"M11": GONE})
    c.expect("M10", "snp.div_type", "U07_10", [ver(MIN, MAX, RUN("M10"), "M10", div_type_terr_div_type_sk="U07_TA"),
                                              ver(MIN, MAX, RUN("M10"), "M10", div_type_terr_div_type_sk="U07_TB")])
    c.note("M11", "U07_10: {TA [1900, MAX] M10} without legs, or {TA [D-1, MAX], TB [1900, 2027-01-11]} with 4 legs (R16)")
    c.div("U07_11", label="U07_11#a", divisiontype00="U07_TA", start="M11")
    c.div("U07_11", label="U07_11#b", divisiontype00="U07_TB", start="M11")
    c.expect("M11", "snp.div_type", "U07_11", [dict(start_dt=MIN, end_dt=MAX), dict(start_dt=_d("M11", -1), end_dt=MAX)])
    c.expect("M11", "diff.div_type", "U07_11", [dict(ctl_do=do, ctl_action=a, start_dt=s, end_dt=MAX)
                                               for s in (MIN, _d("M11", -1)) for do, a in (("R", "I"), ("U", "D"))])
    c.expect("M11", "snp.div_", "U07_11", [hub(RUN("M11"), "M11")])
    c.expect("M11", "snp.div_h", "U07_11", [ver(MIN, MAX, RUN("M11"), "M11", p1month="999912")],
             note="brand-new key: 1900 (C11 mechanism); the two copies collapse")


_LEGS = ["2023-06-20 10:00:00", "2023-10-15 10:00:00", "2023-12-10 10:00:00", "2024-01-22 10:00:00"]


def u08():
    c = case(id="U08", chain="mday", title="Fork same_day_hist_layout: d_ history since 2023, INIT + 2 INCRs on the INIT day, INCR on the 1st",
             rules=["R10", "R05", "R07", "R15", "S2-chgdivh-init", "L8-histmix-init", "L9-histmix-incr", "M1-hist-mover-truncation",
                    "M2-loader-mover-window-contract"], changes=["HPWR-close-open-only", "HPWR-silent-drop"], tables=["div_h"],
             summary="DEV timeline (audit sec. 1, B3, F4 b). d_ legs 2023-06-20 / 2023-10-15 / 2023-12-10 / 2024-01-22: the INIT "
                     "spreads closed versions over p1month 202310, 202312, 202401 (+202701 when m_ differs from the last leg; "
                     "O/tables/chgdivh/ChgDivHSql.java:177 end = next start - 1). M11/M12 on the INIT day: the open INIT "
                     "version [2027-01-13, MAX] cannot close at D-2 = 2027-01-11 and is dropped without legs, the new one "
                     "opens [D-1 = 2027-01-12, MAX] - it OVERLAPS the INIT-closed last leg [2024-01-22, 2027-01-12] by one day "
                     "(DEV B3: 6477 such pairs). U08_2 (m_ = last leg, open since 2024) closes normally at 2027-01-11 into "
                     "202701. M13 (02-01): U08_4 closes [2027-01-13, 2027-01-30] into 202701; 202701 is in the rewrite zone "
                     "(returned as is), 2023/2024 partitions are below the mover cut and stay. " + _HM + "; "
                     "O/impl/mover/HistMovingNoCacheGenerator.java:34-35 (truncation from yyyyMM(now - 1 month)).")
    chg = {1: ["M11"], 2: ["M11"], 3: ["M12"], 4: ["M13"], 5: ["M11", "M12"]}
    for i in range(1, 7):
        k = "U08_%d" % i
        same = i in (2, 6)
        now = "%s v3" % k if same else "%s now" % k
        r = c.div(k, fullname=now, at={ld: dict(fullname="%s %s" % (k, ld)) for ld in chg.get(i, [])})
        for j, vf in enumerate(_LEGS):
            c.hist(r, vf, j + 1, fullname="%s v%d" % (k, j))
        old = [ver("2023-06-20", "2023-10-14", START, "M10", div_name="%s v0" % k, p1month="202310"),
               ver("2023-10-15", "2023-12-09", START, "M10", div_name="%s v1" % k, p1month="202312"),
               ver("2023-12-10", "2024-01-21", START, "M10", div_name="%s v2" % k, p1month="202401")]
        v3c = ver("2024-01-22", "2027-01-12", START, "M10", div_name="%s v3" % k, p1month="202701")
        v3o = ver("2024-01-22", MAX, START, "M10", div_name="%s v3" % k, p1month="999912")
        nowv = ver(_d("M10"), MAX, START, "M10", div_name="%s now" % k, p1month="999912")
        init = old + ([v3o] if same else [v3c, nowv])
        c.expect("M10", "snp.div_h", k, init)

        def opened(ld, start=None):
            return ver(start or _d(ld, -1), MAX, RUN(ld), ld, div_name="%s %s" % (k, ld), p1month="999912")

        def ri(ld, start=None):
            s = start or _d(ld, -1)
            return [leg("R", "I", s, MAX, RUN(ld), div_name="%s %s" % (k, ld)), leg("U", "D", s, MAX, RUN(ld), div_name="%s %s" % (k, ld))]

        if i in (1, 5):
            st1 = old + [v3c, opened("M11")]
            c.expect("M11", "snp.div_h", k, st1, note="INIT version dropped (end 2027-01-11 < start); one-day overlap with v3")
            c.expect("M11", "diff.div_h", k, ri("M11"))
            if i == 1:
                for ld in ("M12", "M13"):
                    c.expect(ld, "snp.div_h", k, st1)
                    c.expect(ld, "diff.div_h", k, [])
            else:
                st2 = old + [v3c, opened("M12")]
                c.expect("M12", "snp.div_h", k, st2, note="second change the same day: M11 version dropped (R15)")
                c.expect("M12", "diff.div_h", k, ri("M12"))
                c.expect("M13", "snp.div_h", k, st2)
        elif i == 2:
            closed = ver("2024-01-22", _d("M11", -2), RUN("M11"), "M11", div_name="%s v3" % k, p1month="202701")
            st1 = old + [closed, opened("M11")]
            c.expect("M11", "snp.div_h", k, st1)
            c.expect("M11", "diff.div_h", k, ri("M11") + [leg("R", "U", "2024-01-22", _d("M11", -2), RUN("M11"), div_name="%s v3" % k),
                                                         leg("U", "U", "2024-01-22", MAX, RUN("M11"), div_name="%s v3" % k)])
            c.expect("M13", "snp.div_h", k, st1, note="202701 row returned as is by the rewrite zone of 02-01")
        elif i == 3:
            c.expect("M11", "snp.div_h", k, init)
            c.expect("M11", "diff.div_h", k, [])
            c.expect("M12", "snp.div_h", k, old + [v3c, opened("M12")])
            c.expect("M12", "diff.div_h", k, ri("M12"))
        elif i == 4:
            for ld in ("M11", "M12"):
                c.expect(ld, "snp.div_h", k, init)
            closed = ver(_d("M10"), _d("M13", -2), RUN("M13"), "M13", div_name="%s now" % k, p1month="202701")
            c.expect("M13", "snp.div_h", k, old + [v3c, closed, opened("M13")])
            c.expect("M13", "diff.div_h", k, ri("M13") + [leg("R", "U", _d("M10"), _d("M13", -2), RUN("M13"), div_name="%s now" % k),
                                                         leg("U", "U", _d("M10"), MAX, RUN("M13"), div_name="%s now" % k)])
        else:   # 6: control, never changes
            for ld in MA[1:]:
                c.expect(ld, "snp.div_h", k, init)
                c.expect(ld, "diff.div_h", k, [])
    c.div("U08_N", fullname="U08_N now", at={"M11": dict(fullname="U08_N M11")})
    c.expect("M10", "snp.div_h", "U08_N", [ver(_d("M10"), MAX, START, "M10", div_name="U08_N now", p1month="999912")])
    c.expect("M11", "snp.div_h", "U08_N", [ver(_d("M11", -1), MAX, RUN("M11"), "M11", div_name="U08_N M11", p1month="999912")],
             note="no d_ history: the INIT version is dropped, the key starts the day before its INIT")
    c.expect("M11", "diff.div_h", "U08_N", [leg("R", "I", _d("M11", -1), MAX, RUN("M11"), div_name="U08_N M11"),
                                           leg("U", "D", _d("M11", -1), MAX, RUN("M11"), div_name="U08_N M11")])


# =========================================================================================================== chain mcut
MC = ["M20", "M21", "M22", "M23", "M24"]
_K = range(1, 5)


def _mcut_init(c, key, name=None):
    c.expect("M20", "snp.div_", key, [hub(RUN("M20"), "M20")])
    attrs = dict(div_name=name) if name else {}
    c.expect("M20", "snp.div_h", key, [ver(_d("M20"), MAX, START, "M20", **attrs)])


_CUT = ("M20-M22 run on 3.0 only (like DEV 2026-10-16), the 4.0 half starts at M23 from a copy of the 3.0 root, so 4.0 "
        "inherits 3.0's own twins: div_ rows with one PK, two open div_h versions [2027-01-12, MAX] with one PK and one "
        "ctl_validfrom, R-only and 3-leg combos in diff.div_ (DEV B1, B2, B4).")


def u10():
    c = case(id="U10", accepted=DV77, accept=["div_:U10_1", "div_:U10_2", "div_:U10_3", "div_:U10_4", "div_h:U10_1", "div_h:U10_2", "div_h:U10_3", "div_h:U10_4"], chain="mcut", title="Fork dtgt_faithful_cutover CT1: source duplicate (short code + name) kept through M24",
             rules=["H13-key-dedup", "R16", "H07-snp-incr"], changes=[], tables=["div_", "div_h", "div_type"], deterministic=False,
             known40="DV-77: 4.0 resolves the tie by POS/IPOS; its outcome must be in the 3.0 set",
             summary=_CUT + " U10_1..4: #a ('01', '<k> name'), #b ('02', '<k> other') on every load. INIT checked; div_/div_h "
                     "every INCR: 1-2 rows (R16, " + _HUB + "; " + _HM + "), not checked. div_type: copies collapse "
                     "(" + _DST + "), [1900, MAX] from M20, no legs - checked on every load.")
    for i in _K:
        k = "U10_%d" % i
        _pair(c, k, {}, dict(shortdivcode="02", fullname="%s other" % k), flip=False)
        _mcut_init(c, k)
        _stable_divtype(c, k, MC, "M20")


def u11():
    c = case(id="U11", chain="mcut", title="Fork dtgt_faithful_cutover CT2: source duplicate, #b GONE at the cutover INCR M23",
             rules=["H13-key-dedup", "R16", "H07-snp-incr", "R07"], changes=[], tables=["div_", "div_h", "div_type"],
             deterministic=False,
             known40="DV-79/DV-03: 4.0 has the key in the input, so its window must collapse the inherited twins as 3.0 does",
             summary=_CUT + " U11_1..4: like U10, #b gone at M23 (source clean after cutover). M23 div_: snp 1-2 rows ('01' "
                     "and/or '02'), input {'01'}: [new01, old01, ..] -> EQ rn2 kept; [new01, old02, ..] -> NEQ rn1 kept, old02 "
                     "(U,U); rn3 drops silently - every order leaves ONE row '01' (" + _HUB + "). div_h M23: one open version "
                     "'<k> name', plus a closed [2027-01-12, 2027-01-12] 'other' when the other copy was rn2 (not checked). "
                     "M24: fixed point - no div_ / div_h legs, one div_ row '01'.")
    for i in _K:
        k = "U11_%d" % i
        _pair(c, k, {}, dict(shortdivcode="02", fullname="%s other" % k), at_b={"M23": GONE}, flip=False)
        _mcut_init(c, k)
        _stable_divtype(c, k, MC, "M20")
        for ld in ("M23", "M24"):
            c.expect(ld, "snp.div_", k, [dict(short_div_code="01")])
        c.expect("M24", "diff.div_", k, [])
        c.expect("M24", "diff.div_h", k, [])


def u12():
    c = case(id="U12", accepted=DV77, accept=["div_type:U12_1", "div_type:U12_2", "div_type:U12_3", "div_type:U12_4"], chain="mcut", title="Fork dtgt_faithful_cutover CT3: two divisiontype00 per key kept (div_type twins from 3.0)",
             rules=["H13-key-dedup", "R16", "R12", "R13"], changes=[], tables=["div_type", "div_", "div_h"], deterministic=False,
             known40="DV-80: once the pair is older than the rewrite zone, 4.0's closure falls back to the vendor (N x N)",
             summary=_CUT + " U12_1..4: types U12_TA/U12_TB, all else equal. M20: TWO div_type rows [1900, MAX] (R12). Every "
                     "INCR: one or two rows; observed 3.0 (fork run 11-10): one row [D-1, MAX] with (R,I)+(U,D), the 1900 row "
                     "dropped silently as rn3 (" + _HS + ") - not checked (R16). div_ and div_h collapse (" + _DST + "): "
                     "unchanged from M20, no legs - checked on every load.")
    for i in _K:
        k = "U12_%d" % i
        _pair(c, k, dict(divisiontype00="U12_TA"), dict(divisiontype00="U12_TB"), flip=False)
        c.expect("M20", "snp.div_type", k, [ver(MIN, MAX, RUN("M20"), "M20", div_type_terr_div_type_sk="U12_TA"),
                                           ver(MIN, MAX, RUN("M20"), "M20", div_type_terr_div_type_sk="U12_TB")])
        _mcut_init(c, k, "%s name" % k)
        for ld in MC[1:]:
            c.expect(ld, "snp.div_", k, [hub(RUN("M20"), "M20")])
            c.expect(ld, "diff.div_", k, [])
            c.expect(ld, "snp.div_h", k, [ver(_d("M20"), MAX, START, "M20", div_name="%s name" % k)])
            c.expect(ld, "diff.div_h", k, [])


def u13():
    c = case(id="U13", chain="mcut", title="Fork dtgt_faithful_cutover CT4: two divisiontype00, #b GONE at the cutover INCR M23",
             rules=["H13-key-dedup", "R16", "R13"], changes=[], tables=["div_type", "div_", "div_h"], deterministic=False,
             summary=_CUT + " U13_1..4: types U13_TA/U13_TB, TB gone at M23. M23 input {TA}: rn2 TA -> EQ kept, other rows "
                     "rn3 dropped; rn2 TB -> new TA [2027-01-13, MAX] + TB closed at 2027-01-12 (" + _HS + "): exactly one "
                     "open TA either way (not checked as a whole). M24: EQUALS, no div_type legs - checked. div_ and div_h "
                     "collapse - checked.")
    for i in _K:
        k = "U13_%d" % i
        _pair(c, k, dict(divisiontype00="U13_TA"), dict(divisiontype00="U13_TB"), at_b={"M23": GONE}, flip=False)
        _mcut_init(c, k, "%s name" % k)
        c.expect("M24", "diff.div_type", k, [])
        for ld in MC[1:]:
            c.expect(ld, "snp.div_h", k, [ver(_d("M20"), MAX, START, "M20", div_name="%s name" % k)])
            c.expect(ld, "diff.div_h", k, [])
            c.expect(ld, "diff.div_", k, [])


def u14():
    c = case(id="U14", accepted=DV77, accept=["div_:U14_1", "div_:U14_2", "div_:U14_3", "div_:U14_4", "div_h:U14_1", "div_h:U14_2", "div_h:U14_3", "div_h:U14_4"], chain="mcut", title="Fork dtgt_faithful_cutover CT5: source duplicate, #a changes its payload at M23",
             rules=["H13-key-dedup", "R16"], changes=[], tables=["div_", "div_h", "div_type"], deterministic=False,
             known40="DV-77: same input on M23 and M24 gave different 3.0 branches in the fork run (CT5_1)",
             summary=_CUT + " U14_1..4: #a ('01', name) changes to ('05', '<k> new') at M23, #b ('02', '<k> other') stays: a "
                     "changed key over inherited twins. INIT and div_type (no legs on any load) checked; div_/div_h R16.")
    for i in _K:
        k = "U14_%d" % i
        c.div(k, label=k + "#a", at={"M23": dict(shortdivcode="05", fullname="%s new" % k)})
        c.div(k, label=k + "#b", shortdivcode="02", fullname="%s other" % k)
        _mcut_init(c, k)
        _stable_divtype(c, k, MC, "M20")


def u15():
    c = case(id="U15", chain="mcut", title="Fork rtfw_stg_leftovers_cutover U11 (without leftovers): name changes on every load across the cutover",
             rules=["R07", "R11", "R15", "L9-histmix-incr", "M2-loader-mover-window-contract"], changes=["HPWR-close-open-only", "HPWR-silent-drop"],
             tables=["div_h"],
             summary="Clean key. M21 and M22 (3.0, INIT day): the open version cannot close at D-2 = 2027-01-11 and is dropped; "
                     "the new one opens [2027-01-12, MAX] (R15). M23 (cutover, 4.0 over the 3.0 root): the M22 version closes "
                     "normally [2027-01-12, 2027-01-12] in 202701, the new opens [2027-01-13, MAX]. M24 (02-01): close "
                     "[2027-01-13, 2027-01-30] into 202701, open [2027-01-31, MAX]; the M23 row in 202701 is returned as is "
                     "(rewrite zone 202702+202701). " + _HM + ". The RT-FW staging leftovers of the fork (pre-step) are IFT.")
    k = "U15_1"
    c.div(k, fullname="U15 v0", at={"M21": dict(fullname="U15 v1"), "M22": dict(fullname="U15 v2"),
                                    "M23": dict(fullname="U15 v3"), "M24": dict(fullname="U15 v4")})
    c.expect("M20", "snp.div_h", k, [ver(_d("M20"), MAX, START, "M20", div_name="U15 v0", p1month="999912")])
    for ld, n in (("M21", "v1"), ("M22", "v2")):
        c.expect(ld, "snp.div_h", k, [ver(_d(ld, -1), MAX, RUN(ld), ld, div_name="U15 " + n, p1month="999912")])
        c.expect(ld, "diff.div_h", k, [leg("R", "I", _d(ld, -1), MAX, RUN(ld), div_name="U15 " + n),
                                      leg("U", "D", _d(ld, -1), MAX, RUN(ld), div_name="U15 " + n)])
    c2 = ver(_d("M22", -1), _d("M23", -2), RUN("M23"), "M23", div_name="U15 v2", p1month="202701")
    c.expect("M23", "snp.div_h", k, [c2, ver(_d("M23", -1), MAX, RUN("M23"), "M23", div_name="U15 v3", p1month="999912")])
    c.expect("M23", "diff.div_h", k, [leg("R", "I", _d("M23", -1), MAX, RUN("M23"), div_name="U15 v3"),
                                     leg("U", "D", _d("M23", -1), MAX, RUN("M23"), div_name="U15 v3"),
                                     leg("R", "U", _d("M22", -1), _d("M23", -2), RUN("M23"), div_name="U15 v2"),
                                     leg("U", "U", _d("M22", -1), MAX, RUN("M23"), div_name="U15 v2")])
    c.expect("M24", "snp.div_h", k, [c2, ver(_d("M23", -1), _d("M24", -2), RUN("M24"), "M24", div_name="U15 v3", p1month="202701"),
                                     ver(_d("M24", -1), MAX, RUN("M24"), "M24", div_name="U15 v4", p1month="999912")])
    c.expect("M24", "diff.div_h", k, [leg("R", "I", _d("M24", -1), MAX, RUN("M24"), div_name="U15 v4"),
                                     leg("U", "D", _d("M24", -1), MAX, RUN("M24"), div_name="U15 v4"),
                                     leg("R", "U", _d("M23", -1), _d("M24", -2), RUN("M24"), div_name="U15 v3"),
                                     leg("U", "U", _d("M23", -1), MAX, RUN("M24"), div_name="U15 v3")])
    c.expect("M24", "snp.div_", k, [hub(RUN("M20"), "M20")])


def u16():
    c = case(id="U16", chain="mcut", title="Fork rtfw_stg_leftovers_cutover U12 / DV-28: div_type returner across the cutover (3.0 cached the end)",
             rules=["R13", "R14", "H07-snp-incr"], changes=["HPWR-returner-start-date", "HPWR-vanish-divtype"], tables=["div_type", "div_h"],
             known40="DV-28: 4.0 has no loader cache; it must take the returner start from the closed version in snp",
             summary="U16_1 vanishes at M21 (3.0): the OLD row closes at current_date-1 = 2027-01-12 into 202701 and 3.0 "
                     "stores (div_sk, 2027-01-12) in stg.loader_cache_div_type (E/slave/loading/DataLoaderHistSnp.java:103-116). "
                     "M22: nothing. M23 (cutover INCR, both versions, 4.0 over the 3.0 root): the key returns - 3.0 opens it at "
                     "cached end + 1 = 2027-01-13 (:86-96), not at 1900. M24: EQUALS. div_h keeps the vanished key open "
                     "(O/impl/loader/DataLoaderHistMix.java:75-80, OLD kept). The fork's emptied cache before the return is "
                     "a SQL seed (IFT).")
    k = "U16_1"
    c.div(k, at={"M21": GONE, "M23": {}})
    t1 = ver(MIN, _d("M21", -1), RUN("M21"), "M21", p1month="202701")
    c.expect("M20", "snp.div_type", k, [ver(MIN, MAX, RUN("M20"), "M20", p1month="999912")])
    c.expect("M21", "snp.div_type", k, [t1], note="vanish closes at today-1")
    c.expect("M21", "diff.div_type", k, [leg("R", "U", MIN, _d("M21", -1), RUN("M21")), leg("U", "U", MIN, MAX, RUN("M21"))])
    c.expect("M22", "snp.div_type", k, [t1])
    c.expect("M22", "diff.div_type", k, [])
    opened = ver(_d("M21"), MAX, RUN("M23"), "M23", p1month="999912")
    c.expect("M23", "snp.div_type", k, [t1, opened], note="returner start = cached end + 1 (not 1900)")
    c.expect("M23", "diff.div_type", k, [leg("R", "I", _d("M21"), MAX, RUN("M23")), leg("U", "D", _d("M21"), MAX, RUN("M23"))])
    c.expect("M24", "snp.div_type", k, [t1, opened])
    c.expect("M24", "diff.div_type", k, [])
    for ld in MC:
        c.expect(ld, "snp.div_h", k, [ver(_d("M20"), MAX, START, "M20")])
    c.expect("M23", "diff.div_h", k, [])


# =========================================================================================================== chain mval
def u40():
    c = case(id="U40", chain="mval", title="DEV C1 (dsrc C11): every FK empty next to a twin with every FK filled",
             rules=["H03-prep", "H07-snp-incr", "H08-diff-legs-incr", "L5-link-incr-outcomes"], changes=[],
             tables=["div_", "div_h", "div_div_relation"],
             summary="U40_N: no BIC, no fact address, no registration number, legal address without fias/okato/oktmo, "
                     "successor unknown. U40_F: all filled and found in the references. div_h: pay_sys_prtcpnt_sk / "
                     "fact_addr_sk NULL vs set (O/tables/divh/DivHTransformer.java:65-69); div_: reg_ord, legal_addr_sk "
                     "(key 'NULL|<txt>|NULL|NULL', O/tables/addr/AddrTransformer.java:248-263); link to an unknown successor "
                     "keeps successor_div_sk NULL. M31 names change (S2: day after INIT, new version starts at the INIT date), "
                     "M33 legal addresses change: (R,U) new key + (U,U) old key. DEV: pay_sys 100% NULL, reg_ord 85%, "
                     "successor 61%, fact_addr ~11%.")
    c.div("U40_N", address_legaladdrdoc="U40 legal N", at={"M31": dict(fullname="U40 N2"), "M33": dict(address_legaladdrdoc="U40 legal N2")})
    c.div("U40_F", address_legaladdrdoc="U40 legal F", address_locationaddress="U40 fact F", requisitesdivision_bic="U40BIC",
          requisitesdivision_registrationnum="1481/40", address_addrfact_fiascode="u40-fias", address_addrfact_okatocode="u40okato",
          address_addrfact_oktmocode="u40oktmo", address_subjectcode="77", address_country="RU",
          at={"M31": dict(fullname="U40 F2"), "M33": dict(address_legaladdrdoc="U40 legal F2")})
    c.succ("U40_N", "U40_unknown", "правопреемник")
    c.succ("U40_F", "U40_N", "правопреемник")
    for t, sid in (("gar", "u40-fias"), ("okato", "u40okato"), ("oktmo", "u40oktmo"), ("pay", "U40BIC")):
        c.ref(t, sid)
    fact = "NULL|U40 fact F|NULL|NULL"
    c.expect("M30", "snp.div_h", "U40_N", [ver(_d("M30"), MAX, START, "M30", pay_sys_prtcpnt_sk=None, fact_addr_sk=None)])
    c.expect("M30", "snp.div_h", "U40_F", [ver(_d("M30"), MAX, START, "M30", pay_sys_prtcpnt_sk=NOTNULL, fact_addr_sk=fact)])
    c.expect("M31", "snp.div_h", "U40_N", [ver(_d("M30"), MAX, RUN("M31"), "M31", div_name="U40 N2", pay_sys_prtcpnt_sk=None, fact_addr_sk=None)],
             note="S2: the INIT version is dropped, the new one starts at D-1 = the INIT date")
    c.expect("M31", "diff.div_h", "U40_N", [leg("R", "I", _d("M30"), MAX, RUN("M31"), div_name="U40 N2"),
                                           leg("U", "D", _d("M30"), MAX, RUN("M31"), div_name="U40 N2")])
    c.expect("M31", "snp.div_h", "U40_F", [ver(_d("M30"), MAX, RUN("M31"), "M31", div_name="U40 F2", pay_sys_prtcpnt_sk=NOTNULL, fact_addr_sk=fact)])
    legal = {"U40_N": ("NULL|U40 legal N|NULL|NULL", "NULL|U40 legal N2|NULL|NULL", None, None),
             "U40_F": ("u40-fias|U40 legal F|u40okato|u40oktmo", "u40-fias|U40 legal F2|u40okato|u40oktmo", "40", "1481/40")}
    for k, (l0, l3, ro, frn) in legal.items():
        c.expect("M30", "snp.div_", k, [hub(RUN("M30"), "M30", legal_addr_sk=l0, reg_ord=ro, full_reg_num=frn)])
        c.expect("M33", "diff.div_", k, [hleg("R", "U", RUN("M33"), legal_addr_sk=l3, reg_ord=ro),
                                        hleg("U", "U", RUN("M30"), legal_addr_sk=l0, reg_ord=ro)])
        c.expect("M33", "snp.div_", k, [hub(RUN("M33"), "M33", legal_addr_sk=l3, reg_ord=ro, full_reg_num=frn)])
    c.expect("M30", "snp.div_div_relation", "U40_N|NULL", [ver(MIN, MAX, RUN("M30"), "M30", source_div_sk="U40_N", successor_div_sk=None)])
    c.expect("M30", "snp.div_div_relation", "U40_F|U40_N", [ver(MIN, MAX, RUN("M30"), "M30", source_div_sk="U40_F", successor_div_sk="U40_N")])
    c.expect("M33", "snp.div_div_relation", "U40_N|NULL", [ver(MIN, MAX, RUN("M30"), "M30", source_div_sk="U40_N", successor_div_sk=None)])


def u41():
    c = case(id="U41", chain="mval", title="DEV D1 (dsrc D11): parent_div_sk points outside the set of divisions",
             rules=["H07-snp-incr", "H08-diff-legs-incr"], changes=[], tables=["div_"],
             summary="U41_1's parent 'U41_OUT' is not a division of the product (DEV: 29%); at M31 it moves to another outside "
                     "parent 'U41_OUT2', at M33 to the existing U41_2. 3.0 takes headdivisioncode through the SK client "
                     "without any check (O/tables/div/DivTransformer.java:43, columnEntities :91-94) and publishes it with "
                     "dirty_flag=false; each move = (R,U)+(U,U).")
    c.div("U41_1", headdivisioncode="U41_OUT", at={"M31": dict(headdivisioncode="U41_OUT2"), "M33": dict(headdivisioncode="U41_2")})
    c.div("U41_2", headdivisioncode="U41_OUT")
    c.expect("M30", "snp.div_", "U41_1", [hub(RUN("M30"), "M30", parent_div_sk=NOTNULL, dirty_flag="false")])
    c.expect("M31", "diff.div_", "U41_1", [hleg("R", "U", RUN("M31"), parent_div_sk=NOTNULL), hleg("U", "U", RUN("M30"), parent_div_sk=NOTNULL)])
    c.expect("M32", "diff.div_", "U41_1", [])
    c.expect("M33", "diff.div_", "U41_1", [hleg("R", "U", RUN("M33"), parent_div_sk="U41_2"), hleg("U", "U", RUN("M31"), parent_div_sk=NOTNULL)])
    c.expect("M33", "snp.div_", "U41_2", [hub(RUN("M30"), "M30", parent_div_sk=NOTNULL)])


def u42():
    c = case(id="U42", chain="mval", title="DEV D2 (dsrc D21): time zone reference outside the time_zone_rf dictionary",
             rules=["H07-snp-incr", "H08-diff-legs-incr"], changes=[], tables=["div_", "time_zone_rf"],
             summary="Dictionary: one row U42_TZ. U42_1 refers to 'U42_TZX' (not in it), U42_2 to 'U42_TZ'; at M31 they swap to "
                     "the known / an unknown zone 'U42_TZY'. 3.0 maps only USZ1 -> KALT and keeps any other value "
                     "(O/tables/div/DivTransformer.java:56-57), no check against time_zone_rf. DEV: 85% of the references.")
    c.tz("U42_TZ", code=210, abbreviation="U42_TZ", name00="U42 zone", timedesignation="0", utcformat="+3")
    c.div("U42_1", address_addrfact_timezone="U42_TZX", at={"M31": dict(address_addrfact_timezone="U42_TZ")})
    c.div("U42_2", address_addrfact_timezone="U42_TZ", at={"M31": dict(address_addrfact_timezone="U42_TZY")})
    c.expect("M30", "snp.time_zone_rf", "U42_TZ", [hub(RUN("M30"), "M30", time_zone_name="U42 zone", time_zone_code="210")])
    c.expect("M30", "snp.div_", "U42_1", [hub(RUN("M30"), "M30", div_time_zone_time_zone_rf_sk=NOTNULL)])
    c.expect("M30", "snp.div_", "U42_2", [hub(RUN("M30"), "M30", div_time_zone_time_zone_rf_sk="U42_TZ")])
    c.expect("M31", "diff.div_", "U42_1", [hleg("R", "U", RUN("M31"), div_time_zone_time_zone_rf_sk="U42_TZ"),
                                          hleg("U", "U", RUN("M30"), div_time_zone_time_zone_rf_sk=NOTNULL)])
    c.expect("M31", "diff.div_", "U42_2", [hleg("R", "U", RUN("M31"), div_time_zone_time_zone_rf_sk=NOTNULL),
                                          hleg("U", "U", RUN("M30"), div_time_zone_time_zone_rf_sk="U42_TZ")])
    c.expect("M33", "snp.time_zone_rf", "U42_TZ", [hub(RUN("M30"), "M30", time_zone_name="U42 zone")])


def u43():
    c = case(id="U43", chain="mval", title="DEV E1 (dsrc E11): garbage dates - close 0201-01-19, close < open, open 1997 / close 2023",
             rules=["H03-prep", "H07-snp-incr", "H08-diff-legs-incr"], changes=[], tables=["div_"],
             known40="DV-10 (reading dates before 1582): the fork run 11-09 gave 'same'",
             summary="U43_1 closedate 0201-01-19 (year 201), U43_2 opened 2023-05-01 and closed 2020-01-01, U43_3 open "
                     "1997-05-20 / close 2023-12-22. 3.0 casts and publishes them unchanged (O/tables/chgdivh/ChgDivHSql.java:18-19, "
                     "O/tables/div/DivTransformer.java:40-41). M31: U43_1 close -> 0202-02-02, U43_2 open -> 1899-12-31: "
                     "(R,U)+(U,U). DEV: 23 rows close < open, min close 0201-01-19.")
    c.div("U43_1", closedate="0201-01-19 00:00:00", at={"M31": dict(closedate="0202-02-02 00:00:00")})
    c.div("U43_2", opendate="2023-05-01 00:00:00", closedate="2020-01-01 00:00:00", at={"M31": dict(opendate="1899-12-31 00:00:00")})
    c.div("U43_3", opendate="1997-05-20 00:00:00", closedate="2023-12-22 00:00:00")
    c.expect("M30", "snp.div_", "U43_1", [hub(RUN("M30"), "M30", div_close_dt="0201-01-19", div_open_dt="2020-01-01")])
    c.expect("M30", "snp.div_", "U43_2", [hub(RUN("M30"), "M30", div_close_dt="2020-01-01", div_open_dt="2023-05-01")])
    c.expect("M30", "snp.div_", "U43_3", [hub(RUN("M30"), "M30", div_close_dt="2023-12-22", div_open_dt="1997-05-20")])
    c.expect("M31", "diff.div_", "U43_1", [hleg("R", "U", RUN("M31"), div_close_dt="0202-02-02"), hleg("U", "U", RUN("M30"), div_close_dt="0201-01-19")])
    c.expect("M31", "diff.div_", "U43_2", [hleg("R", "U", RUN("M31"), div_open_dt="1899-12-31"), hleg("U", "U", RUN("M30"), div_open_dt="2023-05-01")])
    c.expect("M33", "snp.div_", "U43_1", [hub(RUN("M31"), "M31", div_close_dt="0202-02-02")])
    c.expect("M33", "diff.div_", "U43_3", [])


def u44():
    c = case(id="U44", chain="mval", title="DEV E1 (dsrc E22): duplicate (fulldivcode, start_dt) in the d_ history",
             rules=["R10", "S2-chgdivh-init", "L8-histmix-init", "R05"], changes=[], tables=["div_h"], deterministic=False,
             known40="DV-78: on one input 3.0 and 4.0 systematically keep different twins (fork dsrc: 3.0 'twin b', 4.0 'twin a')",
             summary="Two d_ rows of U44_1 with the SAME ctl_validfrom 2026-04-04 10:00 and different names, two more on 2026-06-06 "
                     "at 09:00 and 18:00 (same start_dt). INIT chain: lead(start_dt) ORDER BY start_dt only "
                     "(O/tables/chgdivh/ChgDivHSql.java:177); of a tie one row gets end = start - 1 and is cut by datediff >= 0 "
                     "(:297): exactly one version per start_dt, which name is the tie (DV-78, not checked). M33: the snapshot "
                     "version closes [2027-01-13, 2027-01-30], 202606 stays outside the rewrite zone - dates checked.")
    r = c.div("U44_1", fullname="U44 now", at={"M33": dict(fullname="U44 M33")})
    c.hist(r, "2026-04-04 10:00:00", 1, fullname="U44 twin a")
    c.hist(r, "2026-04-04 10:00:00", 2, fullname="U44 twin b")
    c.hist(r, "2026-06-06 09:00:00", 3, fullname="U44 morning")
    c.hist(r, "2026-06-06 18:00:00", 4, fullname="U44 evening")
    init = [dict(start_dt="2026-04-04", end_dt="2026-06-05", p1month="202606"),
            dict(start_dt="2026-06-06", end_dt=_d("M30", -1), p1month="202701"),
            dict(start_dt=_d("M30"), end_dt=MAX, p1month="999912")]
    c.expect("M30", "snp.div_h", "U44_1", init)
    c.expect("M31", "snp.div_h", "U44_1", init)
    c.expect("M33", "snp.div_h", "U44_1", init[:2] + [dict(start_dt=_d("M30"), end_dt=_d("M33", -2), p1month="202701"),
                                                     dict(start_dt=_d("M33", -1), end_dt=MAX, p1month="999912")])


def u45():
    c = case(id="U45", chain="mval", title="DEV F1 (dsrc F11): leading/trailing/double spaces in codes, names and type codes",
             rules=["H03-prep", "H05-hash", "S3-chgdivh-incr", "H07-snp-incr"], changes=[], tables=["div_", "div_h", "div_type", "terr_div_type"],
             summary="fulldivcode ' U45_1' and 'U45_2 ', name with double inner spaces (len 87), type codes 'U45_T ' and ' U45_T'. "
                     "3.0 trims only address parts (O/tables/chgdivh/ChgDivHSql.java:24-38); codes, names and types pass as "
                     "they are: keys with spaces, two terr_div_type hub rows 'U45_T ' and ' U45_T'. M31: a trailing space added "
                     "to a name is a change (new div_h version); U45_3's type loses its space: div_type closes 'U45_T ' at "
                     "2027-01-12, opens 'U45_T' at the INIT date, terr_div_type gets a NEW 'U45_T'. DEV: 70 of 8070 type codes, "
                     "17 div_ codes, 25 names.")
    long_name = ("U45  Отделение  №8590/07777  " + "x" * 87)[:87]
    c.div(" U45_1", fullname=long_name, divisiontype00="U45_T ")
    c.div("U45_2 ", fullname=" U45 name ", divisiontype00=" U45_T", at={"M31": dict(fullname=" U45 name  ")})
    c.div("U45_3", divisiontype00="U45_T ", at={"M31": dict(divisiontype00="U45_T")})
    c.expect("M30", "snp.div_", " U45_1", [hub(RUN("M30"), "M30", code=" U45_1")])
    c.expect("M30", "snp.div_h", " U45_1", [ver(_d("M30"), MAX, START, "M30", div_name=long_name)])
    c.expect("M33", "snp.div_h", " U45_1", [ver(_d("M30"), MAX, START, "M30", div_name=long_name)])
    c.expect("M30", "snp.terr_div_type", "U45_T ", [hub(RUN("M30"), "M30", div_type_code="U45_T ")])
    c.expect("M30", "snp.terr_div_type", " U45_T", [hub(RUN("M30"), "M30", div_type_code=" U45_T")])
    c.expect("M31", "snp.div_h", "U45_2 ", [ver(_d("M30"), MAX, RUN("M31"), "M31", div_name=" U45 name  ")])
    c.expect("M31", "diff.div_h", "U45_2 ", [leg("R", "I", _d("M30"), MAX, RUN("M31"), div_name=" U45 name  "),
                                            leg("U", "D", _d("M30"), MAX, RUN("M31"), div_name=" U45 name  ")])
    c.expect("M31", "snp.div_type", "U45_3", [ver(MIN, _d("M31", -2), RUN("M31"), "M31", div_type_terr_div_type_sk="U45_T ", p1month="202701"),
                                             ver(_d("M31", -1), MAX, RUN("M31"), "M31", div_type_terr_div_type_sk="U45_T", p1month="999912")])
    c.expect("M31", "snp.terr_div_type", "U45_T", [hub(RUN("M31"), "M31", div_type_code="U45_T")])
    c.expect("M31", "snp.terr_div_type", "U45_T ", [hub(RUN("M30"), "M30", div_type_code="U45_T ")],
             note="the hub keeps the old code (vanished hub rows stay)")


def u46():
    c = case(id="U46", chain="mval", title="DEV F2 (dsrc F21): very long address strings (>100 characters)",
             rules=["R08", "H07-snp-incr"], changes=[], tables=["div_h", "addr"],
             summary="Fact address of 130 characters, legal address of 150; at M31 the fact address changes in character 120 -> "
                     "the address key (src_txt is part of it, O/tables/addr/AddrTransformer.java:248-263) changes, div_h gets "
                     "a new version with the new fact_addr_sk (S2: start at the INIT date). No truncation in 3.0. DEV: 18 strings > 100.")
    fact = ("U46 fact " + "улица Длинная дом 1 " * 10)[:130]
    fact2 = fact[:119] + "Z" + fact[120:]
    c.div("U46_1", address_locationaddress=fact, address_legaladdrdoc=("U46 legal " + "проспект " * 20)[:150],
          at={"M31": dict(address_locationaddress=fact2)})
    c.expect("M30", "snp.div_h", "U46_1", [ver(_d("M30"), MAX, START, "M30", fact_addr_sk=addr_key(src=fact))])
    c.expect("M31", "snp.div_h", "U46_1", [ver(_d("M30"), MAX, RUN("M31"), "M31", fact_addr_sk=addr_key(src=fact2))])
    c.expect("M30", "snp.addr", addr_key(src=fact), [hub(RUN("M30"), "M30", src_txt=fact)])


def u47():
    c = case(id="U47", chain="mval", title="DEV F3 (dsrc F31): one name on many keys; the literal string 'null' as a name",
             rules=["H05-hash", "H07-snp-incr"], changes=[], tables=["div_h"],
             summary="U47_1..3 are named 'null' (the string), U47_4..8 share '(800)5555550', U47_9 is named 'NULL'. 3.0 keeps the "
                     "strings (no NULL mapping in ChgDivHSql.java:16 / DivHTransformer.java:34). M31: U47_1 -> '(800)5555550', "
                     "U47_4 -> 'null': new versions, others EQUALS. DEV: 'null' on 116 keys, one name on 770 keys.")
    for i in range(1, 10):
        name = "null" if i <= 3 else "(800)5555550" if i <= 8 else "NULL"
        at = {"M31": dict(fullname="(800)5555550")} if i == 1 else {"M31": dict(fullname="null")} if i == 4 else None
        k = "U47_%d" % i
        c.div(k, fullname=name, at=at)
        c.expect("M30", "snp.div_h", k, [ver(_d("M30"), MAX, START, "M30", div_name=name)])
        if at:
            new = at["M31"]["fullname"]
            c.expect("M31", "snp.div_h", k, [ver(_d("M30"), MAX, RUN("M31"), "M31", div_name=new)])
            c.expect("M31", "diff.div_h", k, [leg("R", "I", _d("M30"), MAX, RUN("M31"), div_name=new),
                                             leg("U", "D", _d("M30"), MAX, RUN("M31"), div_name=new)])
        else:
            c.expect("M31", "diff.div_h", k, [])


# ========================================================================================================= chain mretry
def u30():
    c = case(id="U30", chain="mretry", title="Fork essa_retry_rerun F3: relaunch of a completed INCR with the same loadingId, then the next INCR",
             rules=["H07-snp-incr", "R07", "R13", "R09"], changes=["NH-undo-keep-validfrom"], tables=["div_", "div_h", "div_type"],
             known40="DV-08/DV-18 decision: replay guard removed (3.0 behaviour); R01 has the same no-op pairs",
             summary="M41 changes U30_1 (name), U30_4 (short code), U30_5 (type); U30_6 unchanged. M42 relaunches loading "
                     "20331 (same sources): every snp table stays as after M41; rows M41 wrote that are still open (U30_1 "
                     "div_h, U30_5 open div_type) get a no-op (R,U)+(U,U) pair with the M41 ctl_validfrom: rows with the current "
                     "ctl_loading are never EQUALS (window desc(ctl_loading), asc(record_status): NEW before OLD, "
                     "E/slave/loading/SnpDataLoader.java:181); the hub (div_) gets no leg (OLD EQ rn2 kept). M43: nothing "
                     "changes, no legs. ctl_loading 20331 is shown as M41. Failure injection of the fork (F1 div_h retry, F2a "
                     "mover retry) is IFT.")
    c.div("U30_1", fullname="U30 A1", at={"M41": dict(fullname="U30 B1")})
    c.div("U30_4", at={"M41": dict(shortdivcode="02")})
    c.div("U30_5", divisiontype00="U30_TA", at={"M41": dict(divisiontype00="U30_TB")})
    c.div("U30_6")
    h1 = [ver(_d("M40"), MAX, RUN("M41"), "M41", div_name="U30 B1")]
    t5 = [ver(MIN, _d("M41", -2), RUN("M41"), "M41", div_type_terr_div_type_sk="U30_TA", p1month="202701"),
          ver(_d("M41", -1), MAX, RUN("M41"), "M41", div_type_terr_div_type_sk="U30_TB", p1month="999912")]
    d4 = [hub(RUN("M41"), "M41", short_div_code="02")]
    legs_h = [leg("R", "I", _d("M40"), MAX, RUN("M41"), div_name="U30 B1"), leg("U", "D", _d("M40"), MAX, RUN("M41"), div_name="U30 B1")]
    legs_t = [leg("R", "I", _d("M41", -1), MAX, RUN("M41"), div_type_terr_div_type_sk="U30_TB"),
              leg("U", "D", _d("M41", -1), MAX, RUN("M41"), div_type_terr_div_type_sk="U30_TB"),
              leg("R", "U", MIN, _d("M41", -2), RUN("M41"), div_type_terr_div_type_sk="U30_TA"),
              leg("U", "U", MIN, MAX, RUN("M41"), div_type_terr_div_type_sk="U30_TA")]
    legs_d = [hleg("R", "U", RUN("M41"), short_div_code="02"), hleg("U", "U", RUN("M40"), short_div_code="01")]
    noop_h = [leg("R", "U", _d("M40"), MAX, RUN("M41"), div_name="U30 B1"), leg("U", "U", _d("M40"), MAX, RUN("M41"), div_name="U30 B1")]
    noop_t = [leg("R", "U", _d("M41", -1), MAX, RUN("M41"), div_type_terr_div_type_sk="U30_TB"),
              leg("U", "U", _d("M41", -1), MAX, RUN("M41"), div_type_terr_div_type_sk="U30_TB")]
    c.expect("M41", "diff.div_h", "U30_1", legs_h)
    c.expect("M41", "diff.div_type", "U30_5", legs_t)
    c.expect("M41", "diff.div_", "U30_4", legs_d)
    c.expect("M42", "diff.div_h", "U30_1", legs_h + noop_h, note="M41 legs plus the rerun's no-op pair (same partition 20331)")
    c.expect("M42", "diff.div_type", "U30_5", legs_t + noop_t, note="no pair for the closed TA row (not in 999912)")
    c.expect("M42", "diff.div_", "U30_4", legs_d, note="hub: no leg on the rerun")
    for ld in ("M41", "M42", "M43"):
        c.expect(ld, "snp.div_h", "U30_1", h1)
        c.expect(ld, "snp.div_type", "U30_5", t5)
        c.expect(ld, "snp.div_", "U30_4", d4)
        c.expect(ld, "snp.div_", "U30_6", [hub(RUN("M40"), "M40")])
        c.expect(ld, "diff.div_h", "U30_6", [])
    for k, t in (("U30_1", "div_h"), ("U30_5", "div_type"), ("U30_4", "div_")):
        c.expect("M43", "diff." + t, k, [])


def u31():
    c = case(id="U31", accepted=DV77, accept=["div_:U31_1", "div_:U31_2", "div_:U31_3", "div_:U31_4"], chain="mretry", title="DV-08 over a dirty target: relaunch with the same loadingId over twins 3.0 wrote itself",
             rules=["H13-key-dedup", "R16", "H07-snp-incr"], changes=[], tables=["div_", "div_h", "div_type"], deterministic=False,
             known40="DV-08 (rn >= 3 and groups with one ctl_loading in window30) - not shown by a run yet",
             summary="U31_1..4: two m_ rows per key, short codes '01'/'02' (order flipped on even keys). M41 writes 1-2 div_ rows "
                     "(R16); M42 = M41 again with the same loadingId: NEW and the M41 rows share ctl_loading, asc(record_status) "
                     "puts the two NEW rows first, then the OLD rows - outcome set as in U01, not checked. div_h and div_type "
                     "collapse (" + _DST + "), stored with ctl_loading M40 -> EQUALS, no legs on M41-M43 - checked.")
    for i in range(1, 5):
        k = "U31_%d" % i
        _pair(c, k, dict(shortdivcode="01", fullname="U31"), dict(shortdivcode="02", fullname="U31"))
        c.expect("M40", "snp.div_", k, [hub(RUN("M40"), "M40")])
        for ld in ("M40", "M41", "M42", "M43"):
            c.expect(ld, "snp.div_h", k, [ver(_d("M40"), MAX, START, "M40", div_name="U31")])
            if ld != "M40":
                c.expect(ld, "diff.div_h", k, [])
        _stable_divtype(c, k, ["M40", "M41", "M42", "M43"], "M40")


for _f in [u01, u02, u03, u04, u05, u06, u07, u08, u10, u11, u12, u13, u14, u15, u16, u40, u41, u42, u43, u44, u45, u46, u47, u30, u31]:
    _f()
