"""Audit 1.3, theme hist: PERIOD tables (div_h, div_type, addr_h) - same-PK closed/open pairs in the rewrite zone,
future month partitions, sid collisions of brand-new keys. One case = one SVOD candidate (DV-NN in the title).

Paths in summaries: 3.0 = vspc_idl_old/src/main/java/ru/sberbank/bigdata/dffess/vspc/, ESSA = essa-common-lib
.../essa/common/. Chain `sameday4` (T05, T06) is NEW: INIT, four INCRs on one calendar day, one INCR the next day.
"""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case, DV77

_HISTMIX = "3.0 impl/loader/DataLoaderHistMix.java:66-69 (only 999912 is merged; current+previous month returned as is), " \
           ":75-89 (closure at D-2, dropped when end < start), :94 (union rewritableData)"
_HISTSNP = "ESSA slave/loading/DataLoaderHistSnp.java:70-73 (999912 merged, current+previous month as is), :86-96 " \
           "(returner start = cached end + 1), :103-108 (vanish closes at current_date-1, change at D-2), :119 (OLD rows kept " \
           "even when end < start; other rows dropped when end < start), :124"


def _pair_setup(c, key, at):
    """INIT: d_ history + a future-dated d_ version (2027-02-01). The snapshot version gets [INIT, Jan 31] in 202701,
    the future version is open; at L1 the snapshot (different name) reopens [D-1 = INIT date, MAX] -> same PK as the
    closed 202701 row (C30 mechanism on the main chain)."""
    r = c.div(key, fullname="%s now" % c.id, at=at)
    c.hist(r, "2026-06-01 10:00:00", 1, fullname="%s v1" % c.id)
    c.hist(r, "2027-02-01 10:00:00", 2, fullname="%s future" % c.id)
    v1 = ver("2026-06-01", d("L0", -1), START, "L0", div_name="%s v1" % c.id, p1month="202701")
    closed = ver(d("L0"), "2027-01-31", START, "L0", div_name="%s now" % c.id, p1month="202701")
    c.expect("L0", "snp.div_h", key, [v1, closed, ver("2027-02-01", MAX, START, "L0", div_name="%s future" % c.id, p1month="999912")])
    opened = ver(d("L1", -1), MAX, RUN("L1"), "L1", div_name="%s now" % c.id, p1month="999912")
    c.expect("L1", "snp.div_h", key, [v1, closed, opened],
             note="future version cannot close at D-2=%s (< its start) and is dropped; the reopened version [%s, MAX] has "
                  "the same PK (div_sk, start_dt) as the closed INIT row [%s, 2027-01-31] in 202701" % (d("L1", -2), d("L1", -1), d("L0")))
    c.expect("L1", "diff.div_h", key, [leg("R", "I", d("L1", -1), MAX, RUN("L1"), div_name="%s now" % c.id),
                                      leg("U", "D", d("L1", -1), MAX, RUN("L1"), div_name="%s now" % c.id)])
    return v1, closed


def t01():
    c = case(id="T01", title="DV-04 (a): div_h closed+open versions with one PK in 202701/999912, change on the SAME day",
             rules=["R10", "R05", "R07", "R15", "M2-loader-mover-window-contract"], changes=["HPWR-close-open-only", "HPWR-silent-drop"],
             tables=["div_h"],
             summary="DV-04. L0: a d_ version dated 2027-02-01 (after INIT) makes the snapshot version [L0, Jan 31] (202701) and "
                     "the future one open. L1 (next day): the snapshot differs from the open version -> the open one would close at "
                     "D-2 < its start and is dropped; the new version opens [D-1 = L0 date, MAX]: closed [L0, Jan 31] and open "
                     "[L0, MAX] share the PK (div_sk, start_dt). L2 (same day as L1) changes the name: 3.0 merges the input only "
                     "with 999912, the open version again cannot close (D-2 < start) and is dropped, the new one opens at "
                     "D-1 = L0 date; the 202701 row [L0, Jan 31] is returned untouched as rewritableData. "
                     "3.0: " + _HISTMIX + "; ChgDivHSql.java:176 (INIT end = next start - 1). "
                     "If DV-04 is real, 4.0 (vendor merge by full PK) loses the closed [L0, Jan 31] row at L2.")
    v1, closed = _pair_setup(c, "T01_1", {"L2": dict(fullname="T01 B")})
    c.expect("L2", "snp.div_h", "T01_1", [v1, closed, ver(d("L2", -1), MAX, RUN("L2"), "L2", div_name="T01 B", p1month="999912")],
             note="the closed [%s, 2027-01-31] row (202701) stays; the L1 version [%s, MAX] is dropped (S1)" % (d("L0"), d("L1", -1)))
    c.expect("L2", "diff.div_h", "T01_1", [leg("R", "I", d("L2", -1), MAX, RUN("L2"), div_name="T01 B"),
                                          leg("U", "D", d("L2", -1), MAX, RUN("L2"), div_name="T01 B")],
             note="no legs for the dropped L1 version and none for the closed 202701 row")


def t02():
    c = case(id="T02", title="DV-04 (b): div_h closed+open versions with one PK in 202701/999912, change on the NEXT day",
             rules=["R10", "R05", "R07", "R11", "M2-loader-mover-window-contract"], changes=["HPWR-close-open-only"],
             tables=["div_h"],
             summary="DV-04. Same L0/L1 as T01 (closed [L0, Jan 31] and open [L0, MAX] with one PK after L1); L2 unchanged. "
                     "L3 (next day) changes the name: the open version closes normally at D-2 = L0 date -> [L0, L0], the new "
                     "opens at D-1; the closed 202701 row [L0, Jan 31] is returned untouched, so 202701 holds two rows with "
                     "start_dt = L0 and different end_dt. 3.0: " + _HISTMIX + ". "
                     "If DV-04 is real, 4.0 (closure LEFT-joined by the full PK) writes [L0, L0] twice instead.")
    v1, closed = _pair_setup(c, "T02_1", {"L3": dict(fullname="T02 B")})
    c.expect("L2", "snp.div_h", "T02_1", [v1, closed, ver(d("L1", -1), MAX, RUN("L1"), "L1", div_name="T02 now", p1month="999912")],
             note="L2: input equals the open version (EQUALS) - nothing changes")
    c.expect("L2", "diff.div_h", "T02_1", [])
    c.expect("L3", "snp.div_h", "T02_1", [v1, closed,
                                         ver(d("L1", -1), d("L3", -2), RUN("L3"), "L3", div_name="T02 now", p1month="202701"),
                                         ver(d("L3", -1), MAX, RUN("L3"), "L3", div_name="T02 B", p1month="999912")],
             note="two rows with PK (T02_1, %s): [%s, 2027-01-31] from INIT and [%s, %s] closed at L3" % (d("L0"), d("L0"), d("L0"), d("L3", -2)))
    c.expect("L3", "diff.div_h", "T02_1", [leg("R", "I", d("L3", -1), MAX, RUN("L3"), div_name="T02 B"),
                                          leg("U", "D", d("L3", -1), MAX, RUN("L3"), div_name="T02 B"),
                                          leg("R", "U", d("L1", -1), d("L3", -2), RUN("L3"), div_name="T02 now"),
                                          leg("U", "U", d("L1", -1), MAX, RUN("L3"), div_name="T02 now")])


def _future_setup(c, key, at=None, hist_at=None):
    """INIT with two d_ versions dated after INIT: closed versions land in the future partitions 202702 and 202703."""
    r = c.div(key, fullname="%s X" % c.id, at=at)
    c.hist(r, "2026-06-01 10:00:00", 1, fullname="%s old" % c.id, at=hist_at)
    c.hist(r, "2027-02-10 10:00:00", 2, fullname="%s Y" % c.id, at=hist_at)
    c.hist(r, "2027-03-05 10:00:00", 3, fullname="%s X" % c.id, at=hist_at)
    old = ver("2026-06-01", d(_init(c), -1), START, _init(c), div_name="%s old" % c.id, p1month="202701")
    fut = [ver(d(_init(c)), "2027-02-09", START, _init(c), div_name="%s X" % c.id, p1month="202702"),
           ver("2027-02-10", "2027-03-04", START, _init(c), div_name="%s Y" % c.id, p1month="202703")]
    opened = ver("2027-03-05", MAX, START, _init(c), div_name="%s X" % c.id, p1month="999912")
    c.expect(_init(c), "snp.div_h", key, [old] + fut + [opened],
             note="the snapshot version starts at the INIT date and ends the day before the first future d_ version")
    return old, opened


def _init(c):
    return {"main": "L0", "empty": "E0"}[c.chain]


def t03():
    c = case(id="T03", title="DV-06 (b): div_h versions closed in FUTURE months (202702, 202703) after a non-empty INCR",
             rules=["R10", "R05", "M1-hist-mover-truncation", "M2-loader-mover-window-contract"], changes=["FX26-rewrite-scope"],
             tables=["div_h"],
             summary="DV-06. INIT (Jan 13) with d_ versions dated 2027-02-10 and 2027-03-05 (after INIT): closed versions "
                     "[Jan 13, Feb 9] in 202702 and [Feb 10, Mar 4] in 202703, open [Mar 5, MAX] equal to the snapshot. L1 "
                     "(non-empty INCR, the key unchanged = EQUALS): the 3.0 mover truncates every snp partition >= "
                     "yyyyMM(now - 1 month) = 202612, i.e. also 202702 and 202703, while the loader stages only 999912 and "
                     "yyyyMM(current_date) / -1 month (202701, 202612): the two future-month rows are lost. "
                     "3.0: impl/mover/HistMovingNoCacheGenerator.java:34-35; ESSA slave/hdfs/HdfsService.java:24-25; "
                     "impl/loader/DataLoaderHistMix.java:66-69,94. If DV-06 is real, 4.0 keeps 202702/202703.")
    old, opened = _future_setup(c, "T03_1")
    for ld in ("L1", "L3"):
        c.expect(ld, "snp.div_h", "T03_1", [old, opened],
                 note="202702 and 202703 truncated by the mover and not restaged by the loader" if ld == "L1" else "still lost")
        c.expect(ld, "diff.div_h", "T03_1", [], note="the loss writes no legs")


def t04():
    c = case(id="T04", chain="empty", title="DV-06 (a): div_h versions closed in FUTURE months are lost on an EMPTY INCR",
             rules=["R10", "R05", "H11-empty-incr", "M1-hist-mover-truncation", "M2-loader-mover-window-contract"],
             changes=["FX26-rewrite-scope"], tables=["div_h"],
             summary="DV-06. Same INIT as T03 on the `empty` chain. E1: the sfsdivision snapshot is empty; 3.0 still runs the "
                     "div_h loader (EmptyChecker does not stop it: ESSA slave/check/EmptyChecker.java:12-17) and the mover "
                     "truncates every partition >= 202612; the loader restages only 999912 (all rows OLD) and 202701/202612, so "
                     "the 202702/202703 rows are lost. 3.0: impl/mover/HistMovingNoCacheGenerator.java:34-35; "
                     "impl/loader/DataLoaderHistMix.java:66-69,94. If DV-06 is real, 4.0 (vendor returns early on an empty "
                     "input) keeps them. The division is back at E3 (EQUALS: nothing changes). The d_ rows exist only at E0 (read by INIT only).")
    old, opened = _future_setup(c, "T04_1", at={"E1": GONE, "E3": {}}, hist_at={"E1": GONE})
    for ld in ("E1", "E2", "E3"):
        c.expect(ld, "snp.div_h", "T04_1", [old, opened])
        c.expect(ld, "diff.div_h", "T04_1", [])


def _tt(c, t):
    return "%s_%s" % (c.id, t)


def t05():
    c = case(id="T05", chain="sameday4",
             title="DV-04 div_type: change, vanish, return, change on one day -> closed+open with one PK; change next day",
             rules=["R13", "R14", "R15", "R05"], changes=["HPWR-vanish-divtype", "HPWR-returner-start-date", "HPWR-silent-drop",
                                                          "HPWR-close-open-only"],
             tables=["div_type"],
             summary="DV-04 for div_type (HistSnp). B1..B4 on Jan 15, B5 on Jan 16. B1 T1->T2: [1900, Jan 13] + [Jan 14, MAX]. "
                     "B2 vanish: [Jan 14, Jan 14] (closed at current_date-1, cache end Jan 14). B3 return: [Jan 15, MAX] "
                     "(cached end + 1). B4 T2->T3: the open version would close at D-2 = Jan 13 < Jan 15 and is dropped, the new "
                     "one opens at D-1 = Jan 14: closed [Jan 14, Jan 14] and open [Jan 14, MAX] share the PK. B5 T3->T4: the "
                     "open one closes at D-2 = Jan 14 -> a second [Jan 14, Jan 14] row (other type), new [Jan 15, MAX]. "
                     "3.0: " + _HISTSNP + "; ESSA slave/loading/LoaderCache.java:96-114. If DV-04 is real, 4.0 deletes or "
                     "duplicates the closed T2 row at B5.")
    c.div("T05_1", divisiontype00=_tt(c, "T1"),
          at={"B1": dict(divisiontype00=_tt(c, "T2")), "B2": GONE, "B3": {}, "B4": dict(divisiontype00=_tt(c, "T3")),
              "B5": dict(divisiontype00=_tt(c, "T4"))})
    D = "B1"
    a = ver(MIN, d(D, -2), RUN("B1"), "B1", div_type_terr_div_type_sk=_tt(c, "T1"), p1month="202701")
    b = ver(d(D, -1), d(D, -1), RUN("B2"), "B2", div_type_terr_div_type_sk=_tt(c, "T2"), p1month="202701")
    c.expect("B0", "snp.div_type", "T05_1", [ver(MIN, MAX, RUN("B0"), "B0", div_type_terr_div_type_sk=_tt(c, "T1"), p1month="999912")])
    c.expect("B1", "snp.div_type", "T05_1", [a, ver(d(D, -1), MAX, RUN("B1"), "B1", div_type_terr_div_type_sk=_tt(c, "T2"), p1month="999912")])
    c.expect("B2", "snp.div_type", "T05_1", [a, b], note="vanish closes at current_date-1 = %s" % d(D, -1))
    c.expect("B2", "diff.div_type", "T05_1", [leg("R", "U", d(D, -1), d(D, -1), RUN("B2")), leg("U", "U", d(D, -1), MAX, RUN("B2"))])
    c.expect("B3", "snp.div_type", "T05_1", [a, b, ver(d(D), MAX, RUN("B3"), "B3", div_type_terr_div_type_sk=_tt(c, "T2"), p1month="999912")],
             note="returner: start = cached end + 1 = %s" % d(D))
    pair_open = ver(d(D, -1), MAX, RUN("B4"), "B4", div_type_terr_div_type_sk=_tt(c, "T3"), p1month="999912")
    c.expect("B4", "snp.div_type", "T05_1", [a, b, pair_open],
             note="closed [%s, %s] (B2) and open [%s, MAX] (B4): one PK (div_sk, start_dt); the B3 version is dropped" % (d(D, -1), d(D, -1), d(D, -1)))
    c.expect("B4", "diff.div_type", "T05_1", [leg("R", "I", d(D, -1), MAX, RUN("B4"), div_type_terr_div_type_sk=_tt(c, "T3")),
                                             leg("U", "D", d(D, -1), MAX, RUN("B4"), div_type_terr_div_type_sk=_tt(c, "T3"))])
    c.expect("B5", "snp.div_type", "T05_1", [a, b,
                                            ver(d("B5", -2), d("B5", -2), RUN("B5"), "B5", div_type_terr_div_type_sk=_tt(c, "T3"), p1month="202701"),
                                            ver(d("B5", -1), MAX, RUN("B5"), "B5", div_type_terr_div_type_sk=_tt(c, "T4"), p1month="999912")],
             note="202701 keeps BOTH [%s, %s] rows: T2 (B2) and T3 (closed at B5)" % (d(D, -1), d(D, -1)))
    c.expect("B5", "diff.div_type", "T05_1", [leg("R", "I", d("B5", -1), MAX, RUN("B5"), div_type_terr_div_type_sk=_tt(c, "T4")),
                                             leg("U", "D", d("B5", -1), MAX, RUN("B5"), div_type_terr_div_type_sk=_tt(c, "T4")),
                                             leg("R", "U", d("B5", -2), d("B5", -2), RUN("B5"), div_type_terr_div_type_sk=_tt(c, "T3")),
                                             leg("U", "U", d("B5", -2), MAX, RUN("B5"), div_type_terr_div_type_sk=_tt(c, "T3"))])


def t06():
    c = case(id="T06", chain="sameday4",
             title="DV-04 div_type: vanish, return, vanish, return on one day -> closed+open with one PK; vanish next day",
             rules=["R13", "R14", "R05"], changes=["HPWR-vanish-divtype", "HPWR-returner-start-date", "HPWR-close-open-only"],
             tables=["div_type"],
             summary="DV-04 for div_type (HistSnp), vanish/return only. B1 vanish: [1900, Jan 14]. B2 return: [Jan 15, MAX]. "
                     "B3 vanish of a version that started today: closed at current_date-1 = Jan 14 -> [Jan 15, Jan 14] "
                     "(end < start; 3.0 keeps OLD rows unfiltered). B4 return: [Jan 15, MAX] -> same PK as the B3 row. B5 "
                     "(next day) vanish: [Jan 15, Jan 15]. 3.0: " + _HISTSNP + ". Note: the end < start row at B3 is itself "
                     "an unusual 3.0 outcome; if 4.0 already differs at B3 that is a separate cause, DV-04 shows at B4/B5.")
    c.div("T06_1", at={"B1": GONE, "B2": {}, "B3": GONE, "B4": {}, "B5": GONE})
    D = "B1"
    a = ver(MIN, d(D, -1), RUN("B1"), "B1", p1month="202701")
    b = ver(d(D), d(D, -1), RUN("B3"), "B3", p1month="202701")
    c.expect("B1", "snp.div_type", "T06_1", [a])
    c.expect("B2", "snp.div_type", "T06_1", [a, ver(d(D), MAX, RUN("B2"), "B2", p1month="999912")])
    c.expect("B3", "snp.div_type", "T06_1", [a, b], note="vanish of the B2 version: end %s < start %s, row kept (OLD)" % (d(D, -1), d(D)))
    c.expect("B3", "diff.div_type", "T06_1", [leg("R", "U", d(D), d(D, -1), RUN("B3")), leg("U", "U", d(D), MAX, RUN("B3"))])
    c.expect("B4", "snp.div_type", "T06_1", [a, b, ver(d(D), MAX, RUN("B4"), "B4", p1month="999912")],
             note="closed [%s, %s] and open [%s, MAX]: one PK" % (d(D), d(D, -1), d(D)))
    c.expect("B4", "diff.div_type", "T06_1", [leg("R", "I", d(D), MAX, RUN("B4")), leg("U", "D", d(D), MAX, RUN("B4"))])
    c.expect("B5", "snp.div_type", "T06_1", [a, b, ver(d("B5", -1), d("B5", -1), RUN("B5"), "B5", p1month="202701")])
    c.expect("B5", "diff.div_type", "T06_1", [leg("R", "U", d("B5", -1), d("B5", -1), RUN("B5")), leg("U", "U", d("B5", -1), MAX, RUN("B5"))])


def t07():
    c = case(id="T07", accepted=DV77, accept=["addr_h:t07a|b|T07c|NULL|NULL"], title="DV-09: addr_h sid collision through '|' of two NEW legal addresses on INCR",
             rules=["R08", "R04", "R07", "H13-key-dedup"], changes=["HPWR-new-key-start-1900"], tables=["addr_h", "addr"],
             deterministic=False,
             summary="DV-09. L3: two new divisions; legal address of T07_1: fias 't07a|b' + text 'T07c', of T07_2: fias 't07a' + "
                     "text 'b|T07c'. addr_sk = concat_ws('|', durty, src, okato, oktmo) = 't07a|b|T07c|NULL|NULL' for both "
                     "(3.0 tables/addr/AddrTransformer.java:248-263), standard_address_txt differs (AddrHTransformer.java:48-68; durty = lower(fias) in ChgDivHSql.java:25) "
                     "-> two addr_h input rows with one PK. 3.0 (DataLoaderHistDiff, one window per PK, tie on ctl_loading and "
                     "record_status): rn=1 sees the other hash -> NOTEQUALS -> [D-1, MAX]; rn=2 has no lead -> NEW -> [1900, MAX] "
                     "(ESSA slave/loading/SnpDataLoader.java:181-205, DataLoaderHistDiff.java:78-84). WHICH text gets which start "
                     "is not deterministic: only the dates are checked. The addr hub groups by sid -> one row "
                     "(AddrTransformer.java:211). If DV-09 is real, 4.0 gives both rows [1900, MAX]. Later loads: the stored "
                     "pair shares a window again -> 1 or 2 open rows (tie), not asserted.")
    c.div("T07_1", start="L3", address_addrfact_fiascode="t07a|b", address_legaladdrdoc="T07c")
    c.div("T07_2", start="L3", address_addrfact_fiascode="t07a", address_legaladdrdoc="b|T07c")
    k = addr_key(durty="t07a|b", src="T07c")
    assert k == addr_key(durty="t07a", src="b|T07c")
    c.expect("L2", "snp.addr_h", k, [])
    c.expect("L3", "snp.addr_h", k, [dict(start_dt=MIN, end_dt=MAX, ctl_loading="L3"), dict(start_dt=d("L3", -1), end_dt=MAX, ctl_loading="L3")],
             note="one row [1900, MAX] and one [D-1, MAX]; which standard_address_txt is which is random")
    c.expect("L3", "diff.addr_h", k, [dict(ctl_do="R", ctl_action="I", start_dt=MIN, end_dt=MAX),
                                      dict(ctl_do="U", ctl_action="D", start_dt=MIN, end_dt=MAX),
                                      dict(ctl_do="R", ctl_action="I", start_dt=d("L3", -1), end_dt=MAX),
                                      dict(ctl_do="U", ctl_action="D", start_dt=d("L3", -1), end_dt=MAX)])
    c.expect("L3", "snp.addr", k, [hub(RUN("L3"), "L3")], note="hub: groupBy(sid) -> one row")
    c.note("L4", "T07 addr_h: the stored pair shares one window -> 1 or 2 rows left (tie); not asserted")


for _f in [t01, t02, t03, t04, t05, t06, t07]:
    _f()
