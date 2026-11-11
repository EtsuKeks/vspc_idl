"""Audit 4.3 (dirty paths), theme hist: PERIOD tables (div_h, div_type, addr_h, div_div_relation) over dirt that 3.0
writes ITSELF (twin PKs, closed rows in 999912, future sections, p1month of year < 1000) and then 4.0 continues from
the 3.0 root (loads marked by="30" run on 3.0 only, as in the chain cutover). Case letter O. Not wired into
cases/__init__.py on purpose (the coordinator adds it after the running regression).

Paths in summaries: 3.0 = vspc_idl_old/src/main/java/ru/sberbank/bigdata/dffess/vspc/ (O/), ESSA = essa-common-lib
.../essa/common/ (E/). Window of 3.0 = E/slave/loading/SnpDataLoader.java:181-205: partition by PK, order by
desc(ctl_loading), asc(record_status); NEW rows compare with lead, the others with lag. Rows with the same ctl_loading
and status (two stored twins, two input twins) are a TIE: their order is the physical order (DV-77), so only
projections that do not depend on it are asserted (deterministic=False where an attribute is left out).

NEW chains (to be added to model.CHAINS by the coordinator; the labels O0..O9, C0..C6 are used only here):
  dhist     O0 INIT 2026-11-26 10:00 id 20100 by 30 | O1 INCR 2026-11-28 10:00 id 20101 by 30 |
            O2 INCR 2026-12-01 10:00 id 20102 both  | O3 INCR 2026-12-02 10:00 id 20103 both        (O04, O05)
  dtwin     O4 INIT 2026-11-20 10:00 id 20110 by 30 | O5 INCR 2026-12-02 10:00 id 20111 both |
            O6 INCR 2026-12-03 10:00 id 20112 both                                                (O06, O07, O08)
  dtwinold  O7 INIT 2026-10-14 10:00 id 20120 by 30 | O8 INCR 2026-12-02 10:00 id 20121 both |
            O9 INCR 2026-12-03 10:00 id 20122 both                                                (O09)
  dnonmono  C0 INIT 2026-11-20 10:00 id 20190 by 30 | C1 INCR 2026-12-02 10:00 id 20180 both      (O10)
  dnull     C2 INIT 2026-11-20 10:00 id 20130 by 30 | C3 INCR 2026-12-01 10:00 id 20131 both |
            C4 INCR 2026-12-02 10:00 id 20132 both                                                (O11)
  dyear     C5 INIT 2026-11-20 10:00 id 20140 by 30 | C6 INCR 2026-12-02 10:00 id 20141 both      (O12)
Dates of these loads are literals here (d() needs the chain in model.CHAINS). Cases O01-O03 run on chain main.
"""
import datetime as _dt

from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case, DV77

_DATES = {"O0": "2026-11-26", "O1": "2026-11-28", "O2": "2026-12-01", "O3": "2026-12-02",
          "O4": "2026-11-20", "O5": "2026-12-02", "O6": "2026-12-03",
          "O7": "2026-10-14", "O8": "2026-12-02", "O9": "2026-12-03",
          "C0": "2026-11-20", "C1": "2026-12-02",
          "C2": "2026-11-20", "C3": "2026-12-01", "C4": "2026-12-02",
          "C5": "2026-11-20", "C6": "2026-12-02",
          "C7": "2026-11-20", "C8": "2026-12-02", "C9": "2026-12-03"}


def _d(label, delta=0):
    """Business date (+delta days) of a load of the NEW chains of this file (literal, see the module doc)."""
    return (_dt.date.fromisoformat(_DATES[label]) + _dt.timedelta(days=delta)).isoformat()


_WIN = "E/slave/loading/SnpDataLoader.java:181-205 (window; NEW: lead, others: lag; status :202-204)"
_HM = ("O/impl/loader/DataLoaderHistMix.java:66-69 (only 999912 enters the window, current+previous month returned as "
       "is), :75-81 (kept: NEW, NEQ&rn1, EQ&rn2, OLD; NEW -> start 1900), :84-89 (NEQ&rn2 closed at another_ctl_validfrom-2, "
       "dropped when end < start), :118 (p1month = yyyyMM(end_dt)), :126-139 (legs)")
_HS = ("E/slave/loading/DataLoaderHistSnp.java:70-73, :82-96 (actual: NEW, NEQ&rn1, EQ&rn2; NEW -> cache end+1 or 1900), "
       ":103-108 (OLD closed at current_date-1, NEQ&rn2 at another_ctl_validfrom-2), :119 (OLD kept, others dropped when "
       "end < start), :124, :139-153 (legs); E/slave/loading/LoaderCache.java:87-114")
_HD = ("E/slave/loading/DataLoaderHistDiff.java:69-72, :78-84 (kept: NEW, NEQ&rn1, EQ&rn2, OLD), :87-92 (NEQ&rn2 closed "
       "at another_ctl_validfrom-2), :97, :99 (INIT start 1900, ctl_validfrom = run), :115-126 (legs)")
_LK = ("O/impl/loader/DataLoaderLinkHistDiff.java:71-74, :80-86, :89-94 (NEQ&rn2 closed at another_ctl_validfrom-2), "
       ":99, :101 (INIT), :114-125 (legs)")
_MV = ("E/slave/move/HistMovingGenerator.java:31-32 and O/impl/mover/HistMovingNoCacheGenerator.java:34-35 (truncate "
       "p1month >= yyyyMM(now-1 month), older partitions are only appended to: E/slave/move/MovingTableService.java:44)")
_INIT = ("O/tables/chgdivh/ChgDivHSql.java:245 (d_ start = ctl_validfrom), :288 (snapshot start = current_date), :252 "
         "(UNION), :207 (lag(hash) by start_dt), :294 (collapse equal neighbours), :177 (end = lead(start_dt)-1), :297 "
         "(datediff(end, start) >= 0)")


def _pm(date_iso):
    return p1month(date_iso)


# ------------------------------------------------------------------------------------------------ chain main

def o01():
    c = case(id="O01", title="DV-78: INIT ties of lag/lead on start_dt in stg.chg_div_h (d_ twins with one ctl_validfrom, d_ at 00:00 next to m_)",
             rules=["S2-chgdivh-init", "R10", "L8-histmix-init"], changes=[], tables=["div_h"], deterministic=False,
             summary="DV-78 (E22 of local_dirty on the main stand). INIT SQL orders each fulldivcode only by start_dt: "
                     + _INIT + ". O01_1: two d_ rows with the SAME ctl_validfrom 2026-06-01 10:00 and different names: the "
                     "first in physical order gets end = start-1 and is cut by datediff >= 0, the second ends at INIT-1. "
                     "Which name survives depends on the scan order (3.0 HiveTableScan vs 4.0 FileScan): only dates are "
                     "checked; the closed name stays in div_h forever. O01_2: a d_ row at exactly 00:00 of the INIT day "
                     "(= current_date of the snapshot row, other name): one version [INIT, MAX] survives, its name is a "
                     "tie; at L1 3.0 either keeps it (EQUALS) or relabels it to [D-1 = INIT, MAX] (the old one closes at "
                     "D-2 < start and is dropped, " + _HM + "): only start/end/p1month are stable. INIT row "
                     "ctl_validfrom = start_dt: DataLoaderHistMix.java:108-110.")
    r = c.div("O01_1", fullname="O01 now")
    c.hist(r, "2026-06-01 10:00:00", 1, fullname="O01 a")
    c.hist(r, "2026-06-01 10:00:00", 2, fullname="O01 b")
    rows = [ver("2026-06-01", d("L0", -1), START, "L0", p1month="202701"), ver(d("L0"), MAX, START, "L0", p1month="999912")]
    c.expect("L0", "snp.div_h", "O01_1", rows, note="ONE closed version from the two d_ twins (name a or b: tie), not two")
    for ld in ("L3", "L7"):
        c.expect(ld, "snp.div_h", "O01_1", rows, note="unchanged; 202701 is below the rewrite zone at L7")
    r2 = c.div("O01_2", fullname="O01 now2")
    c.hist(r2, "2027-01-13 00:00:00", 1, fullname="O01 d0")
    c.expect("L0", "snp.div_h", "O01_2", [ver(d("L0"), MAX, START, "L0", p1month="999912")],
             note="one version [INIT, MAX]; name 'O01 now2' or 'O01 d0' (tie of equal start_dt)")
    stable = [dict(start_dt=d("L0"), end_dt=MAX, p1month="999912")]
    c.expect("L1", "snp.div_h", "O01_2", stable,
             note="name was the snapshot one: EQUALS, untouched; name was the d_ one: relabelled [D-1, MAX] with (R,I)+(U,D)")
    c.expect("L3", "snp.div_h", "O01_2", stable)
    c.note("L1", "O01_2 diff.div_h: no legs or (R,I)+(U,D) [INIT, MAX], depending on the INIT tie; not asserted")


def o02():
    c = case(id="O02", accepted=DV77, accept=["div_type:O02_1"], title="DV-85: div_type returner comes back as a PAIR (two types): rn1 starts D-1, rn2 cache end+1",
             rules=["R13", "R14", "R16", "H13-key-dedup"], changes=["HPWR-returner-start-date", "HPWR-vanish-divtype"],
             tables=["div_type"], deterministic=False,
             summary="DV-85. O02_1 type T1 until L2; L3 gone: closed [1900, L3-1] (OLD at current_date-1, key end cached). "
                     "L4 (Feb 1) returns with two snapshot rows of one fulldivcode, types TA and TB: in the window both are "
                     "NEW (tie), rn1 sees the other hash -> NOTEQUALS&rn1 -> start D-1 = Jan 31; rn2 has no lead -> stays NEW "
                     "-> returner start = cached end + 1 = Jan 15. " + _HS + "; " + _WIN + ". Which type gets which start is "
                     "a tie: types are not checked. If DV-85 is real, 4.0 (window30 for the pair) gives the NEW one 1900. "
                     "L5: TB is gone; the two stored open rows share ctl_loading (tie) -> not asserted. div_/div_h of the "
                     "pair (equal names) follow DV-01 and are not asserted here.")
    c.div("O02_1", label="O02_1#a", divisiontype00="O02_T1", at={"L3": GONE, "L4": dict(divisiontype00="O02_TA")})
    c.div("O02_1", label="O02_1#b", divisiontype00="O02_TB", start="L4", at={"L5": GONE})
    closed = ver(MIN, d("L3", -1), RUN("L3"), "L3", p1month="202701")
    c.expect("L3", "snp.div_type", "O02_1", [closed])
    c.expect("L3", "diff.div_type", "O02_1", [leg("R", "U", MIN, d("L3", -1), RUN("L3")), leg("U", "U", MIN, MAX, RUN("L3"))])
    c.expect("L4", "snp.div_type", "O02_1", [closed, ver(d("L4", -1), MAX, RUN("L4"), "L4", p1month="999912"),
                                            ver(d("L3"), MAX, RUN("L4"), "L4", p1month="999912")],
             note="rn1 (NOTEQUALS) [%s, MAX], rn2 (NEW, cached end %s) [%s, MAX]" % (d("L4", -1), d("L3", -1), d("L3")))
    c.expect("L4", "diff.div_type", "O02_1", [leg("R", "I", d("L4", -1), MAX, RUN("L4")), leg("U", "D", d("L4", -1), MAX, RUN("L4")),
                                             leg("R", "I", d("L3"), MAX, RUN("L4")), leg("U", "D", d("L3"), MAX, RUN("L4"))])
    c.note("L5", "O02_1 div_type: two open rows with one ctl_loading meet the input TA (tie) -> 1 or 2 rows; not asserted")


def o03():
    c = case(id="O03", title="DV-86: NEW&rn2 NOTEQUALS is CLOSED by 3.0 (future d_ version at INIT + twin snapshot rows at L1)",
             rules=["R10", "R05", "L9-histmix-incr", "H13-key-dedup"], changes=["HPWR-close-open-only", "HPWR-silent-drop"],
             tables=["div_h"], deterministic=False,
             summary="DV-86 (DV-01 x DV-06). L0: d_ versions 2026-06-01 and 2027-01-20 (after INIT): [Jun 1, Jan 12], "
                     "[Jan 13, Jan 19] (snapshot) in 202701 and the open future one [Jan 20, MAX] with ctl_validfrom = START "
                     "(DataLoaderHistMix.java:108-110). L1: two snapshot rows of O03_1 ('O03 now' and 'O03 twin'). Window: "
                     "NEW x (rn1, lead = the other NEW) NOTEQUALS -> [D-1, MAX]; NEW y (rn2, lead = the stored future row) "
                     "NOTEQUALS&rn2 -> closed at its ctl_validfrom - 2 = Jan 18 >= start D-1 = Jan 13 -> KEPT [Jan 13, Jan 18] "
                     "with (R,U)+(U,U); the stored future row is rn3 -> dropped without legs. " + _HM + "; " + _WIN + ". "
                     "x/y order is a tie: names are not checked. 202701 then holds three rows starting Jan 13. If DV-86 "
                     "is real, 4.0 (closing only SRC=OLD) drops y and its legs. L2: the twin is gone; the open row is "
                     "kept or relabelled to [Jan 13, MAX] (tie) - start/end/p1month are stable.")
    r = c.div("O03_1", fullname="O03 now")
    c.div("O03_1", label="O03_1#twin", fullname="O03 twin", start="L1", at={"L2": GONE})
    c.hist(r, "2026-06-01 10:00:00", 1, fullname="O03 v1")
    c.hist(r, "2027-01-20 10:00:00", 2, fullname="O03 fut")
    init = [ver("2026-06-01", d("L0", -1), START, "L0", p1month="202701"), ver(d("L0"), "2027-01-19", START, "L0", p1month="202701")]
    c.expect("L0", "snp.div_h", "O03_1", init + [ver("2027-01-20", MAX, START, "L0", p1month="999912")])
    c.expect("L1", "snp.div_h", "O03_1", init + [ver(d("L1", -1), "2027-01-18", RUN("L1"), "L1", p1month="202701"),
                                                ver(d("L1", -1), MAX, RUN("L1"), "L1", p1month="999912")],
             note="NEW rn2 closed at the future version's ctl_validfrom - 2; the future version itself is dropped (rn3)")
    c.expect("L1", "diff.div_h", "O03_1", [leg("R", "I", d("L1", -1), MAX, RUN("L1")), leg("U", "D", d("L1", -1), MAX, RUN("L1")),
                                          leg("R", "U", d("L1", -1), "2027-01-18", RUN("L1")), leg("U", "U", d("L1", -1), MAX, RUN("L1"))])
    stable = [dict(start_dt="2026-06-01", end_dt=d("L0", -1), p1month="202701"), dict(start_dt=d("L0"), end_dt="2027-01-19", p1month="202701"),
              dict(start_dt=d("L1", -1), end_dt="2027-01-18", p1month="202701"), dict(start_dt=d("L1", -1), end_dt=MAX, p1month="999912")]
    for ld in ("L2", "L3", "L7"):
        c.expect(ld, "snp.div_h", "O03_1", stable, note="202701 rows untouched (rewritable, then below the zone at L7)")
    c.note("L2", "O03_1 diff.div_h: none (open name was 'O03 now') or (R,I)+(U,D) [D-1, MAX] (it was the twin); not asserted")


# ------------------------------------------------------------------------------------------------ chain dhist

def o04():
    c = case(id="O04", chain="dhist",
             title="DV-83/DV-82(a)/DV-86: 3.0 itself writes a CLOSED version into 999912 with the PK of the open one; 4.0 continues",
             rules=["R10", "R05", "R07", "R11", "L9-histmix-incr", "M2-loader-mover-window-contract"],
             changes=["HPWR-close-open-only", "HPWR-silent-drop", "HPWR-p1month-recalc"], tables=["div_h"], deterministic=False,
             summary="O0 (3.0 INIT): O04_1/O04_2 exist ONLY in the d_ history (no snapshot row; INIT does not join to the "
                     "snapshot: " + _INIT + ") with one version dated 9999-12-20 -> [9999-12-20, MAX], ctl_validfrom START. "
                     "O1 (3.0 INCR): the division appears twice in the snapshot (names X, Y). Window: rn1 NEW NOTEQUALS "
                     "-> [D-1, MAX]; rn2 NEW NOTEQUALS (lead = the stored row) -> closed at 9999-12-20 - 2 = 9999-12-18 >= D-1 "
                     "-> [Nov 27, 9999-12-18] with p1month 999912 (yyyyMM(end)); the stored row (rn3) is dropped. So 3.0 "
                     "leaves in 999912 an open and a CLOSED version with one PK (div_sk, Nov 27), both ctl_loading O1. "
                     + _HM + "; " + _WIN + ". 4.0 takes over after O1. O2: O04_1 gets ONE row with a new name W: rn1 NEW "
                     "NOTEQUALS [D-1, MAX]; rn2 (the open or the closed stored row, tie) NOTEQUALS&rn2 -> closed at D-2 "
                     "(Nov 29 >= Nov 27) with (R,U)+(U,U) (U end MAX); rn3 dropped - dates equal for both orders. O04_2 "
                     "is gone at O2: rn1 kept as is, rn2 closes at RUN@O1-2 = Nov 26 < Nov 27 and is dropped: one row "
                     "start Nov 27 (end MAX or 9999-12-18: tie), no legs. DV-83: the vendor does not close a 999912 row "
                     "with end >= D-1 the 3.0 way; DV-82(a): openIdx counts every 999912 row as open.")
    for k in ("O04_1", "O04_2"):
        at_b = {"O2": GONE}
        at_a = {"O2": dict(fullname="O04 W")} if k == "O04_1" else {"O2": GONE}
        r = c.div(k, label=k + "#x", fullname="O04 X", start="O1", at=at_a)
        c.div(k, label=k + "#y", fullname="O04 Y", start="O1", at=at_b)
        c.hist(r, "9999-12-20 12:00:00", 1, fullname="O04 fut")
        c.expect("O0", "snp.div_h", k, [ver("9999-12-20", MAX, START, "O0", div_name="O04 fut", p1month="999912")],
                 note="a d_-only key gets a div_h version at INIT")
        pair = [ver(_d("O1", -1), MAX, RUN("O1"), "O1", p1month="999912"),
                ver(_d("O1", -1), "9999-12-18", RUN("O1"), "O1", p1month="999912")]
        c.expect("O1", "snp.div_h", k, pair, note="open + closed version with one PK, both in 999912 (names X/Y: tie)")
        c.expect("O1", "diff.div_h", k, [leg("R", "I", _d("O1", -1), MAX, RUN("O1")), leg("U", "D", _d("O1", -1), MAX, RUN("O1")),
                                        leg("R", "U", _d("O1", -1), "9999-12-18", RUN("O1")), leg("U", "U", _d("O1", -1), MAX, RUN("O1"))])
    after = [ver(_d("O1", -1), _d("O2", -2), RUN("O2"), "O2", p1month=_pm(_d("O2", -2))),
             ver(_d("O2", -1), MAX, RUN("O2"), "O2", p1month="999912")]
    c.expect("O2", "snp.div_h", "O04_1", after,
             note="the other stored row (rn3) is dropped without legs; the closed name is X or Y (tie), the open one is W")
    c.expect("O2", "diff.div_h", "O04_1", [leg("R", "I", _d("O2", -1), MAX, RUN("O2")), leg("U", "D", _d("O2", -1), MAX, RUN("O2")),
                                          leg("R", "U", _d("O1", -1), _d("O2", -2), RUN("O2")), leg("U", "U", _d("O1", -1), MAX, RUN("O2"))])
    c.expect("O3", "snp.div_h", "O04_1", after, note="EQUALS: unchanged")
    c.expect("O3", "diff.div_h", "O04_1", [])
    one = [dict(start_dt=_d("O1", -1), ctl_validfrom=RUN("O1"), ctl_loading="O1", p1month="999912")]
    for ld in ("O2", "O3"):
        c.expect(ld, "snp.div_h", "O04_2", one, note="one of the two stored rows survives (end MAX or 9999-12-18: tie)")
        c.expect(ld, "diff.div_h", "O04_2", [], note="rn2 closes at %s < its start and is dropped silently" % _d("O1", -2))


def o05():
    c = case(id="O05", accepted=DV77, accept=["div_type:O05_1"], chain="dhist", title="DV-85/DV-28: key vanished under 3.0 (loader cache), returns as a PAIR on the first 4.0 load",
             rules=["R13", "R14", "R16"], changes=["HPWR-returner-start-date", "HPWR-vanish-divtype"], tables=["div_type"],
             deterministic=False,
             summary="O0 (3.0 INIT) type T1; O1 (3.0) gone: closed [1900, Nov 27], the end is written to 3.0's "
                     "stg.loader_cache_div_type (copied with the 3.0 root to 4.0). O2 (first 4.0 load): two snapshot rows, "
                     "types TA/TB: rn1 NOTEQUALS -> [D-1 = Nov 30, MAX], rn2 NEW -> cached end + 1 = Nov 28. " + _HS + "; "
                     + _WIN + ". 4.0 reads no cache (closed history scan, DV-28) and, by DV-85, gives the window's NEW row "
                     "1900. Types are a tie (not checked). O3: TB gone, two open rows with one ctl_loading (tie): not asserted.")
    c.div("O05_1", label="O05_1#a", divisiontype00="O05_T1", at={"O1": GONE, "O2": dict(divisiontype00="O05_TA")})
    c.div("O05_1", label="O05_1#b", divisiontype00="O05_TB", start="O2", at={"O3": GONE})
    closed = ver(MIN, _d("O1", -1), RUN("O1"), "O1", p1month=_pm(_d("O1", -1)))
    c.expect("O1", "snp.div_type", "O05_1", [closed])
    c.expect("O2", "snp.div_type", "O05_1", [closed, ver(_d("O2", -1), MAX, RUN("O2"), "O2", p1month="999912"),
                                            ver(_d("O1"), MAX, RUN("O2"), "O2", p1month="999912")])
    c.expect("O2", "diff.div_type", "O05_1", [leg("R", "I", _d("O2", -1), MAX, RUN("O2")), leg("U", "D", _d("O2", -1), MAX, RUN("O2")),
                                             leg("R", "I", _d("O1"), MAX, RUN("O2")), leg("U", "D", _d("O1"), MAX, RUN("O2"))])
    c.note("O3", "O05_1 div_type: two open rows of O2 (tie) meet the input TA -> 1 or 2 rows; not asserted")


# ------------------------------------------------------------------------------------------------ twins written by the 3.0 INIT

def _twins(c, pre, ld_init, gone_at, all_gone=False):
    """INIT twins with one PK, all [1900, MAX] RUN@INIT: div_type (two types of one fulldivcode), addr_h (legal addresses
    colliding through '|'), div_div_relation (source|successor colliding through '|'); everything vanishes at gone_at.
    Returns the keys."""
    k_type = pre + "_1"
    c.div(k_type, label=k_type + "#a", divisiontype00=pre + "_TA", at={gone_at: GONE})
    c.div(k_type, label=k_type + "#b", divisiontype00=pre + "_TB", at={gone_at: GONE})
    low = pre.lower()
    c.div(pre + "_2", address_addrfact_fiascode=low + "a|b", address_legaladdrdoc=pre + "c", at={gone_at: GONE})
    c.div(pre + "_3", address_addrfact_fiascode=low + "a", address_legaladdrdoc="b|" + pre + "c", at={gone_at: GONE})
    k_addr = addr_key(durty=low + "a|b", src=pre + "c")
    assert k_addr == addr_key(durty=low + "a", src="b|" + pre + "c")
    A, B, C = pre + "_A", pre + "_B", pre + "_C"
    for k in (A, B + "|" + C, A + "|" + B, C):
        c.div(k, at={gone_at: GONE} if all_gone else {})
    c.succ(A, B + "|" + C, "правопреемник", at={gone_at: GONE})
    c.succ(A + "|" + B, C, "правопреемник", at={gone_at: GONE})
    k_link = "|".join((A, B, C))
    init = [ver(MIN, MAX, RUN(ld_init), ld_init, p1month="999912")] * 2
    c.expect(ld_init, "snp.div_type", k_type, [ver(MIN, MAX, RUN(ld_init), ld_init, div_type_terr_div_type_sk=pre + "_TA", p1month="999912"),
                                              ver(MIN, MAX, RUN(ld_init), ld_init, div_type_terr_div_type_sk=pre + "_TB", p1month="999912")],
             note="INIT writes both types: one PK (div_sk, 1900) twice (no dedup, DV-01)")
    c.expect(ld_init, "snp.addr_h", k_addr, [ver(MIN, MAX, RUN(ld_init), ld_init, standard_addr_txt=pre + "c", p1month="999912"),
                                            ver(MIN, MAX, RUN(ld_init), ld_init, standard_addr_txt="b|" + pre + "c", p1month="999912")],
             note="two legal addresses with one addr_sk (AddrTransformer.java:248-263), different text")
    c.expect(ld_init, "snp.div_div_relation", k_link, init, note="two relations with one key, different source/successor sk")
    return k_type, k_addr, k_link


def _twins_gone(c, k_type, k_addr, k_link, ld_init, ld_inc, ld_next, rn2_end):
    """Expectations when the twins' key has NO input at ld_inc (the first INCR after the 3.0 INIT)."""
    t = [ver(MIN, _d(ld_inc, -1), RUN(ld_inc), ld_inc, p1month=_pm(_d(ld_inc, -1))),
         ver(MIN, rn2_end, RUN(ld_inc), ld_inc, p1month=_pm(rn2_end))]
    tlegs = [leg("R", "U", MIN, _d(ld_inc, -1), RUN(ld_inc)), leg("U", "U", MIN, MAX, RUN(ld_inc)),
             leg("R", "U", MIN, rn2_end, RUN(ld_inc)), leg("U", "U", MIN, MAX, RUN(ld_inc))]
    c.expect(ld_inc, "snp.div_type", k_type, t,
             note="rn1 OLD closed at current_date-1, rn2 NOTEQUALS closed at ctl_validfrom(rn1)-2 = %s; types: tie" % rn2_end)
    c.expect(ld_inc, "diff.div_type", k_type, tlegs)
    h = [ver(MIN, MAX, RUN(ld_init), ld_init, p1month="999912"), ver(MIN, rn2_end, RUN(ld_inc), ld_inc, p1month=_pm(rn2_end))]
    hlegs = [leg("R", "U", MIN, rn2_end, RUN(ld_inc)), leg("U", "U", MIN, MAX, RUN(ld_inc))]
    for tb, k in (("addr_h", k_addr), ("div_div_relation", k_link)):
        c.expect(ld_inc, "snp." + tb, k, h, note="rn1 OLD stays open, rn2 NOTEQUALS closed at ctl_validfrom(rn1)-2 = %s" % rn2_end)
        c.expect(ld_inc, "diff." + tb, k, hlegs)
        c.expect(ld_next, "snp." + tb, k, h, note="one open row left: nothing to do")
        c.expect(ld_next, "diff." + tb, k, [])
    c.expect(ld_next, "snp.div_type", k_type, t, note="closed rows untouched")
    c.expect(ld_next, "diff.div_type", k_type, [])


def o06():
    c = case(id="O06", accepted=DV77, accept=["addr_h:o06a|b|O06c|NULL|NULL"], chain="dtwin",
             title="DV-79 (+DV-03): twin PKs written by the 3.0 INIT (div_type, addr_h, div_div_relation), key without input, closure IN the zone",
             rules=["R13", "R16", "R07", "L5-link-incr-outcomes", "H13-key-dedup", "M2-loader-mover-window-contract"],
             changes=["HPWR-vanish-divtype", "HPWR-silent-drop"], tables=["div_type", "addr_h", "div_div_relation"],
             deterministic=False,
             summary="O4 (3.0 INIT, Nov 20) writes twins with one PK, all [1900, MAX] RUN@O4: div_type of O06_1 (types TA, "
                     "TB; O/tables/divtype/DivTypeTransformer.java:33-43 distinct; DataLoaderHistSnp.java:126-127 INIT), addr_h of the "
                     "legal addresses of O06_2/O06_3 colliding through '|' (AddrHTransformer.java:46-66, "
                     "AddrTransformer.java:248-263) and div_div_relation 'O06_A|O06_B|O06_C' from two successor rows "
                     "(DivDivRelationTransformer.java:33-62). O5 (Dec 2, first 4.0 load): everything is gone from the "
                     "source. 3.0 window over 999912 only: rn1 (tie) OLD, rn2 NOTEQUALS (lag = rn1). div_type: rn1 closed "
                     "at current_date-1 = Dec 1, rn2 at RUN@O4-2 = Nov 18 (" + _HS + "). addr_h / relation: rn1 stays "
                     "open, rn2 closed [1900, Nov 18] with (R,U)+(U,U) (" + _HD + "; " + _LK + "). Nov 18 is in 202611 = "
                     "previous month, inside the rewrite zone. " + _WIN + ": which twin is rn1 is a tie, so types and "
                     "texts are not checked after O4. If DV-79 is real, 4.0 (winKeys only from keys with input for "
                     "addr_h/relation) keeps both rows open forever; div_type goes through window30 (withInput=false).")
    k_type, k_addr, k_link = _twins(c, "O06", "O4", "O5")
    _twins_gone(c, k_type, k_addr, k_link, "O4", "O5", "O6", _d("O4", -2))


def o07():
    c = case(id="O07", chain="dtwin",
             title="DV-03: twin PKs written by the 3.0 INIT (div_type, div_div_relation) meet ONE different input row",
             rules=["R13", "R16", "L5-link-incr-outcomes", "H13-key-dedup"], changes=["HPWR-close-date-loadDate-2"],
             tables=["div_type", "div_div_relation"], deterministic=False,
             summary="DV-03 on PERIOD tables with input. O4 (3.0 INIT): div_type O07_1 types TA/TB, relation "
                     "'O07_E|O07_F|O07_G|O07_H' from successors E->F|G|H and E|F->G|H: twins [1900, MAX]. O5: the input "
                     "has ONE row that differs from both (type TC; successor E|F|G->H). Window: NEW rn1 NOTEQUALS -> "
                     "[D-1, MAX]; OLD rn2 (tie) NOTEQUALS&rn2 -> closed at the NEW ctl_validfrom - 2 = Nov 30 with "
                     "(R,U)+(U,U); OLD rn3 dropped without legs - same dates whichever twin is rn2 (" + _WIN + "; " + _HS
                     + "; " + _LK + "). With an input EQUAL to one twin the 3.0 result depends on the tie (G01_6, DV-77) "
                     "and is not used here. If DV-03 is real, 4.0 multiplies or keeps the twins. O6: input unchanged -> EQUALS.")
    k = "O07_1"
    c.div(k, label=k + "#a", divisiontype00="O07_TA", at={"O5": dict(divisiontype00="O07_TC")})
    c.div(k, label=k + "#b", divisiontype00="O07_TB", at={"O5": GONE})
    E, F, G, H = "O07_E", "O07_F", "O07_G", "O07_H"
    for x in (E, "|".join((F, G, H)), E + "|" + F, G + "|" + H, "|".join((E, F, G)), H):
        c.div(x)
    c.succ(E, "|".join((F, G, H)), "правопреемник", at={"O5": GONE})
    c.succ(E + "|" + F, G + "|" + H, "правопреемник", at={"O5": GONE})
    c.succ("|".join((E, F, G)), H, "правопреемник", start="O5")
    kl = "|".join((E, F, G, H))
    c.expect("O4", "snp.div_type", k, [ver(MIN, MAX, RUN("O4"), "O4", p1month="999912")] * 2)
    c.expect("O4", "snp.div_div_relation", kl, [ver(MIN, MAX, RUN("O4"), "O4", p1month="999912")] * 2)
    for tb, kk in (("div_type", k), ("div_div_relation", kl)):
        rows = [ver(MIN, _d("O5", -2), RUN("O5"), "O5", p1month=_pm(_d("O5", -2))), ver(_d("O5", -1), MAX, RUN("O5"), "O5", p1month="999912")]
        c.expect("O5", "snp." + tb, kk, rows, note="one twin closed at D-2, the other dropped without legs, the input opens at D-1")
        c.expect("O5", "diff." + tb, kk, [leg("R", "I", _d("O5", -1), MAX, RUN("O5")), leg("U", "D", _d("O5", -1), MAX, RUN("O5")),
                                         leg("R", "U", MIN, _d("O5", -2), RUN("O5")), leg("U", "U", MIN, MAX, RUN("O5"))])
        c.expect("O6", "snp." + tb, kk, rows, note="EQUALS: unchanged")
        c.expect("O6", "diff." + tb, kk, [])


def o08():
    c = case(id="O08", chain="dtwin",
             title="DV-06 on a 3.0 target: versions in FUTURE sections (202701, 999901) written by the 3.0 INIT are lost on the first INCR",
             rules=["R10", "R05", "M1-hist-mover-truncation", "M2-loader-mover-window-contract"], changes=["FX26-rewrite-scope"],
             tables=["div_h"],
             summary="DV-06 (accepted) on dirt left by 3.0. O4 (3.0 INIT, Nov 20): d_ versions 2026-06-01, 2027-01-10 and "
                     "9999-01-02 after the snapshot: [Jun 1, Nov 19] (202611), [Nov 20, 2027-01-09] (202701, future), "
                     "[2027-01-10, 9999-01-01] (999901, the 'eternal open' one) and [9999-01-02, MAX] (999912) - "
                     + _INIT + ". O5 (Dec 2, first 4.0 load, snapshot unchanged): the window sees only the 999912 row: "
                     "NEW NOTEQUALS&rn1 -> [Dec 1, MAX] with (R,I)+(U,D); the stored row would close at Nov 30 < its start "
                     "and is dropped (" + _HM + "). The mover truncates every partition >= 202611 (" + _MV + ") and the "
                     "loader restages only 999912, 202612 and 202611: 202701 and 999901 are lost. 4.0 keeps them (DV-06, "
                     "accepted; expectation in 3.0 terms). O6: EQUALS.")
    r = c.div("O08_1", fullname="O08 now")
    c.hist(r, "2026-06-01 10:00:00", 1, fullname="O08 v1")
    c.hist(r, "2027-01-10 10:00:00", 2, fullname="O08 x1")
    c.hist(r, "9999-01-02 10:00:00", 3, fullname="O08 x2")
    v1 = ver("2026-06-01", _d("O4", -1), START, "O4", div_name="O08 v1", p1month=_pm(_d("O4", -1)))
    c.expect("O4", "snp.div_h", "O08_1", [v1, ver(_d("O4"), "2027-01-09", START, "O4", div_name="O08 now", p1month="202701"),
                                         ver("2027-01-10", "9999-01-01", START, "O4", div_name="O08 x1", p1month="999901"),
                                         ver("9999-01-02", MAX, START, "O4", div_name="O08 x2", p1month="999912")])
    rows = [v1, ver(_d("O5", -1), MAX, RUN("O5"), "O5", div_name="O08 now", p1month="999912")]
    c.expect("O5", "snp.div_h", "O08_1", rows, note="202701 and 999901 truncated by the 3.0 mover and not restaged")
    c.expect("O5", "diff.div_h", "O08_1", [leg("R", "I", _d("O5", -1), MAX, RUN("O5"), div_name="O08 now"),
                                          leg("U", "D", _d("O5", -1), MAX, RUN("O5"), div_name="O08 now")])
    c.expect("O6", "snp.div_h", "O08_1", rows)
    c.expect("O6", "diff.div_h", "O08_1", [])


# ------------------------------------------------------------------------------------------------ chain dtwinold

def o09():
    c = case(id="O09", chain="dtwinold",
             title="DV-80: twin PKs written by the 3.0 INIT, key without input, closure BELOW the rewrite zone (202610)",
             rules=["R13", "R16", "L5-link-incr-outcomes", "M1-hist-mover-truncation", "M2-loader-mover-window-contract"],
             changes=["HPWR-vanish-divtype", "FX26-rewrite-scope"], tables=["div_type", "addr_h", "div_div_relation"],
             deterministic=False,
             summary="DV-80. Same twins as O06 but the 3.0 INIT runs on Oct 14 and the first 4.0 load on Dec 2: rn2 closes "
                     "at RUN@O7-2 = Oct 12, partition 202610 below the zone (cut point 202611). 3.0 writes the closed row "
                     "into 202610 (only partitions >= the cut point are truncated, older ones are appended: " + _MV + "). "
                     "div_type: [1900, Dec 1] (202612) + [1900, Oct 12] (202610), four legs (" + _HS + "); addr_h and "
                     "relation: rn1 stays open, [1900, Oct 12] (" + _HD + "; " + _LK + "). " + _WIN + ": types/texts are a "
                     "tie and not checked. If DV-80 is real, 4.0 hands the group to the vendor: VANISH closes every open "
                     "div_type twin at D-1 and the PK merge gives N x N rows; addr_h/relation stay untouched (DV-79).")
    k_type, k_addr, k_link = _twins(c, "O09", "O7", "O8")
    _twins_gone(c, k_type, k_addr, k_link, "O7", "O8", "O9", _d("O7", -2))


# ------------------------------------------------------------------------------------------------ chain dnonmono

def o10():
    c = case(id="O10", chain="dnonmono",
             title="DV-13 on PERIOD tables: 3.0 twins (and single keys) with ctl_loading ABOVE the INCR's loadingId",
             rules=["R13", "R16", "L5-link-incr-outcomes", "H13-key-dedup"], changes=["HPWR-new-key-start-1900"],
             tables=["div_type", "addr_h", "div_div_relation", "div_h"], deterministic=False,
             summary="DV-13 (accepted, expectation in 3.0 terms). C0 (3.0 INIT, loadingId 20190) writes twins as in O06; "
                     "C1 (first 4.0 load, loadingId 20180 < 20190) keeps ONE of them in the input. The window orders "
                     "desc(ctl_loading): both stored twins come FIRST (tie), the input row last (" + _WIN + "). div_type "
                     "O10_1 (input TA): rn1 OLD closed at current_date-1 = Dec 1, rn2 NOTEQUALS closed at RUN@C0-2 = Nov 18, "
                     "the input (rn3, no lead) stays NEW -> [1900, MAX] (no cache after INIT): three rows, six legs (" + _HS
                     + "). addr_h of O10_2/O10_3 (O10_3 gone) and relation 'O10_A|O10_B|O10_C' (one successor row gone): rn1 "
                     "OLD stays open with its C0 stamps, rn2 closed [1900, Nov 18], the input NEW -> another [1900, MAX]: "
                     "three rows with PK (key, 1900) (" + _HD + "; " + _LK + "). div_h of O10_4 (unchanged): the stored "
                     "version stays (rn1 OLD) and the input becomes NEW [1900, MAX] with (R,I)+(U,D) (" + _HM + "). "
                     "Closed twins' types/texts are a tie and not checked.")
    k = "O10_1"
    c.div(k, label=k + "#a", divisiontype00="O10_TA")
    c.div(k, label=k + "#b", divisiontype00="O10_TB", at={"C1": GONE})
    c.div("O10_2", address_addrfact_fiascode="o10a|b", address_legaladdrdoc="O10c")
    c.div("O10_3", address_addrfact_fiascode="o10a", address_legaladdrdoc="b|O10c", at={"C1": GONE})
    ka = addr_key(durty="o10a|b", src="O10c")
    assert ka == addr_key(durty="o10a", src="b|O10c")
    A, B, C = "O10_A", "O10_B", "O10_C"
    for x in (A, B + "|" + C, A + "|" + B, C):
        c.div(x)
    c.succ(A, B + "|" + C, "правопреемник")
    c.succ(A + "|" + B, C, "правопреемник", at={"C1": GONE})
    kl = "|".join((A, B, C))
    c.div("O10_4", fullname="O10 single")
    c.expect("C0", "snp.div_type", k, [ver(MIN, MAX, RUN("C0"), "C0", p1month="999912")] * 2)
    c.expect("C1", "snp.div_type", k, [ver(MIN, _d("C1", -1), RUN("C1"), "C1", p1month=_pm(_d("C1", -1))),
                                      ver(MIN, _d("C0", -2), RUN("C1"), "C1", p1month=_pm(_d("C0", -2))),
                                      ver(MIN, MAX, RUN("C1"), "C1", p1month="999912")],
             note="the stored twins rank above the input: both closed (OLD today-1, NOTEQUALS rn2 at C0-2), input NEW at 1900")
    c.expect("C1", "diff.div_type", k, [leg("R", "U", MIN, _d("C1", -1), RUN("C1")), leg("U", "U", MIN, MAX, RUN("C1")),
                                       leg("R", "U", MIN, _d("C0", -2), RUN("C1")), leg("U", "U", MIN, MAX, RUN("C1")),
                                       leg("R", "I", MIN, MAX, RUN("C1")), leg("U", "D", MIN, MAX, RUN("C1"))])
    for tb, kk in (("addr_h", ka), ("div_div_relation", kl)):
        c.expect("C0", "snp." + tb, kk, [ver(MIN, MAX, RUN("C0"), "C0", p1month="999912")] * 2)
        c.expect("C1", "snp." + tb, kk, [ver(MIN, MAX, RUN("C0"), "C0", p1month="999912"),
                                         ver(MIN, _d("C0", -2), RUN("C1"), "C1", p1month=_pm(_d("C0", -2))),
                                         ver(MIN, MAX, RUN("C1"), "C1", p1month="999912")],
                 note="rn1 OLD (C0) kept, rn2 closed, the input (rn3, NEW) opens at 1900: three rows with PK (key, 1900)")
        c.expect("C1", "diff." + tb, kk, [leg("R", "U", MIN, _d("C0", -2), RUN("C1")), leg("U", "U", MIN, MAX, RUN("C1")),
                                          leg("R", "I", MIN, MAX, RUN("C1")), leg("U", "D", MIN, MAX, RUN("C1"))])
    c.expect("C1", "snp.div_h", "O10_4", [ver(_d("C0"), MAX, START, "C0", div_name="O10 single", p1month="999912"),
                                         ver(MIN, MAX, RUN("C1"), "C1", div_name="O10 single", p1month="999912")],
             note="unchanged key: the stored version (higher ctl_loading) stays, the input is NEW at 1900")
    c.expect("C1", "diff.div_h", "O10_4", [leg("R", "I", MIN, MAX, RUN("C1"), div_name="O10 single"),
                                          leg("U", "D", MIN, MAX, RUN("C1"), div_name="O10 single")])


# ------------------------------------------------------------------------------------------------ chain dnull

def o11():
    c = case(id="O11", accepted=DV77, accept=["div_h:\\N", "div_type:\\N"], chain="dnull",
             title="DV-81: NULL-key PERIOD (div_h, div_type) on INCR: three snapshot rows without fulldivcode -> 3.0 keeps ONE version",
             rules=["R04", "R07", "R13", "R16", "H13-key-dedup"], changes=["HPWR-silent-drop"], tables=["div_h", "div_type"],
             deterministic=False,
             summary="DV-81. C2 (3.0 INIT): one division without fulldivcode ('O11 A', O11_TA): div_h [Nov 20, MAX] START, "
                     "div_type [1900, MAX]. C3 (first 4.0 load): it is gone, three others without fulldivcode arrive (names "
                     "B/C/D, types TB/TC/TD). 3.0 window over the NULL partition (" + _WIN + "): NEW rn1 NOTEQUALS -> "
                     "[D-1, MAX] with (R,I)+(U,D); NEW rn2 NOTEQUALS&rn2 -> closed at D-2 < D-1 and dropped; NEW rn3 and the "
                     "stored row (rn4) are NOTEQUALS beyond rn2 -> dropped WITHOUT legs, so the INIT version disappears "
                     "(" + _HM + "; " + _HS + "). C4 (same three rows): the same again -> one row [D-1, MAX], relabelled. "
                     "Which name/type survives is a tie: not checked. If DV-81 is real, 4.0 (PERIOD_NULL_KEY outside the "
                     "window) writes three open NULL versions with one PK and closes the old one with legs.")
    c.div("O11_1", fulldivcode=None, fullname="O11 A", divisiontype00="O11_TA", at={"C3": GONE})
    for i, n in ((2, "B"), (3, "C"), (4, "D")):
        c.div("O11_%d" % i, fulldivcode=None, fullname="O11 %s" % n, divisiontype00="O11_T%s" % n, start="C3")
    c.owns(None, "NULL fulldivcode of O11_1..O11_4 (chain dnull)")
    c.expect("C2", "snp.div_h", None, [ver(_d("C2"), MAX, START, "C2", div_name="O11 A", p1month="999912")])
    c.expect("C2", "snp.div_type", None, [ver(MIN, MAX, RUN("C2"), "C2", div_type_terr_div_type_sk="O11_TA", p1month="999912")])
    for ld in ("C3", "C4"):
        for tb in ("div_h", "div_type"):
            c.expect(ld, "snp." + tb, None, [ver(_d(ld, -1), MAX, RUN(ld), ld, p1month="999912")],
                     note="one NULL version; the previous one and two of the three inputs vanish without legs")
            c.expect(ld, "diff." + tb, None, [leg("R", "I", _d(ld, -1), MAX, RUN(ld)), leg("U", "D", _d(ld, -1), MAX, RUN(ld))])


# ------------------------------------------------------------------------------------------------ chain dyear

def o12():
    c = case(id="O12", chain="dyear",
             title="DV-65 on a 3.0 target: p1month '20101' (year < 1000) and 189912 written by the 3.0 INIT, kept by the next INCR",
             rules=["R10", "S2-chgdivh-init", "M1-hist-mover-truncation"], changes=["HPWR-p1month-recalc"], tables=["div_h"],
             summary="DV-65 (class D, recorded). C5 (3.0 INIT): O12_1 has d_ versions 0201-01-05 and 0201-01-19 (12:00, "
                     "inside the window 0200-03-01..0300-02-28 where Julian = Gregorian, and away from the Moscow LMT "
                     "offset): [0201-01-05, 0201-01-18] gets p1month = concat(year, lpad(month, 2)) = '201'||'01' = '20101' "
                     "(5 characters: E/utils/Expression.java:65-72, DataLoaderHistMix.java:118); O12_2 has 1899-11-01 and "
                     "1900-01-01 -> [1899-11-01, 1899-12-31] in 189912 (as W01). Both partitions are below every rewrite "
                     "zone (" + _MV + "; '20101' < '202611' as text and as number). C6 (first 4.0 load, snapshot "
                     "unchanged, EQUALS): 3.0 leaves both untouched. 4.0 reads 3.0's non-canonical partition here; its own "
                     "INIT would write 190001 (V/dto/SectionBy.java:162-166, DV-65).")
    r1 = c.div("O12_1", fullname="O12 now")
    c.hist(r1, "0201-01-05 12:00:00", 1, fullname="O12 a")
    c.hist(r1, "0201-01-19 12:00:00", 2, fullname="O12 b")
    r2 = c.div("O12_2", fullname="O12 now2")
    c.hist(r2, "1899-11-01 12:00:00", 1, fullname="O12 c")
    c.hist(r2, "1900-01-01 12:00:00", 2, fullname="O12 d")
    rows1 = [ver("0201-01-05", "0201-01-18", START, "C5", div_name="O12 a", p1month="20101"),
             ver("0201-01-19", _d("C5", -1), START, "C5", div_name="O12 b", p1month=_pm(_d("C5", -1))),
             ver(_d("C5"), MAX, START, "C5", div_name="O12 now", p1month="999912")]
    rows2 = [ver("1899-11-01", "1899-12-31", START, "C5", div_name="O12 c", p1month="189912"),
             ver("1900-01-01", _d("C5", -1), START, "C5", div_name="O12 d", p1month=_pm(_d("C5", -1))),
             ver(_d("C5"), MAX, START, "C5", div_name="O12 now2", p1month="999912")]
    for ld in ("C5", "C6"):
        c.expect(ld, "snp.div_h", "O12_1", rows1, note="year 201: p1month '20101' (five characters)")
        c.expect(ld, "snp.div_h", "O12_2", rows2)
    c.expect("C6", "diff.div_h", "O12_1", [])
    c.expect("C6", "diff.div_h", "O12_2", [])


def o13():
    c = case(id="O13", chain="dtwinempty",
             title="DV-79 on an EMPTY INCR: twin PKs written by the 3.0 INIT (div_type, addr_h, div_div_relation), every source empty",
             rules=["R13", "R16", "R07", "L5-link-incr-outcomes", "H13-key-dedup", "M2-loader-mover-window-contract"],
             changes=["HPWR-vanish-divtype", "HPWR-silent-drop"], tables=["div_type", "addr_h", "div_div_relation"],
             deterministic=False,
             summary="As O06, but the chain holds only this case, so C8 (Dec 2, first 4.0 load) is an INCR with every "
                     "source empty. 3.0 has no early exit (E/slave/check/EmptyChecker.java:13-16): the window runs over "
                     "999912 alone, with the same outcome as O06. addr_h / relation: rn1 OLD stays open, rn2 closed "
                     "[1900, Nov 18] with (R,U)+(U,U) (" + _HD + "; " + _LK + "); div_type: " + _HS + ". " + _WIN + ". "
                     "4.0 needs the empty-INCR marker for PERIOD (W/compat/CompatHooks.emptyIncrMarker, DV-79), else the "
                     "vendor exits early (V/PublisherImpl.java:230-235) and both rows stay open. C9: empty again, nothing to do.")
    k_type, k_addr, k_link = _twins(c, "O13", "C7", "C8", all_gone=True)
    _twins_gone(c, k_type, k_addr, k_link, "C7", "C8", "C9", _d("C7", -2))


for _f in [o01, o02, o03, o04, o05, o06, o07, o08, o09, o10, o11, o12, o13]:
    _f()
