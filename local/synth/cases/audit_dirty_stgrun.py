"""Audit phase 4.3, dirty paths, theme stgrun (stg, write, reruns on dirt): DV-08 DV-28 (cases), DV-17 DV-19 DV-22
DV-24 DV-25 DV-34 DV-47 DV-51 DV-73 DV-84 DV-89..DV-95 (not expressible here: IFT). Case letter S.

Paths in summaries: O/ = 3.0 vspc_idl_old/src/main/java/ru/sberbank/bigdata/dffess/vspc/, E/ = ESSA essa-common-lib
.../essa/common/ (audit_v2/SCHEMA.md).

Cases here:
  * S01 (DV-08 over a dirty target): chain `rerundirty` (NEW, loads R10..R14). 3.0 itself writes the dirt: INIT with
    loadingId 20205, then (3.0 only) an INCR with the LOWER id 20200 -> PK twins in div_ and two open versions in div_h,
    a same-PK closed+open pair in div_type (mechanism of H08). 4.0 takes over the 3.0 root after R11 and both versions
    rerun R11 twice (R12, R13: same loadingId 20200), then a normal INCR R14. S01_1 unchanged, S01_2 changes at the rerun.
  * S02 (DV-08, changed input on a clean target): chain `rerun` (existing): the R3 rerun changes div_h / div_type a
    second time on the same day and adds a new fact address (H07 variant for period tables and addr, R2.arb row 63).
  * S03 (DV-28, returner cache across the cutover and on dirt): chain `rerundirty`: vanish written by 3.0 (cache in
    stg.loader_cache_div_type), return on both; vanish of the open half of a closed+open same-PK pair; return inside a
    rerun with the same loadingId.

Dates of chain `rerundirty` are literals (the chain is added to model.CHAINS by the coordinator; d() needs it there):
  R10 INIT 2027-01-13 10:00 id 20205 (3.0 only); R11 INCR 2027-01-14 10:00 id 20200 (3.0 only, lower id);
  R12 INCR 2027-01-14 14:00 id 20200 (rerun of R11, both); R13 INCR 2027-01-14 18:00 id 20200 (2nd rerun, both);
  R14 INCR 2027-01-15 10:00 id 20206 (both). ctl_loading 20200 is shown as R11 (first label of the id).
"""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case

CH = "rerundirty"
J12, J13, J14 = "2027-01-12", "2027-01-13", "2027-01-14"   # D-2 / D-1 of R11..R13, D-1 of R14 (J13 = INIT date R10)
OPEN, JAN = "999912", "202701"

_WIN = "E/slave/loading/SnpDataLoader.java:179-204 (window partitionBy pk orderBy desc(ctl_loading), asc(record_status): " \
       "NEW before OLD on equal ctl_loading; rn1 NEW compares lead, others lag)"
_HUB = "E/slave/loading/DataLoaderHubDiff.java:75-80 (kept: NEW, NOTEQUALS rn1, EQUALS rn2, OLD), :127-144 (legs: NEW, " \
       "NOTEQUALS rn1/rn2; ctl_do R only for rn1)"
_MIX = "O/impl/loader/DataLoaderHistMix.java:75-81 (kept: NEW start 1900, NOTEQUALS rn1, EQUALS rn2 with own ctl_validfrom, " \
       "OLD), :84-89 (NOTEQUALS rn2 closed at another_vf-2, dropped when end < start), :92,128-139 (legs = rows with ctl_loading=L)"
_SNP = "E/slave/loading/DataLoaderHistSnp.java:70-73 (only 999912 merged, current/previous month as is), :82-96 (kept: NEW, " \
       "NOTEQUALS rn1, EQUALS rn2; NEW start = cached end + 1 or 1900), :103-108 (OLD closed at current_date-1, NOTEQUALS rn2 " \
       "at another_vf-2), :110-116 (OLD closures -> cache), :119 (OLD rows kept, others dropped when end < start), " \
       ":122,139-153 (legs = rows with ctl_loading=L before the 202701/202612 union)"


def _l1_type(t):
    """div_type after R11 (3.0, lower loadingId): the INIT row (rn1, OLD) closed today-1, the input (rn2, NEW) opens at
    1900 (cache empty after INIT): closed [1900, Jan 13] (202701) and open [1900, MAX] with one PK (div_sk, 1900)."""
    a = ver(MIN, J13, RUN("R11"), "R11", div_type_terr_div_type_sk=t, p1month=JAN)
    b = ver(MIN, MAX, RUN("R11"), "R11", div_type_terr_div_type_sk=t, p1month=OPEN)
    legs = [leg("R", "I", MIN, MAX, RUN("R11"), div_type_terr_div_type_sk=t), leg("U", "D", MIN, MAX, RUN("R11"), div_type_terr_div_type_sk=t),
            leg("R", "U", MIN, J13, RUN("R11"), div_type_terr_div_type_sk=t), leg("U", "U", MIN, MAX, RUN("R11"), div_type_terr_div_type_sk=t)]
    return a, b, legs


def s01():
    c = case(id="S01", chain=CH,
             title="DV-08 over a 3.0-made dirty target: INCR rerun (same loadingId, twice) over hub/div_h/div_type twins (chain rerundirty)",
             rules=["H06-record-status", "H07-snp-incr", "H08-diff-legs-incr", "R07", "R13", "R11"],
             changes=["NH-undo-keep-validfrom", "HPWR-silent-drop"], tables=["div_", "div_h", "div_type"],
             summary="DV-08 on dirt (SVOD_dirty DV-08: rn>=3 and same-L groups, R2.arb rows 62-64). R10 INIT id 20205 and R11 INCR "
                     "id 20200 run on 3.0 only and leave twins (as H08): div_ two rows (L 20205 and 20200), div_h two open "
                     "versions [Jan 13, MAX] (L 20205) + [1900, MAX] (L 20200), div_type closed [1900, Jan 13] + open [1900, MAX] "
                     "with one PK; 4.0 starts from that root. R12 and R13 rerun id 20200, R14 is a normal INCR (id 20206). "
                     "Window: " + _WIN + ". S01_1 (input unchanged): rerun window = OLD(20205) rn1, NEW(20200) rn2, OLD(20200) "
                     "rn3. div_: rn1 stays OLD (kept), NEW EQUALS rn2 kept with RUN of the rerun, OLD(20200) EQUALS rn3 "
                     "dropped, no legs (" + _HUB + "). div_h: NEW EQUALS rn2 kept with ITS start D-1 = Jan 13 (not 1900): "
                     "two rows with PK (S01_1, Jan 13), the [1900, MAX] row vanishes without a leg, no-op (R,U)+(U,U) of the "
                     "new row (" + _MIX + "). div_type: only 999912 is merged, NEW and OLD both 20200 -> EQUALS rn2 OLD kept "
                     "-> no-op (R,U)+(U,U) [1900, MAX] with ctl_validfrom RUN@R11; R13 appends the SAME pair again (exact "
                     "duplicate legs, " + _SNP + "; diff append E/slave/move/ClassicMovingGenerator.java:19-25). R14: NEW(20206) "
                     "rn1 EQUALS (dropped), OLD(20205) EQUALS rn2 kept, OLD(20200) EQUALS rn3 dropped: div_ and div_h collapse "
                     "to the INIT row without legs. S01_2 changes short code, name and type at R12 (rerun). div_: NEW rn2 "
                     "NOTEQUALS -> not kept, only a (U,U) leg with the NEW payload; OLD(20200) rn3 NOTEQUALS dropped: snp keeps "
                     "the INIT row '01' only (the change is lost); R13 (same input) has no 20200 row left -> NEW '02' rn2 kept "
                     "again with (U,I)+(U,D); R14 publishes '02' (R,U)+(U,U), the 20200 twin dropped silently (rn3). div_h at "
                     "R12: NEW rn2 NOTEQUALS closed at another_vf-2 = Jan 12 < start Jan 13 -> dropped, OLD(20200) rn3 dropped: "
                     "only the INIT row, no R12 legs; R13: NEW rn2 (lead NULL) kept as NEW [1900, MAX]; R14: normal close [Jan 13, "
                     "Jan 13] / open [Jan 14, MAX], the 20200 row dropped silently (rn3). div_type at R12: NOTEQUALS rn1 opens "
                     "[Jan 13, MAX], rn2 closes [1900, Jan 12]: 202701 holds [1900, Jan 13] (R11) AND [1900, Jan 12] (R12), one "
                     "PK; R13 no-op pair of the R12 version. Every window has a strict order (no ties): deterministic.")
    for k, at in (("S01_1", None),
                  ("S01_2", {"R12": dict(shortdivcode="02", fullname="S01 B", divisiontype00="S01_T2")})):
        c.div(k, fullname="S01 A", divisiontype00="S01_T1", at=at)
    T1, T2 = "S01_T1", "S01_T2"
    for k in ("S01_1", "S01_2"):
        c.expect("R10", "snp.div_", k, [hub(RUN("R10"), "R10", short_div_code="01")])
        c.expect("R10", "snp.div_h", k, [ver(J13, MAX, START, "R10", div_name="S01 A")])
        c.expect("R10", "snp.div_type", k, [ver(MIN, MAX, RUN("R10"), "R10", div_type_terr_div_type_sk=T1, p1month=OPEN)])
        c.expect("R11", "snp.div_", k, [hub(RUN("R10"), "R10", short_div_code="01"), hub(RUN("R11"), "R11", short_div_code="01")],
                 note="3.0 dirt: the stored row (higher ctl_loading 20205) is rn1 and stays OLD, the input (20200) stays NEW")
        c.expect("R11", "snp.div_h", k, [ver(J13, MAX, START, "R10", div_name="S01 A"), ver(MIN, MAX, RUN("R11"), "R11", div_name="S01 A")])
        a, b, _ = _l1_type(T1)
        c.expect("R11", "snp.div_type", k, [a, b], note="closed [1900, %s] and open [1900, MAX]: one PK (div_sk, 1900)" % J13)
    hub_l1 = [hleg("U", "I", RUN("R11"), short_div_code="01"), hleg("U", "D", RUN("R11"), short_div_code="01")]
    h_l1 = [leg("R", "I", MIN, MAX, RUN("R11"), div_name="S01 A"), leg("U", "D", MIN, MAX, RUN("R11"), div_name="S01 A")]
    a, b, t_l1 = _l1_type(T1)

    # ---- S01_1: unchanged input
    k = "S01_1"
    c.expect("R11", "diff.div_", k, hub_l1)
    c.expect("R11", "diff.div_h", k, h_l1)
    c.expect("R11", "diff.div_type", k, t_l1)
    init_h = ver(J13, MAX, START, "R10", div_name="S01 A")
    idle_t = [leg("R", "U", MIN, MAX, RUN("R11"), div_type_terr_div_type_sk=T1), leg("U", "U", MIN, MAX, RUN("R11"), div_type_terr_div_type_sk=T1)]
    acc_h = list(h_l1)
    for ld, n in (("R12", 1), ("R13", 2)):
        c.expect(ld, "snp.div_", k, [hub(RUN("R10"), "R10", short_div_code="01"), hub(RUN(ld), "R11", short_div_code="01")],
                 note="rn1 OLD(20205) kept; NEW(20200) EQUALS rn2 kept, re-stamped; OLD(20200) EQUALS rn3 dropped")
        c.expect(ld, "diff.div_", k, hub_l1, note="no new hub legs on the rerun; only the R11 legs under id 20200")
        c.expect(ld, "snp.div_h", k, [init_h, ver(J13, MAX, RUN(ld), "R11", div_name="S01 A")],
                 note="NEW EQUALS rn2 kept with its own start D-1=%s: PK twin (S01_1, %s); [1900, MAX] gone without a leg" % (J13, J13))
        acc_h = acc_h + [leg("R", "U", J13, MAX, RUN(ld), div_name="S01 A"), leg("U", "U", J13, MAX, RUN(ld), div_name="S01 A")]
        c.expect(ld, "diff.div_h", k, list(acc_h))
        c.expect(ld, "snp.div_type", k, [a, b])
        c.expect(ld, "diff.div_type", k, t_l1 + idle_t * n,
                 note="R11 legs + %d identical no-op pair(s) (R,U)+(U,U) [1900, MAX] RUN@R11 (exact duplicates after R13)" % n)
    c.expect("R14", "snp.div_", k, [hub(RUN("R10"), "R10", short_div_code="01")], note="twin collapses: OLD(20200) EQUALS rn3 dropped")
    c.expect("R14", "diff.div_", k, [])
    c.expect("R14", "snp.div_h", k, [init_h], note="the 20200 version [%s, MAX] is EQUALS rn3 and dropped without a leg" % J13)
    c.expect("R14", "diff.div_h", k, [])
    c.expect("R14", "snp.div_type", k, [a, b])
    c.expect("R14", "diff.div_type", k, [])

    # ---- S01_2: short code, name and type change at the rerun R12
    k = "S01_2"
    c.expect("R11", "diff.div_", k, hub_l1)
    c.expect("R12", "snp.div_", k, [hub(RUN("R10"), "R10", short_div_code="01")],
             note="NEW rn2 NOTEQUALS (vs OLD 20200) and OLD(20200) rn3 NOTEQUALS: neither kept; the change is lost")
    c.expect("R12", "diff.div_", k, hub_l1 + [hleg("U", "U", RUN("R12"), short_div_code="02")],
             note="one (U,U) with the NEW payload (rn2), nothing for rn3")
    c.expect("R13", "snp.div_", k, [hub(RUN("R10"), "R10", short_div_code="01"), hub(RUN("R13"), "R11", short_div_code="02")])
    c.expect("R13", "diff.div_", k, hub_l1 + [hleg("U", "U", RUN("R12"), short_div_code="02"),
                                             hleg("U", "I", RUN("R13"), short_div_code="02"), hleg("U", "D", RUN("R13"), short_div_code="02")])
    c.expect("R14", "snp.div_", k, [hub(RUN("R14"), "R14", short_div_code="02")])
    c.expect("R14", "diff.div_", k, [hleg("R", "U", RUN("R14"), short_div_code="02"), hleg("U", "U", RUN("R10"), short_div_code="01")],
             note="the 20200 twin '02' is NOTEQUALS rn3 (vs '01') and dropped without a leg")
    c.expect("R11", "diff.div_h", k, h_l1)
    c.expect("R12", "snp.div_h", k, [init_h],
             note="NEW rn2 closed at Jan 12 < its start Jan 13 -> dropped; OLD(20200) rn3 dropped: only the INIT row")
    c.expect("R12", "diff.div_h", k, h_l1, note="no R12 legs")
    c.expect("R13", "snp.div_h", k, [init_h, ver(MIN, MAX, RUN("R13"), "R11", div_name="S01 B")])
    c.expect("R13", "diff.div_h", k, h_l1 + [leg("R", "I", MIN, MAX, RUN("R13"), div_name="S01 B"),
                                           leg("U", "D", MIN, MAX, RUN("R13"), div_name="S01 B")])
    c.expect("R14", "snp.div_h", k, [ver(J13, J13, RUN("R14"), "R14", div_name="S01 A", p1month=JAN),
                                    ver(J14, MAX, RUN("R14"), "R14", div_name="S01 B", p1month=OPEN)],
             note="the 20200 version [1900, MAX] 'S01 B' is NOTEQUALS rn3 and dropped without a leg")
    c.expect("R14", "diff.div_h", k, [leg("R", "I", J14, MAX, RUN("R14"), div_name="S01 B"), leg("U", "D", J14, MAX, RUN("R14"), div_name="S01 B"),
                                     leg("R", "U", J13, J13, RUN("R14"), div_name="S01 A"), leg("U", "U", J13, MAX, RUN("R14"), div_name="S01 A")])
    c.expect("R11", "diff.div_type", k, t_l1)
    b2 = ver(MIN, J12, RUN("R12"), "R11", div_type_terr_div_type_sk=T1, p1month=JAN)
    n2 = ver(J13, MAX, RUN("R12"), "R11", div_type_terr_div_type_sk=T2, p1month=OPEN)
    t_l2 = [leg("R", "I", J13, MAX, RUN("R12"), div_type_terr_div_type_sk=T2), leg("U", "D", J13, MAX, RUN("R12"), div_type_terr_div_type_sk=T2),
            leg("R", "U", MIN, J12, RUN("R12"), div_type_terr_div_type_sk=T1), leg("U", "U", MIN, MAX, RUN("R12"), div_type_terr_div_type_sk=T1)]
    for ld in ("R12", "R13", "R14"):
        c.expect(ld, "snp.div_type", k, [a, b2, n2],
                 note="202701: [1900, %s] (R11) and [1900, %s] (R12) share the PK (S01_2, 1900)" % (J13, J12))
    c.expect("R12", "diff.div_type", k, t_l1 + t_l2)
    c.expect("R13", "diff.div_type", k, t_l1 + t_l2 + [leg("R", "U", J13, MAX, RUN("R12"), div_type_terr_div_type_sk=T2),
                                                     leg("U", "U", J13, MAX, RUN("R12"), div_type_terr_div_type_sk=T2)],
             note="no-op pair of the R12 version (ctl_loading 20200, EQUALS rn2)")
    c.expect("R14", "diff.div_type", k, [])


def s02():
    c = case(id="S02", chain="rerun",
             title="DV-08, changed input on a clean target: the R3 rerun changes div_h / div_type again the same day and adds a fact address",
             rules=["R07", "R11", "R13", "R15", "H07-snp-incr", "H08-diff-legs-incr", "R08"],
             changes=["HPWR-silent-drop", "HPWR-new-key-start-1900"], tables=["div_h", "div_type", "addr", "addr_h"],
             summary="DV-08 with a changed input (SVOD_dirty DV-08, R2.arb row 63: H07 variant for period tables and a new "
                     "address). R2 (id 6001) changes S02_1's name A->B and S02_2's type T1->T2 (normal close D-2 / open D-1). "
                     "R3 = R2 again (same id 6001, same day) with name C and type T3: NEW and OLD both carry 6001, NEW is rn1 "
                     "(" + _WIN + "), NOTEQUALS rn1 opens [D-1, MAX]; the R2 version (NOTEQUALS rn2) closes at another_vf-2 = "
                     "D-2 < its start D-1 and is dropped without a leg (O/impl/loader/DataLoaderHistMix.java:84-89; div_type "
                     "E/slave/loading/DataLoaderHistSnp.java:103-108,119): diff id 6001 then holds (R,I)+(U,D) of B/T2 AND of "
                     "C/T3 with one PK (key, D-1). The [.., D-2] closures written by R2 sit in 202701 and are returned as is "
                     "(DataLoaderHistMix.java:66-69,94). S02_3 gets a fact address only at R3: new addr / addr_h key (1900) and "
                     "a normal div_h close/open, all under id 6001 (shown R2). 3.0 runs the full ETL on a rerun (no replay "
                     "guard, E/master/app/recover/DataProductRecoveryImpl.java:33-38) and appends to diff (E/slave/move/"
                     "ClassicMovingGenerator.java:19-25). INIT versions start at the d_ date (collapsed with the snapshot, as R01_1).")
    r1 = c.div("S02_1", fullname="S02 A", at={"R2": dict(fullname="S02 B"), "R3": dict(fullname="S02 C")})
    c.hist(r1, "2026-04-04 10:00:00", 1)
    c.div("S02_2", divisiontype00="S02_T1", at={"R2": dict(divisiontype00="S02_T2"), "R3": dict(divisiontype00="S02_T3")})
    r3 = c.div("S02_3", at={"R3": dict(address_locationaddress="S02 fact")})
    c.hist(r3, "2026-04-04 10:00:00", 1)
    m1, m2 = d("R2", -1, chain="rerun"), d("R2", -2, chain="rerun")
    h0 = "2026-04-04"
    # S02_1 div_h
    closed = ver(h0, m2, RUN("R2"), "R2", div_name="S02 A", p1month=JAN)
    l2 = [leg("R", "I", m1, MAX, RUN("R2"), div_name="S02 B"), leg("U", "D", m1, MAX, RUN("R2"), div_name="S02 B"),
          leg("R", "U", h0, m2, RUN("R2"), div_name="S02 A"), leg("U", "U", h0, MAX, RUN("R2"), div_name="S02 A")]
    c.expect("R2", "snp.div_h", "S02_1", [closed, ver(m1, MAX, RUN("R2"), "R2", div_name="S02 B", p1month=OPEN)])
    c.expect("R2", "diff.div_h", "S02_1", l2)
    c.expect("R3", "snp.div_h", "S02_1", [closed, ver(m1, MAX, RUN("R3"), "R2", div_name="S02 C", p1month=OPEN)],
             note="the R2 version [%s, MAX] 'S02 B' closes at %s < its start and is dropped" % (m1, m2))
    c.expect("R3", "diff.div_h", "S02_1", l2 + [leg("R", "I", m1, MAX, RUN("R3"), div_name="S02 C"),
                                               leg("U", "D", m1, MAX, RUN("R3"), div_name="S02 C")],
             note="R2 legs + (R,I)+(U,D) of 'S02 C' with the same PK (S02_1, %s) as the R2 (R,I); no leg for 'S02 B'" % m1)
    # S02_2 div_type
    tc = ver(MIN, m2, RUN("R2"), "R2", div_type_terr_div_type_sk="S02_T1", p1month=JAN)
    t2 = [leg("R", "I", m1, MAX, RUN("R2"), div_type_terr_div_type_sk="S02_T2"), leg("U", "D", m1, MAX, RUN("R2"), div_type_terr_div_type_sk="S02_T2"),
          leg("R", "U", MIN, m2, RUN("R2"), div_type_terr_div_type_sk="S02_T1"), leg("U", "U", MIN, MAX, RUN("R2"), div_type_terr_div_type_sk="S02_T1")]
    c.expect("R1", "snp.div_type", "S02_2", [ver(MIN, MAX, RUN("R1"), "R0", div_type_terr_div_type_sk="S02_T1", p1month=OPEN)])
    c.expect("R2", "snp.div_type", "S02_2", [tc, ver(m1, MAX, RUN("R2"), "R2", div_type_terr_div_type_sk="S02_T2", p1month=OPEN)])
    c.expect("R2", "diff.div_type", "S02_2", t2)
    c.expect("R3", "snp.div_type", "S02_2", [tc, ver(m1, MAX, RUN("R3"), "R2", div_type_terr_div_type_sk="S02_T3", p1month=OPEN)],
             note="the T2 version [%s, MAX] is NOTEQUALS rn2, closed at %s < start, dropped (not OLD)" % (m1, m2))
    c.expect("R3", "diff.div_type", "S02_2", t2 + [leg("R", "I", m1, MAX, RUN("R3"), div_type_terr_div_type_sk="S02_T3"),
                                                  leg("U", "D", m1, MAX, RUN("R3"), div_type_terr_div_type_sk="S02_T3")])
    # S02_3 fact address at the rerun
    ka = addr_key(src="S02 fact")
    c.expect("R2", "snp.div_h", "S02_3", [ver(h0, MAX, START, "R0", fact_addr_sk=None)])
    c.expect("R2", "snp.addr", ka, [])
    c.expect("R3", "snp.div_h", "S02_3", [ver(h0, m2, RUN("R3"), "R2", fact_addr_sk=None, p1month=JAN),
                                         ver(m1, MAX, RUN("R3"), "R2", fact_addr_sk=ka, p1month=OPEN)])
    c.expect("R3", "diff.div_h", "S02_3", [leg("R", "I", m1, MAX, RUN("R3"), fact_addr_sk=ka), leg("U", "D", m1, MAX, RUN("R3"), fact_addr_sk=ka),
                                          leg("R", "U", h0, m2, RUN("R3"), fact_addr_sk=None), leg("U", "U", h0, MAX, RUN("R3"), fact_addr_sk=None)])
    c.expect("R3", "snp.addr", ka, [hub(RUN("R3"), "R2", src_txt="S02 fact")])
    c.expect("R3", "diff.addr", ka, [hleg("R", "I", RUN("R3")), hleg("U", "D", RUN("R3"))])
    c.expect("R3", "snp.addr_h", ka, [ver(MIN, MAX, RUN("R3"), "R2", standard_addr_txt="S02 fact")])
    c.expect("R3", "diff.addr_h", ka, [leg("R", "I", MIN, MAX, RUN("R3")), leg("U", "D", MIN, MAX, RUN("R3"))])


def s03():
    c = case(id="S03", chain=CH,
             title="DV-28: div_type returners across the cutover and on dirt (cache written by 3.0; vanish of a same-PK pair; return in a rerun)",
             rules=["R13", "R14", "R15", "H07-snp-incr"], changes=["HPWR-vanish-divtype", "HPWR-returner-start-date"],
             tables=["div_type"],
             summary="DV-28 (returner cache, SVOD_dirty DV-28; S3.arb S3d-6, R1.arb R1-D5). 3.0 keeps returners in "
                     "stg.loader_cache_div_type: vanish = OLD closed at current_date-1 and dumped to the cache, return = NEW "
                     "with start = cached end + 1 (" + _SNP + "; E/slave/loading/LoaderCache.java:87-114: dump keeps old "
                     "records, the new one wins). 4.0 has no cache. Chain rerundirty: R10/R11 on 3.0 only (R11 with the lower id "
                     "20200), then 4.0 from the 3.0 root. S03_2: vanishes at R11 (3.0 writes [1900, Jan 13] and the cache), "
                     "returns at R14 (both): start = Jan 14. S03_1: present at R11 -> the 3.0 dirt of S01 (closed [1900, Jan 13] "
                     "+ open [1900, MAX], one PK; note the R11 cache already holds S03_1 -> Jan 13 while it is open); vanishes "
                     "at R12 (rerun): the open half closes at Jan 13 -> 202701 holds TWO rows [1900, Jan 13] with one PK and one "
                     "payload; returns at R14: start = Jan 14. S03_3: vanishes at R11, returns at R12 = rerun of R11 (same id 20200): "
                     "start = cached end + 1 = Jan 14 (> D-1); R13 (2nd rerun) appends the no-op pair of that version; the cache "
                     "record stays (only toCache is filtered, DataLoaderHistSnp.java:99,110-116). Deterministic.")
    c.div("S03_1", at={"R12": GONE, "R14": {}})
    c.div("S03_2", at={"R11": GONE, "R14": {}})
    c.div("S03_3", at={"R11": GONE, "R12": {}})
    T = "S03_T"
    a, b, t_l1 = _l1_type(T)
    van1 = ver(MIN, J13, RUN("R11"), "R11", div_type_terr_div_type_sk=T, p1month=JAN)
    van1_legs = [leg("R", "U", MIN, J13, RUN("R11"), div_type_terr_div_type_sk=T), leg("U", "U", MIN, MAX, RUN("R11"), div_type_terr_div_type_sk=T)]
    for k in ("S03_1", "S03_2", "S03_3"):
        c.expect("R10", "snp.div_type", k, [ver(MIN, MAX, RUN("R10"), "R10", div_type_terr_div_type_sk=T, p1month=OPEN)])
    # S03_1
    k = "S03_1"
    b_closed = ver(MIN, J13, RUN("R12"), "R11", div_type_terr_div_type_sk=T, p1month=JAN)
    l2 = [leg("R", "U", MIN, J13, RUN("R12"), div_type_terr_div_type_sk=T), leg("U", "U", MIN, MAX, RUN("R12"), div_type_terr_div_type_sk=T)]
    c.expect("R11", "snp.div_type", k, [a, b])
    c.expect("R11", "diff.div_type", k, t_l1)
    for ld in ("R12", "R13"):
        c.expect(ld, "snp.div_type", k, [a, b_closed],
                 note="vanish of the open half: two rows [1900, %s] with one PK (S03_1, 1900) in 202701" % J13)
        c.expect(ld, "diff.div_type", k, t_l1 + l2)
    c.expect("R14", "snp.div_type", k, [a, b_closed, ver(J14, MAX, RUN("R14"), "R14", div_type_terr_div_type_sk=T, p1month=OPEN)],
             note="returner: start = cached end + 1 = %s" % J14)
    c.expect("R14", "diff.div_type", k, [leg("R", "I", J14, MAX, RUN("R14"), div_type_terr_div_type_sk=T),
                                        leg("U", "D", J14, MAX, RUN("R14"), div_type_terr_div_type_sk=T)])
    # S03_2
    k = "S03_2"
    c.expect("R11", "snp.div_type", k, [van1])
    c.expect("R11", "diff.div_type", k, van1_legs)
    c.expect("R13", "snp.div_type", k, [van1])
    c.expect("R13", "diff.div_type", k, van1_legs, note="no legs while absent")
    c.expect("R14", "snp.div_type", k, [van1, ver(J14, MAX, RUN("R14"), "R14", div_type_terr_div_type_sk=T, p1month=OPEN)],
             note="vanish written by 3.0 (cache), return on the taken-over root: start = %s" % J14)
    c.expect("R14", "diff.div_type", k, [leg("R", "I", J14, MAX, RUN("R14"), div_type_terr_div_type_sk=T),
                                        leg("U", "D", J14, MAX, RUN("R14"), div_type_terr_div_type_sk=T)])
    # S03_3
    k = "S03_3"
    back = ver(J14, MAX, RUN("R12"), "R11", div_type_terr_div_type_sk=T, p1month=OPEN)
    l_back = [leg("R", "I", J14, MAX, RUN("R12"), div_type_terr_div_type_sk=T), leg("U", "D", J14, MAX, RUN("R12"), div_type_terr_div_type_sk=T)]
    idle = [leg("R", "U", J14, MAX, RUN("R12"), div_type_terr_div_type_sk=T), leg("U", "U", J14, MAX, RUN("R12"), div_type_terr_div_type_sk=T)]
    c.expect("R11", "snp.div_type", k, [van1])
    c.expect("R12", "snp.div_type", k, [van1, back], note="return inside the rerun: start = cached end + 1 = %s, not D-1 = %s" % (J14, J13))
    c.expect("R12", "diff.div_type", k, van1_legs + l_back)
    c.expect("R13", "snp.div_type", k, [van1, back])
    c.expect("R13", "diff.div_type", k, van1_legs + l_back + idle, note="2nd rerun: no-op pair of the returned version")
    c.expect("R14", "snp.div_type", k, [van1, back])
    c.expect("R14", "diff.div_type", k, [])


for _f in [s01, s02, s03]:
    _f()
