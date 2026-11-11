"""div_h and div_type version chains: closing at D-2, opening at D-1, same-day / next-day relabel, vanish and return, month boundaries."""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case


def c01():
    c = case(id="C01", title="div_h INIT history is rebuilt from the d_ diff log",
             rules=["R10", "S2-chgdivh-init", "L8-histmix-init", "R07", "M2-loader-mover-window-contract"],
             changes=["PREP-keep-producer-ctl_validfrom", "VF-INIT-carry"], scenario="",
             tables=["div_h"],
             summary="INIT builds the version chain from d_ rows with ctl_do='R' and snapshot_ LIKE '%DIF%' plus the "
                     "current snapshot (start=INIT date). Equal consecutive versions collapse; of two versions on one "
                     "calendar day only the last survives; ctl_validfrom = start_dt (00:00). Later a normal change at "
                     "L4 closes the snapshot version; the INIT versions in partitions 202605/202701 must survive L4..L7.")
    r = c.div("C01_1", fullname="C01 v3 current", at={"L4": dict(fullname="C01 v4")})
    c.hist(r, "2026-03-10 10:00:00", 1, fullname="C01 v1")
    c.hist(r, "2026-06-01 09:00:00", 2, fullname="C01 v2 morning")
    c.hist(r, "2026-06-01 15:00:00", 3, fullname="C01 v2 evening")
    c.hist(r, "2026-09-20 10:00:00", 4, fullname="C01 v2 evening")   # equal to previous -> collapsed
    c.hist(r, "2026-10-01 10:00:00", 5, ctl_do="U", fullname="C01 ignored ctl_do=U")
    c.hist(r, "2026-10-02 10:00:00", 6, snapshot_="SNP", fullname="C01 ignored snapshot_=SNP")
    init = [ver("2026-03-10", "2026-05-31", START, "L0", div_name="C01 v1", p1month="202605"),
            ver("2026-06-01", "2027-01-12", START, "L0", div_name="C01 v2 evening", p1month="202701")]
    c.expect("L0", "snp.div_h", "C01_1", init + [ver(d("L0"), MAX, START, "L0", div_name="C01 v3 current", p1month="999912")],
             note="v2 morning dropped (same day as v2 evening), 4th d_ row collapsed (same hash), ctl_do=U and snapshot_=SNP rows ignored")
    c.expect("L0", "diff.div_h", "C01_1", [], note="INIT writes no diff")
    after = init + [ver(d("L0"), d("L4", -2), RUN("L4"), "L4", div_name="C01 v3 current", p1month="202701"),
                    ver(d("L4", -1), MAX, RUN("L4"), "L4", div_name="C01 v4", p1month="999912")]
    c.expect("L4", "snp.div_h", "C01_1", after, note="old open version closed at D-2=%s, new opens at D-1" % d("L4", -2))
    c.expect("L4", "diff.div_h", "C01_1", [
        leg("R", "I", d("L4", -1), MAX, RUN("L4"), div_name="C01 v4"),
        leg("U", "D", d("L4", -1), MAX, RUN("L4"), div_name="C01 v4"),
        leg("R", "U", d("L0"), d("L4", -2), RUN("L4"), div_name="C01 v3 current"),
        leg("U", "U", d("L0"), MAX, RUN("L4"), div_name="C01 v3 current")],
        note="closing legs carry the closing run's ctl_validfrom (not the old one) - HistMix")
    c.expect("L7", "snp.div_h", "C01_1", after, note="202605 and 202701 are outside the L7 rewrite zone (>=202702): untouched")


def c02():
    c = case(id="C02", title="Normal change chain: L3, then 1st and 2nd of month (closures land in previous month)",
             rules=["R07", "R11", "L9-histmix-incr", "M1-hist-mover-truncation", "M2-loader-mover-window-contract"],
             changes=["HPWR-close-date-loadDate-2", "HPWR-geq-threshold-month", "FX26-rewrite-scope", "HPWR-read-whole-snp"],
             scenario="S6", tables=["div_h"],
             summary="Every change closes the open version at D-2 and opens a new one at D-1. On L4 (1st) and L5 (2nd) "
                     "the closed rows go to partition 202701 (previous month); on L5 the closed version lasts one day. "
                     "At L7 partition 202701 is outside the zone and must stay byte-identical.")
    r = c.div("C02_1", fullname="C02 A", at={"L3": dict(fullname="C02 B"), "L4": dict(fullname="C02 C"),
                                            "L5": dict(fullname="C02 D"), "L7": dict(fullname="C02 E")})
    c.hist(r, "2026-05-05 10:00:00", 1)
    c.expect("L0", "snp.div_h", "C02_1", [ver("2026-05-05", MAX, START, "L0", div_name="C02 A")],
             note="snapshot equals the last d_ version -> collapsed, start = d_ ctl_validfrom")
    v_a = ver("2026-05-05", d("L3", -2), RUN("L3"), "L3", div_name="C02 A", p1month="202701")
    v_b = ver(d("L3", -1), d("L4", -2), RUN("L4"), "L4", div_name="C02 B", p1month="202701")
    v_c = ver(d("L4", -1), d("L5", -2), RUN("L5"), "L5", div_name="C02 C", p1month="202701")
    v_d = ver(d("L5", -1), d("L7", -2), RUN("L7"), "L7", div_name="C02 D", p1month="202702")
    c.expect("L3", "snp.div_h", "C02_1", [v_a, ver(d("L3", -1), MAX, RUN("L3"), "L3", div_name="C02 B", p1month="999912")])
    c.expect("L5", "snp.div_h", "C02_1", [v_a, v_b, v_c, ver(d("L5", -1), MAX, RUN("L5"), "L5", div_name="C02 D", p1month="999912")],
             note="L5 = 2nd: version C lives exactly one day [%s,%s]" % (d("L4", -1), d("L5", -2)))
    c.expect("L5", "diff.div_h", "C02_1", [
        leg("R", "I", d("L5", -1), MAX, RUN("L5"), div_name="C02 D", p1month="999912"),
        leg("U", "D", d("L5", -1), MAX, RUN("L5"), div_name="C02 D", p1month="999912"),
        leg("R", "U", d("L4", -1), d("L5", -2), RUN("L5"), div_name="C02 C", p1month="202701"),
        leg("U", "U", d("L4", -1), MAX, RUN("L5"), div_name="C02 C", p1month="999912")])
    c.expect("L7", "snp.div_h", "C02_1", [v_a, v_b, v_c, v_d, ver(d("L7", -1), MAX, RUN("L7"), "L7", div_name="C02 E", p1month="999912")])


def c03():
    c = case(id="C03", chain="samepk", title="S2: change on the day after INIT drops the INIT version (no closing legs)",
             rules=["R11", "R15", "L9-histmix-incr", "R10"],
             changes=["HPWR-consec-day-relabel", "HPWR-silent-drop", "HPWR-silent-drop-diff-suppress"], scenario="S2",
             tables=["div_h"],
             summary="A division that exists only in the snapshot gets start = INIT date (current_date) at INIT. "
                     "On the next day D-1 equals that start, so the closure end D-2 < start: 3.0 deletes the old version "
                     "and writes only (R,I)+(U,D) for the new one. Known 4.0-baseline defect: duplicate PK.")
    c.div("C03_1", fullname="C03 A", at={"K1": dict(fullname="C03 B")})
    c.expect("K0", "snp.div_h", "C03_1", [ver(d("K0"), MAX, START, "K0", div_name="C03 A")])
    c.expect("K1", "snp.div_h", "C03_1", [ver(d("K1", -1), MAX, RUN("K1"), "K1", div_name="C03 B")],
             note="INIT version [%s,MAX] would close at %s < start -> removed" % (d("K0"), d("K1", -2)))
    c.expect("K1", "diff.div_h", "C03_1", [leg("R", "I", d("K1", -1), MAX, RUN("K1"), div_name="C03 B"),
                                          leg("U", "D", d("K1", -1), MAX, RUN("K1"), div_name="C03 B")])


def c04():
    c = case(id="C04", chain="samepk", title="S1: second change on the same calendar day removes the intermediate version",
             rules=["R15", "R07", "L9-histmix-incr"],
             changes=["HPWR-consec-day-relabel", "HPWR-silent-drop", "HPWR-silent-drop-diff-suppress"], scenario="S1",
             tables=["div_h"],
             summary="L1 makes a normal change (old closed at D-2, new B opens at D-1). L2 (same day) changes again: B "
                     "started D-1 and cannot be closed at D-2, so B vanishes from snp and diff has only (R,I)+(U,D) of C.")
    r = c.div("C04_1", fullname="C04 A", at={"K1": dict(fullname="C04 B"), "K2": dict(fullname="C04 C")})
    c.hist(r, "2026-08-01 10:00:00", 1)
    closed_a = ver("2026-08-01", d("K1", -2), RUN("K1"), "K1", div_name="C04 A")
    c.expect("K1", "snp.div_h", "C04_1", [closed_a, ver(d("K1", -1), MAX, RUN("K1"), "K1", div_name="C04 B")])
    c.expect("K1", "diff.div_h", "C04_1", [
        leg("R", "I", d("K1", -1), MAX, RUN("K1"), div_name="C04 B"), leg("U", "D", d("K1", -1), MAX, RUN("K1"), div_name="C04 B"),
        leg("R", "U", "2026-08-01", d("K1", -2), RUN("K1"), div_name="C04 A"), leg("U", "U", "2026-08-01", MAX, RUN("K1"), div_name="C04 A")])
    c.expect("K2", "snp.div_h", "C04_1", [closed_a, ver(d("K2", -1), MAX, RUN("K2"), "K2", div_name="C04 C")])
    c.expect("K2", "diff.div_h", "C04_1", [leg("R", "I", d("K2", -1), MAX, RUN("K2"), div_name="C04 C"),
                                          leg("U", "D", d("K2", -1), MAX, RUN("K2"), div_name="C04 C")],
             note="no leg mentions version B: it silently disappears")


def c06():
    c = case(id="C06", title="Key vanishes and returns (twice): hubs/div_h keep it, div_type closes and re-opens",
             rules=["H02-input-is-full-snapshot", "H12-deletes", "R02", "R13", "R14", "L6-link-effective"],
             changes=["HPWR-vanish-divtype", "HPWR-returner-start-date"], tables=["div_h", "div_", "div_type", "addr", "addr_h"],
             summary="3.0 never closes a vanished key in div_h/addr_h/hubs. div_type (HistSnp) closes it at today-1 and "
                     "remembers the end in loader_cache_div_type; when the key returns, start = cached end + 1 "
                     "(the absence gap is erased). Second vanish at L6, return at L7 with a changed name.")
    r = c.div("C06_1", fullname="C06 A", address_locationaddress="C06 fact",
              at={"L3": GONE, "L4": {}, "L6": GONE, "L7": dict(fullname="C06 B")})
    c.hist(r, "2026-04-04 10:00:00", 1)
    c.expect("L3", "snp.div_h", "C06_1", [ver("2026-04-04", MAX, START, "L0", div_name="C06 A")], note="vanished: untouched")
    c.expect("L3", "diff.div_h", "C06_1", [])
    t1 = ver(MIN, d("L3", -1), RUN("L3"), "L3", p1month="202701")
    c.expect("L3", "snp.div_type", "C06_1", [t1], note="vanish closes at today-1 (not D-2)")
    c.expect("L3", "diff.div_type", "C06_1", [leg("R", "U", MIN, d("L3", -1), RUN("L3")), leg("U", "U", MIN, MAX, RUN("L3"))])
    c.expect("L4", "snp.div_type", "C06_1", [t1, ver(d("L3"), MAX, RUN("L4"), "L4", p1month="999912")],
             note="returner: start = cached end + 1 = %s (not D-1 = %s)" % (d("L3"), d("L4", -1)))
    c.expect("L4", "diff.div_type", "C06_1", [leg("R", "I", d("L3"), MAX, RUN("L4")), leg("U", "D", d("L3"), MAX, RUN("L4"))])
    t2 = ver(d("L3"), d("L6", -1), RUN("L6"), "L6", p1month="202702")
    c.expect("L7", "snp.div_type", "C06_1", [t1, t2, ver(d("L6"), MAX, RUN("L7"), "L7", p1month="999912")])
    c.expect("L7", "snp.div_h", "C06_1", [ver("2026-04-04", d("L7", -2), RUN("L7"), "L7", div_name="C06 A"),
                                         ver(d("L7", -1), MAX, RUN("L7"), "L7", div_name="C06 B")])
    c.expect("L7", "snp.div_", "C06_1", [hub(RUN("L0"), "L0")], note="hub: vanish/return/name change leave it untouched")
    k = addr_key(src="C06 fact")
    c.expect("L7", "snp.addr_h", k, [ver(MIN, MAX, RUN("L0"), "L0")])
    c.expect("L7", "snp.addr", k, [hub(RUN("L0"), "L0")])


def c07():
    c = case(id="C07", title="S3: div_type returner after >11 days across a month boundary",
             rules=["R13", "R14"], changes=["HPWR-returner-start-date", "HPWR-read-whole-snp", "HPWR-midd-filter-removed"],
             scenario="S3", tables=["div_type", "terr_div_type", "div_h"],
             summary="Vanishes at L3 (closed [1900,%s] in partition 202701), returns at L7 (Mar 2) with another type. "
                     "3.0 reopens at cached end+1 = %s: far behind today-max_increment_depth_days and in a partition "
                     "outside the L7 rewrite zone. div_h is not touched by vanish/return." % (d("L3", -1), d("L3")))
    c.div("C07_1", divisiontype00="C07_T1", at={"L3": GONE, "L7": dict(divisiontype00="C07_T2")})
    t1 = ver(MIN, d("L3", -1), RUN("L3"), "L3", div_type_terr_div_type_sk="C07_T1", p1month="202701")
    c.expect("L3", "snp.div_type", "C07_1", [t1])
    c.expect("L7", "snp.div_type", "C07_1", [t1, ver(d("L3"), MAX, RUN("L7"), "L7", div_type_terr_div_type_sk="C07_T2", p1month="999912")])
    c.expect("L7", "diff.div_type", "C07_1", [leg("R", "I", d("L3"), MAX, RUN("L7"), div_type_terr_div_type_sk="C07_T2"),
                                             leg("U", "D", d("L3"), MAX, RUN("L7"), div_type_terr_div_type_sk="C07_T2")])
    c.expect("L7", "snp.div_h", "C07_1", [ver(d("L0"), MAX, START, "L0")])
    c.expect("L7", "diff.terr_div_type", "C07_T2", [hleg("R", "I", RUN("L7")), hleg("U", "D", RUN("L7"))])


def c08():
    c = case(id="C08", title="div_type change on the 2nd of month (closure into previous month) and on the 15th",
             rules=["R13", "M1-hist-mover-truncation", "M2-loader-mover-window-contract"],
             changes=["HPWR-geq-threshold-month", "FX26-rewrite-scope"], scenario="S6", tables=["div_type", "terr_div_type"],
             summary="Type T1->T2 at L5 (Feb 2): old version closes at D-2 = Jan 31 (partition 202701, rewritten at L5). "
                     "T2->T3 at L6. At L7 the 202701 row must be unchanged.")
    c.div("C08_1", divisiontype00="C08_T1", at={"L5": dict(divisiontype00="C08_T2"), "L6": dict(divisiontype00="C08_T3")})
    a = ver(MIN, d("L5", -2), RUN("L5"), "L5", div_type_terr_div_type_sk="C08_T1", p1month="202701")
    c.expect("L5", "snp.div_type", "C08_1", [a, ver(d("L5", -1), MAX, RUN("L5"), "L5", div_type_terr_div_type_sk="C08_T2", p1month="999912")])
    final = [a, ver(d("L5", -1), d("L6", -2), RUN("L6"), "L6", div_type_terr_div_type_sk="C08_T2", p1month="202702"),
             ver(d("L6", -1), MAX, RUN("L6"), "L6", div_type_terr_div_type_sk="C08_T3", p1month="999912")]
    c.expect("L6", "snp.div_type", "C08_1", final)
    c.expect("L7", "snp.div_type", "C08_1", final)
    c.expect("L7", "snp.terr_div_type", "C08_T1", [hub(RUN("L0"), "L0")], note="old type stays in the hub")


def c09():
    c = case(id="C09", chain="samepk", title="div_type: second change on the same day (S1 for HistSnp)",
             rules=["R13", "R15"], changes=["HPWR-consec-day-relabel", "HPWR-silent-drop"], scenario="S1",
             tables=["div_type"],
             summary="T1->T2 at L1, T2->T3 at L2 (same day): T2 started D-1, cannot close at D-2 -> removed.")
    c.div("C09_1", divisiontype00="C09_T1", at={"K1": dict(divisiontype00="C09_T2"), "K2": dict(divisiontype00="C09_T3")})
    a = ver(MIN, d("K1", -2), RUN("K1"), "K1", div_type_terr_div_type_sk="C09_T1")
    c.expect("K1", "snp.div_type", "C09_1", [a, ver(d("K1", -1), MAX, RUN("K1"), "K1", div_type_terr_div_type_sk="C09_T2")])
    c.expect("K2", "snp.div_type", "C09_1", [a, ver(d("K2", -1), MAX, RUN("K2"), "K2", div_type_terr_div_type_sk="C09_T3")])
    c.expect("K2", "diff.div_type", "C09_1", [leg("R", "I", d("K2", -1), MAX, RUN("K2"), div_type_terr_div_type_sk="C09_T3"),
                                             leg("U", "D", d("K2", -1), MAX, RUN("K2"), div_type_terr_div_type_sk="C09_T3")])


def c10():
    c = case(id="C10", title="div_type: vanish at L1 and return at L2 on the same day",
             rules=["R13", "R14"], changes=["HPWR-vanish-divtype", "HPWR-returner-start-date"], tables=["div_type"],
             summary="Vanish closes at today-1 = %s; return the same day reopens at cached end+1 = %s (later than D-1)."
                     % (d("L1", -1), d("L1")))
    c.div("C10_1", at={"L1": GONE, "L2": {}})
    a = ver(MIN, d("L1", -1), RUN("L1"), "L1", p1month="202701")
    c.expect("L1", "snp.div_type", "C10_1", [a])
    c.expect("L1", "diff.div_type", "C10_1", [leg("R", "U", MIN, d("L1", -1), RUN("L1")), leg("U", "U", MIN, MAX, RUN("L1"))])
    c.expect("L2", "snp.div_type", "C10_1", [a, ver(d("L1"), MAX, RUN("L2"), "L2", p1month="999912")])
    c.expect("L2", "diff.div_type", "C10_1", [leg("R", "I", d("L1"), MAX, RUN("L2")), leg("U", "D", d("L1"), MAX, RUN("L2"))])
    c.expect("L2", "snp.div_h", "C10_1", [ver(d("L0"), MAX, START, "L0")])


def c11():
    c = case(id="C11", title="Brand-new division at INCR: start_dt = 1900-01-01 in every PERIOD table",
             rules=["R07", "R13", "L5-link-incr-outcomes", "H08-diff-legs-incr", "R08"],
             changes=["HPWR-new-key-start-1900"], tables=["div_h", "div_type", "div_", "addr", "addr_h", "terr_div_type"],
             summary="A key without an open version is NEW: [1900-01-01, 9999-12-31] (not D-1), legs (R,I)+(U,D). "
                     "Hubs: (R,I)+(U,D) with the run timestamp.")
    c.div("C11_1", start="L3", fullname="C11 A", address_locationaddress="C11 fact")
    k = addr_key(src="C11 fact")
    c.expect("L2", "snp.div_h", "C11_1", [])
    c.expect("L3", "snp.div_h", "C11_1", [ver(MIN, MAX, RUN("L3"), "L3", div_name="C11 A", fact_addr_sk=k)])
    c.expect("L3", "diff.div_h", "C11_1", [leg("R", "I", MIN, MAX, RUN("L3")), leg("U", "D", MIN, MAX, RUN("L3"))])
    c.expect("L3", "snp.div_type", "C11_1", [ver(MIN, MAX, RUN("L3"), "L3")])
    c.expect("L3", "diff.div_type", "C11_1", [leg("R", "I", MIN, MAX, RUN("L3")), leg("U", "D", MIN, MAX, RUN("L3"))])
    c.expect("L3", "snp.div_", "C11_1", [hub(RUN("L3"), "L3")])
    c.expect("L3", "diff.div_", "C11_1", [hleg("R", "I", RUN("L3")), hleg("U", "D", RUN("L3"))])
    c.expect("L3", "snp.addr", k, [hub(RUN("L3"), "L3")])
    c.expect("L3", "diff.addr", k, [hleg("R", "I", RUN("L3")), hleg("U", "D", RUN("L3"))])
    c.expect("L3", "snp.addr_h", k, [ver(MIN, MAX, RUN("L3"), "L3")])
    c.expect("L3", "diff.addr_h", k, [leg("R", "I", MIN, MAX, RUN("L3")), leg("U", "D", MIN, MAX, RUN("L3"))])
    c.expect("L3", "diff.terr_div_type", "C11_T", [hleg("R", "I", RUN("L3")), hleg("U", "D", RUN("L3"))])


def c12():
    c = case(id="C12", title="New division at L1, normal change at L3 (D-1 / D-2 chain starts from 1900)",
             rules=["R07", "R11"], changes=["HPWR-new-key-start-1900", "HPWR-close-date-loadDate-2"], tables=["div_h"],
             summary="NEW at L1 -> [1900, MAX]; change at L3 -> [1900, D-2] + [D-1, MAX].")
    c.div("C12_1", start="L1", fullname="C12 A", at={"L3": dict(fullname="C12 B")})
    c.expect("L1", "snp.div_h", "C12_1", [ver(MIN, MAX, RUN("L1"), "L1", div_name="C12 A")])
    c.expect("L3", "snp.div_h", "C12_1", [ver(MIN, d("L3", -2), RUN("L3"), "L3", div_name="C12 A", p1month="202701"),
                                         ver(d("L3", -1), MAX, RUN("L3"), "L3", div_name="C12 B", p1month="999912")])


def c30():
    c = case(id="C30", chain="samepk", title="Out-of-order and future-dated d_ versions at INIT; INCR next day overlaps them",
             rules=["R10", "S2-chgdivh-init", "R07", "R15"], changes=["HPWR-close-open-only", "HPWR-silent-drop"],
             tables=["div_h", "div_"],
             summary="d_ rows arrive with ctl_loading order != ctl_validfrom order; the chain is ordered by ctl_validfrom. "
                     "One d_ version is dated 2027-02-01 (after INIT): the snapshot version gets [Jan 13, Jan 31] and the "
                     "future one is open, so div_ takes short code '99' from it. At L1 the open (future) version cannot "
                     "close at D-2 (< its start) and is dropped; the new version [Jan 13, MAX] overlaps the INIT row "
                     "[Jan 13, Jan 31] with the same start_dt (same PK) - this is what 3.0 really does.")
    r = c.div("C30_1", fullname="C30 now")
    c.hist(r, "2026-01-05 10:00:00", 7, fullname="C30 v1")
    c.hist(r, "2026-07-15 10:00:00", 5, fullname="C30 v2")
    c.hist(r, "2026-03-20 10:00:00", 6, fullname="C30 v1b")
    c.hist(r, "2027-02-01 10:00:00", 8, fullname="C30 future", shortdivcode="99")
    hist = [ver("2026-01-05", "2026-03-19", START, "K0", div_name="C30 v1"),
            ver("2026-03-20", "2026-07-14", START, "K0", div_name="C30 v1b"),
            ver("2026-07-15", d("K0", -1), START, "K0", div_name="C30 v2"),
            ver(d("K0"), "2027-01-31", START, "K0", div_name="C30 now")]
    c.expect("K0", "snp.div_h", "C30_1", hist + [ver("2027-02-01", MAX, START, "K0", div_name="C30 future")])
    c.expect("K0", "snp.div_", "C30_1", [hub(RUN("K0"), "K0", short_div_code="99")])
    c.expect("K1", "snp.div_h", "C30_1", hist + [ver(d("K1", -1), MAX, RUN("K1"), "K1", div_name="C30 now")])
    c.expect("K1", "diff.div_h", "C30_1", [leg("R", "I", d("K1", -1), MAX, RUN("K1"), div_name="C30 now"),
                                          leg("U", "D", d("K1", -1), MAX, RUN("K1"), div_name="C30 now")])
    c.expect("K1", "diff.div_", "C30_1", [hleg("R", "U", RUN("K1"), short_div_code="01"), hleg("U", "U", RUN("K0"), short_div_code="99")])


for _f in [c01, c02, c03, c04, c06, c07, c08, c09, c10, c11, c12, c30]:
    _f()
