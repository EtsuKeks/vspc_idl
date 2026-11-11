"""Run modes: empty sources, partial arrival, rerun, cutover."""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case, DV77


def e01():
    c = case(id="E01", chain="empty", title="S5: INCR with an empty sfsdivision snapshot; the divisions return two loads later",
             rules=["R19", "H11-empty-incr", "R13"], changes=["HPWR-vanish-divtype"], scenario="S5",
             tables=["div_type", "div_h", "div_"],
             summary="3.0 runs every loader on empty input: div_type closes ALL open versions at today-1 with (R,U)+(U,U); "
                     "hubs, div_h, addr_h and the link stay unchanged. 4.0 failed fast in "
                     "DivTypeTransformation in the 2026-11-08 baseline (fixed since). "
                     "E2 (everything empty): nothing more changes. E3: the divisions return -> div_type reopens at the "
                     "closed end + 1 (= E1 date), div_h/div_ untouched.")
    c.div("E01_1", at={"E1": GONE, "E3": {}})
    c.div("E01_2", at={"E1": GONE, "E3": {}})
    for k in ("E01_1", "E01_2"):
        c.expect("E0", "snp.div_type", k, [ver(MIN, MAX, RUN("E0"), "E0")])
        c.expect("E1", "snp.div_type", k, [ver(MIN, d("E1", -1), RUN("E1"), "E1")])
        c.expect("E1", "diff.div_type", k, [leg("R", "U", MIN, d("E1", -1), RUN("E1")), leg("U", "U", MIN, MAX, RUN("E1"))])
        c.expect("E1", "snp.div_h", k, [ver(d("E0"), MAX, START, "E0")])
        c.expect("E1", "snp.div_", k, [hub(RUN("E0"), "E0")])
        c.expect("E2", "snp.div_type", k, [ver(MIN, d("E1", -1), RUN("E1"), "E1")])
        c.expect("E2", "diff.div_type", k, [])
        c.expect("E3", "snp.div_type", k, [ver(MIN, d("E1", -1), RUN("E1"), "E1"), ver(d("E1"), MAX, RUN("E3"), "E3")],
                 note="returner: start = closed end + 1 = %s, not D-1" % d("E1"))
        c.expect("E3", "diff.div_type", k, [leg("R", "I", d("E1"), MAX, RUN("E3")), leg("U", "D", d("E1"), MAX, RUN("E3"))])
        c.expect("E3", "snp.div_h", k, [ver(d("E0"), MAX, START, "E0")])


def e02():
    c = case(id="E02", chain="empty", title="INCR with empty sfsdivision keeps the link and time zones; then every source empty",
             rules=["R19", "H11-empty-incr", "H12-deletes", "L6-link-effective"], changes=[], tables=["div_div_relation", "time_zone_rf"],
             summary="E1: successors LEFT JOIN the empty snapshot give source_sid = successor_sid = NULL -> a NEW relation "
                     "'NULL|NULL' [MIN, MAX] with (R,I)+(U,D); E02_1|E02_2 is not in the input and stays open (no deletes "
                     "in the link loader), no legs; rfzonetime is unchanged -> no legs. E2: rfzonetime and successors "
                     "empty too -> the hub keeps its row, both relations stay open, no legs anywhere.")
    c.tz("E02_TZ", code=202, abbreviation="E02_TZ", name00="E02 zone", timedesignation="0", utcformat="+3", at={"E2": GONE})
    c.div("E02_1", at={"E1": GONE})
    c.div("E02_2", at={"E1": GONE})
    c.succ("E02_1", "E02_2", "правопреемник", at={"E2": GONE})
    rel = [ver(MIN, MAX, RUN("E0"), "E0")]
    for ld in ("E1", "E2"):
        c.expect(ld, "snp.div_div_relation", "E02_1|E02_2", rel)
        c.expect(ld, "diff.div_div_relation", "E02_1|E02_2", [])
        c.expect(ld, "snp.time_zone_rf", "E02_TZ", [hub(RUN("E0"), "E0")])
        c.expect(ld, "diff.time_zone_rf", "E02_TZ", [])
    c.owns("NULL|NULL", "relation of the successors joined to the empty sfsdivision on E1")
    nn = [ver(MIN, MAX, RUN("E1"), "E1", source_div_sk=None, successor_div_sk=None)]
    c.expect("E1", "snp.div_div_relation", "NULL|NULL", nn, note="NEW in INCR -> start_dt = MIN, not D-1")
    c.expect("E1", "diff.div_div_relation", "NULL|NULL", [leg("R", "I", MIN, MAX, RUN("E1")), leg("U", "D", MIN, MAX, RUN("E1"))])
    c.expect("E2", "snp.div_div_relation", "NULL|NULL", nn, note="empty link input: nothing new, nothing closed")
    c.expect("E2", "diff.div_div_relation", "NULL|NULL", [])


def b01():
    c = case(id="B01", chain="emptysrc", title="One non-sfsdivision source empty at a time (rfzonetime, successors, gar, okato/oktmo, pay)",
             rules=["H02-input-is-full-snapshot", "H12-deletes", "S3-chgdivh-incr", "R07", "R11"], changes=[],
             tables=["time_zone_rf", "div_div_relation", "addr", "addr_h", "div_", "div_h"],
             summary="S1: rfzonetime and successors empty -> time_zone_rf keeps its row, the relation stays open, no "
                     "legs (empty PU input). S2: both back (EQUALS, nothing), the four reference tables empty: the gold "
                     "fias/okato/oktmo values disappear -> the legal address gets a NEW key '<fias>|B01 legal|NULL|NULL' "
                     "(new addr/addr_h rows, div_.legal_addr_sk moves, the old address row stays); the BIC loses its "
                     "pay_sys_prtcpnt_sk -> div_h closes at D-2 and opens at D-1.")
    r = c.div("B01_1", address_legaladdrdoc="B01 legal", address_addrfact_fiascode="b01-fias",
              address_addrfact_okatocode="b01okato", address_addrfact_oktmocode="b01oktmo", requisitesdivision_bic="B01BIC")
    c.hist(r, "2026-06-06 10:00:00", 1)
    c.div("B01_2")
    c.tz("B01_TZ", code=301, abbreviation="B01_TZ", name00="B01 zone", timedesignation="0", utcformat="+3", at={"S1": GONE, "S2": {}})
    c.succ("B01_1", "B01_2", "правопреемник", at={"S1": GONE, "S2": {}})
    for t, sid in (("gar", "b01-fias"), ("okato", "b01okato"), ("oktmo", "b01oktmo"), ("pay", "B01BIC")):
        c.ref(t, sid, at={"S2": GONE})
    k0 = addr_key(durty="b01-fias", src="B01 legal", okato="b01okato", oktmo="b01oktmo")
    k1 = addr_key(durty="b01-fias", src="B01 legal")
    for ld in ("S1", "S2"):
        c.expect(ld, "snp.time_zone_rf", "B01_TZ", [hub(RUN("S0"), "S0")])
        c.expect(ld, "diff.time_zone_rf", "B01_TZ", [])
        c.expect(ld, "snp.div_div_relation", "B01_1|B01_2", [ver(MIN, MAX, RUN("S0"), "S0")])
        c.expect(ld, "diff.div_div_relation", "B01_1|B01_2", [])
    c.expect("S1", "diff.addr", k0, [])
    c.expect("S2", "snp.addr", k0, [hub(RUN("S0"), "S0", fias_code="b01-fias")])
    c.expect("S2", "snp.addr", k1, [hub(RUN("S2"), "S2", fias_code=None, okato2_sk=None)])
    c.expect("S2", "diff.addr", k1, [hleg("R", "I", RUN("S2")), hleg("U", "D", RUN("S2"))])
    c.expect("S2", "snp.addr_h", k1, [ver(MIN, MAX, RUN("S2"), "S2")])
    c.expect("S2", "diff.div_", "B01_1", [hleg("R", "U", RUN("S2"), legal_addr_sk=k1), hleg("U", "U", RUN("S0"), legal_addr_sk=k0)])
    c.expect("S2", "snp.div_h", "B01_1", [ver("2026-06-06", d("S2", -2), RUN("S2"), "S2", pay_sys_prtcpnt_sk=NOTNULL),
                                         ver(d("S2", -1), MAX, RUN("S2"), "S2", pay_sys_prtcpnt_sk=None)])


def z01():
    c = case(id="Z01", chain="cold", title="Loads onto empty targets; INIT with every source empty",
             rules=["R19", "H11-empty-incr", "H09-init", "R07", "R13"], changes=["HPWR-new-key-start-1900"],
             tables=["div_h", "div_", "div_type", "div_div_relation", "time_zone_rf", "addr", "addr_h", "terr_div_type"],
             summary="Z0: INCR on never-initialised targets with empty sources: no rows, SUCCESS. Z1: INCR with data "
                     "onto the empty targets: every key is NEW ([1900, 9999], (R,I)+(U,D)). Z2: INIT with every source "
                     "empty over the loaded targets: every snp and diff table becomes empty, SUCCESS (D-05). Z3: the "
                     "same INIT on empty targets: still empty, SUCCESS.")
    c.div("Z01_1", start="Z1", address_locationaddress="Z01 fact", at={"Z2": GONE})
    c.div("Z01_2", start="Z1", at={"Z2": GONE})
    c.tz("Z01_TZ", start="Z1", code=401, abbreviation="Z01_TZ", name00="Z01 zone", timedesignation="0", utcformat="+3", at={"Z2": GONE})
    c.succ("Z01_1", "Z01_2", "правопреемник", start="Z1", at={"Z2": GONE})
    k = addr_key(src="Z01 fact")
    new = [ver(MIN, MAX, RUN("Z1"), "Z1")]
    legs = [leg("R", "I", MIN, MAX, RUN("Z1")), leg("U", "D", MIN, MAX, RUN("Z1"))]
    hlegs = [hleg("R", "I", RUN("Z1")), hleg("U", "D", RUN("Z1"))]
    checks = [("div_h", "Z01_1", new, legs), ("div_type", "Z01_1", new, legs), ("div_div_relation", "Z01_1|Z01_2", new, legs),
              ("addr_h", k, new, legs), ("div_", "Z01_1", [hub(RUN("Z1"), "Z1")], hlegs), ("addr", k, [hub(RUN("Z1"), "Z1")], hlegs),
              ("time_zone_rf", "Z01_TZ", [hub(RUN("Z1"), "Z1")], hlegs), ("terr_div_type", "Z01_T", [hub(RUN("Z1"), "Z1")], hlegs)]
    for t, key, snp, diff in checks:
        c.expect("Z0", "snp." + t, key, [])
        c.expect("Z1", "snp." + t, key, snp)
        c.expect("Z1", "diff." + t, key, diff)
        for ld in ("Z2", "Z3"):
            c.expect(ld, "snp." + t, key, [], note="INIT with empty input empties the table")
            c.expect(ld, "diff." + t, key, [])


def i01():
    c = case(id="I01", chain="emptyinit", title="INIT over data with rfzonetime + successors empty, then with only d_ history",
             rules=["H09-init", "R10", "S2-chgdivh-init"], changes=[], tables=["time_zone_rf", "div_div_relation", "div_h", "div_", "div_type"],
             summary="I1: INIT again with rfzonetime and successors empty: time_zone_rf and div_div_relation become empty, "
                     "the other tables are rebuilt from the data (stamped I1). I2: INIT with the sfsdivision snapshot "
                     "empty but d_ history present: div_type and terr_div_type become empty (they read only the "
                     "snapshot); div_h/div_ are built from what chg_div_h INIT yields for the d_ rows alone. I3: snapshot and "
                     "d_ empty, the other sources present: every division table is emptied; time_zone_rf is rebuilt.")
    r = c.div("I01_1", fullname="I01 now", address_locationaddress="I01 fact", at={"I2": GONE})
    c.hist(r, "2026-05-05 10:00:00", 1, fullname="I01 hist", at={"I3": GONE})
    c.div("I01_2", at={"I2": GONE})
    c.tz("I01_TZ", code=501, abbreviation="I01_TZ", name00="I01 zone", timedesignation="0", utcformat="+3", at={"I1": GONE, "I2": {}})
    c.succ("I01_1", "I01_2", "правопреемник", at={"I1": GONE, "I2": {}})
    c.expect("I0", "snp.div_h", "I01_1", [ver("2026-05-05", d("I0", -1), START, "I0", div_name="I01 hist"),
                                         ver(d("I0"), MAX, START, "I0", div_name="I01 now")])
    c.expect("I1", "snp.time_zone_rf", "I01_TZ", [])
    c.expect("I1", "snp.div_div_relation", "I01_1|I01_2", [])
    c.expect("I1", "snp.div_h", "I01_1", [ver("2026-05-05", d("I1", -1), START, "I1", div_name="I01 hist"),
                                         ver(d("I1"), MAX, START, "I1", div_name="I01 now")],
             note="rebuilt by the second INIT: the snapshot version now starts at the I1 date")
    c.expect("I1", "snp.div_", "I01_1", [hub(RUN("I1"), "I1")])
    c.expect("I2", "snp.div_type", "I01_1", [])
    c.expect("I2", "snp.terr_div_type", "I01_T", [])
    c.expect("I2", "snp.time_zone_rf", "I01_TZ", [hub(RUN("I2"), "I2")])
    for t, k in (("div_h", "I01_1"), ("div_", "I01_1"), ("div_type", "I01_1"), ("addr", addr_key(src="I01 fact"))):
        c.expect("I3", "snp." + t, k, [], note="INIT with empty snapshot and d_: emptied")
    c.expect("I3", "snp.time_zone_rf", "I01_TZ", [hub(RUN("I3"), "I3")])
    c.expect("I3", "snp.div_div_relation", "I01_1|I01_2", [], note="successors present, but both divisions are unknown")


def p01():
    c = case(id="P01", chain="partial", title="Key set replaced ({A:T} -> {B:T}), then 110 of 150 open keys vanish",
             rules=["R13", "R14", "R07"], changes=["HPWR-vanish-divtype", "HPWR-new-key-start-1900"], tables=["div_type", "div_h"],
             summary="P1: A vanishes and B arrives - every open div_type version vanishes: A closes at today-1 "
                     "((R,U)+(U,U)), B is NEW [1900, 9999]; div_h keeps A open. P2: 149 new keys. P3: 110 of the 150 "
                     "open keys vanish (73%): each closes at today-1; there is no threshold in 3.0.")
    c.div("P01_A", at={"P1": GONE})
    c.div("P01_B", start="P1")
    ns = ["P01_N%03d" % i for i in range(1, 150)]
    keep = 39
    for i, k in enumerate(ns):
        c.div(k, start="P2", at={"P3": GONE} if i >= keep else None)
    c.expect("P1", "snp.div_type", "P01_A", [ver(MIN, d("P1", -1), RUN("P1"), "P1")])
    c.expect("P1", "diff.div_type", "P01_A", [leg("R", "U", MIN, d("P1", -1), RUN("P1")), leg("U", "U", MIN, MAX, RUN("P1"))])
    c.expect("P1", "snp.div_type", "P01_B", [ver(MIN, MAX, RUN("P1"), "P1")])
    c.expect("P1", "diff.div_type", "P01_B", [leg("R", "I", MIN, MAX, RUN("P1")), leg("U", "D", MIN, MAX, RUN("P1"))])
    c.expect("P1", "snp.div_h", "P01_A", [ver(d("P0"), MAX, START, "P0")])
    c.expect("P2", "snp.div_type", "P01_N001", [ver(MIN, MAX, RUN("P2"), "P2")])
    for k in ns[keep:]:
        c.expect("P3", "snp.div_type", k, [ver(MIN, d("P3", -1), RUN("P3"), "P3")])
        c.expect("P3", "diff.div_type", k, [leg("R", "U", MIN, d("P3", -1), RUN("P3")), leg("U", "U", MIN, MAX, RUN("P3"))])
    for k in ns[:keep] + ["P01_B"]:
        c.expect("P3", "diff.div_type", k, [])
    c.expect("P3", "snp.div_type", "P01_N039", [ver(MIN, MAX, RUN("P2"), "P2")])


def r01():
    c = case(id="R01", chain="rerun", title="A completed load rerun with the same loadingId (INIT and INCR)",
             known40="phase 3 (j0164-j0205): same as 3.0 including the no-op (R,U)+(U,U) pairs of the R3 rerun (DV-08)",
             rules=["H09-init", "H07-snp-incr", "R07", "R13"], changes=[], tables=["div_h", "div_", "div_type"],
             summary="R1 = R0 again (same loadingId 6000): INIT rewrites everything, stamped with the rerun. R3 = R2 "
                     "again (same loadingId 6001, same sources): every snp table stays as after R2, but 3.0 appends a no-op "
                     "(R,U)+(U,U) pair (old ctl_validfrom) for every row R2 itself wrote and that is still open "
                     "(R01_1's new div_h version, R01_3 in div_h and div_type): rows with the current ctl_loading are not "
                     "EQUALS for 3.0. (ctl_loading of both runs shows the first label.)")
    r = c.div("R01_1", fullname="R01 A", at={"R2": dict(fullname="R01 B")})
    c.hist(r, "2026-04-04 10:00:00", 1)
    c.div("R01_2", at={"R2": GONE})
    c.div("R01_3", start="R2")
    c.expect("R1", "snp.div_", "R01_1", [hub(RUN("R1"), "R0")])
    c.expect("R1", "snp.div_h", "R01_1", [ver("2026-04-04", MAX, START, "R0", div_name="R01 A")])
    after = {"div_h": ("R01_1", [ver("2026-04-04", d("R2", -2), RUN("R2"), "R2", div_name="R01 A"),
                                  ver(d("R2", -1), MAX, RUN("R2"), "R2", div_name="R01 B")]),
             "div_type": ("R01_2", [ver(MIN, d("R2", -1), RUN("R2"), "R2")])}
    for t, (k, rows) in after.items():
        c.expect("R2", "snp." + t, k, rows)
        c.expect("R3", "snp." + t, k, rows, note="rerun with the same loadingId: unchanged")
    c.expect("R3", "snp.div_type", "R01_3", [ver(MIN, MAX, RUN("R2"), "R2")])
    c.expect("R3", "diff.div_h", "R01_1", [leg("R", "I", d("R2", -1), MAX, RUN("R2"), div_name="R01 B"),
                                          leg("U", "D", d("R2", -1), MAX, RUN("R2"), div_name="R01 B"),
                                          leg("R", "U", "2026-04-04", d("R2", -2), RUN("R2"), div_name="R01 A"),
                                          leg("U", "U", "2026-04-04", MAX, RUN("R2"), div_name="R01 A"),
                                          leg("R", "U", d("R2", -1), MAX, RUN("R2"), div_name="R01 B"),
                                          leg("U", "U", d("R2", -1), MAX, RUN("R2"), div_name="R01 B")],
             note="the R2 legs plus a no-op (R,U)+(U,U) pair from the rerun: rows stored with the current ctl_loading "
                  "are not EQUALS for 3.0 (desc(ctl_loading) window, cf. D-29)")
    c.expect("R3", "diff.div_type", "R01_3", [leg("R", "I", MIN, MAX, RUN("R2")), leg("U", "D", MIN, MAX, RUN("R2")),
                                             leg("R", "U", MIN, MAX, RUN("R2")), leg("U", "U", MIN, MAX, RUN("R2"))])
    c.expect("R3", "snp.div_", "R01_3", [hub(RUN("R2"), "R2")])


def g01():
    c = case(id="G01", accepted=DV77, accept=["div_type:G01_6"], chain="cutover", title="Cutover: first INCR after a 3.0 INIT (4.0 runs it over the 3.0 targets)",
             rules=["R07", "R11", "R13", "H07-snp-incr"], changes=[], tables=["div_h", "div_type", "div_", "terr_div_type"],
             deterministic=False,
             known40="phase 3 (j0175/j0196): G1 matches; G01_6 is a window tie in both versions (1 or 2 rows [D-1, MAX], DV-77)",
             summary="G0 (3.0 only) writes the targets, G1 runs on both: G01_1 changes its name (close D-2 / open D-1), "
                     "G01_2 vanishes (div_type closed at today-1), G01_3 is new (1900); G01_4 has reg number "
                     "'1481/3000000000' (3.0 reg_ord NULL, no change); G01_5 has no type (NULL-key hub row, untouched); "
                     "G01_6 has two types at INIT (duplicate div_type PK written by 3.0); at G1 3.0 keeps one or both rows, "
                     "always relabelled to [D-1, MAX] (window tie of the twins, DV-77; observed: TA alone, or TA+TB).")
    r = c.div("G01_1", fullname="G01 A", at={"G1": dict(fullname="G01 B")})
    c.hist(r, "2026-03-03 10:00:00", 1)
    c.div("G01_2", at={"G1": GONE})
    c.div("G01_3", start="G1")
    c.div("G01_4", requisitesdivision_registrationnum="1481/3000000000")
    c.div("G01_5", divisiontype00=None)
    c.owns(None, "NULL divisiontype00 of G01_5")
    c.div("G01_6", label="G01_6#a", divisiontype00="G01_TA")
    c.div("G01_6", label="G01_6#b", divisiontype00="G01_TB")
    c.expect("G0", "snp.div_type", "G01_6", [ver(MIN, MAX, RUN("G0"), "G0", div_type_terr_div_type_sk="G01_TA"),
                                            ver(MIN, MAX, RUN("G0"), "G0", div_type_terr_div_type_sk="G01_TB")])
    # stable projection ">= 1 open [D-1, MAX], no 1900 version": the row count (1 or 2, window tie) cannot be asserted,
    # check.py matches exact multisets, so it is a note only
    c.note("G1", "G01_6 snp.div_type: >= 1 open version [%s, MAX] @G1 and no [1900, ..] version; 1 row (TA) or 2 (TA+TB) "
                 "depending on the window tie, legs (R,I)+(U,D) or 4 legs; not asserted" % d("G1", -1))
    c.expect("G1", "snp.div_h", "G01_1", [ver("2026-03-03", d("G1", -2), RUN("G1"), "G1", div_name="G01 A"),
                                         ver(d("G1", -1), MAX, RUN("G1"), "G1", div_name="G01 B")])
    c.expect("G1", "snp.div_type", "G01_2", [ver(MIN, d("G1", -1), RUN("G1"), "G1")])
    c.expect("G1", "snp.div_type", "G01_3", [ver(MIN, MAX, RUN("G1"), "G1")])
    c.expect("G1", "snp.div_", "G01_4", [hub(RUN("G0"), "G0", reg_ord=None)])
    c.expect("G1", "diff.div_", "G01_4", [])
    c.expect("G1", "snp.terr_div_type", None, [hub(RUN("G0"), "G0")])
    c.expect("G1", "diff.terr_div_type", None, [])


def d01():
    c = case(id="D01", chain="sameday", title="INIT, INCR, INCR on one calendar day (versions before the INIT date)",
             rules=["R07", "R11", "R13", "R14", "R15", "L9-histmix-incr"],
             changes=["HPWR-consec-day-relabel", "HPWR-silent-drop", "HPWR-new-key-start-1900", "HPWR-returner-start-date"],
             tables=["div_h", "div_type"],
             summary="D01_1 (snapshot only, INIT version starts on the INIT date) changes at D1: the new version starts "
                     "D-1 (before the INIT version), the INIT version would close at D-2 < its start and is removed; at "
                     "D2 the D1 version started D-1 and is relabelled. D01_2 (d_ history) closes normally at D-2 at D1, "
                     "and at D2 its D1 version is relabelled. D01_3 is new at D1 (1900). D01_4 vanishes at D1 (div_type "
                     "closed at today-1) and returns at D2 (start = closed end + 1 = the INIT date).")
    c.div("D01_1", fullname="D01 A", at={"D1": dict(fullname="D01 B"), "D2": dict(fullname="D01 C")})
    r = c.div("D01_2", fullname="D01 A", at={"D1": dict(fullname="D01 B"), "D2": dict(fullname="D01 C")})
    c.hist(r, "2026-05-05 10:00:00", 1)
    c.div("D01_3", start="D1")
    c.div("D01_4", at={"D1": GONE, "D2": {}})
    c.expect("D1", "snp.div_h", "D01_1", [ver(d("D1", -1), MAX, RUN("D1"), "D1", div_name="D01 B")])
    c.expect("D2", "snp.div_h", "D01_1", [ver(d("D2", -1), MAX, RUN("D2"), "D2", div_name="D01 C")])
    closed = ver("2026-05-05", d("D1", -2), RUN("D1"), "D1", div_name="D01 A")
    c.expect("D1", "snp.div_h", "D01_2", [closed, ver(d("D1", -1), MAX, RUN("D1"), "D1", div_name="D01 B")])
    c.expect("D2", "snp.div_h", "D01_2", [closed, ver(d("D2", -1), MAX, RUN("D2"), "D2", div_name="D01 C")])
    c.expect("D1", "snp.div_type", "D01_3", [ver(MIN, MAX, RUN("D1"), "D1")])
    c.expect("D1", "snp.div_h", "D01_3", [ver(MIN, MAX, RUN("D1"), "D1")])
    c.expect("D1", "snp.div_type", "D01_4", [ver(MIN, d("D1", -1), RUN("D1"), "D1")])
    c.expect("D2", "snp.div_type", "D01_4", [ver(MIN, d("D1", -1), RUN("D1"), "D1"), ver(d("D1"), MAX, RUN("D2"), "D2")])


for _f in [e01, e02, b01, z01, i01, p01, r01, g01, d01]:
    _f()
