"""Hub tables (div_, terr_div_type, time_zone_rf): (R,U)+(U,U) legs, EQUALS retention, value mappings."""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case


def c05():
    c = case(id="C05", title="EQUALS keeps old ctl_*; attributes outside the div_h hash change only div_",
             rules=["R07", "R18", "H07-snp-incr", "H08-diff-legs-incr", "H14-ctl-semantics-summary"],
             changes=["HPWR-equals-retention", "NH-undo-keep-validfrom"], tables=["div_h", "div_"],
             summary="div_h has no short code / close date, so an L3 change of those leaves div_h untouched (INIT row "
                     "keeps START / L0 forever). div_ (hub) gets (R,U) with new values and (U,U) with old values "
                     "carrying the OLD row's ctl_validfrom (RUN@L0).")
    r = c.div("C05_1", fullname="C05 A", at={"L3": dict(shortdivcode="02", closedate="2027-01-10 00:00:00")})
    c.hist(r, "2026-02-02 10:00:00", 1)
    c.expect("L3", "snp.div_h", "C05_1", [ver("2026-02-02", MAX, START, "L0", div_name="C05 A")])
    c.expect("L3", "diff.div_h", "C05_1", [])
    c.expect("L0", "snp.div_", "C05_1", [hub(RUN("L0"), "L0", short_div_code="01", div_close_dt=None)])
    c.expect("L3", "snp.div_", "C05_1", [hub(RUN("L3"), "L3", short_div_code="02", div_close_dt="2027-01-10")])
    c.expect("L3", "diff.div_", "C05_1", [hleg("R", "U", RUN("L3"), short_div_code="02", div_close_dt="2027-01-10"),
                                         hleg("U", "U", RUN("L0"), short_div_code="01", div_close_dt=None)])
    c.expect("L7", "snp.div_h", "C05_1", [ver("2026-02-02", MAX, START, "L0", div_name="C05 A")])


def c24():
    c = case(id="C24", title="div_ hub: close date, time zone (USZ1->KALT), parent change; vanished key stays",
             rules=["H07-snp-incr", "H08-diff-legs-incr", "H02-input-is-full-snapshot", "H12-deletes", "H14-ctl-semantics-summary"],
             changes=["NH-undo-keep-validfrom", "NH-hash-essa-xxhash-concat"], tables=["div_"],
             summary="Each change: (R,U) new values with RUN of this load + (U,U) previous values with the previous "
                     "ctl_validfrom. Address time zone 'USZ1' is mapped to 'KALT'. The key vanishes at L6 and keeps "
                     "the L4 row forever.")
    c.div("C24_P", fullname="C24 parent 1")
    c.div("C24_P2", fullname="C24 parent 2")
    c.tz("MSK", code=1, abbreviation="MSK", name00="Московское", timedesignation="0", utcformat="+3")
    c.owns("MSK", "time zone row MSK is created by this case")
    c.div("C24_1", address_addrfact_timezone="USZ1", headdivisioncode="C24_P",
          at={"L3": dict(closedate="2027-01-10 00:00:00"),
              "L4": dict(address_addrfact_timezone="MSK", headdivisioncode="C24_P2"), "L6": GONE})
    c.expect("L0", "snp.div_", "C24_1", [hub(RUN("L0"), "L0", div_time_zone_time_zone_rf_sk="KALT", parent_div_sk="C24_P", div_close_dt=None)])
    c.expect("L3", "diff.div_", "C24_1", [hleg("R", "U", RUN("L3"), div_close_dt="2027-01-10"), hleg("U", "U", RUN("L0"), div_close_dt=None)])
    c.expect("L4", "diff.div_", "C24_1", [
        hleg("R", "U", RUN("L4"), div_time_zone_time_zone_rf_sk="MSK", parent_div_sk="C24_P2", div_close_dt="2027-01-10"),
        hleg("U", "U", RUN("L3"), div_time_zone_time_zone_rf_sk="KALT", parent_div_sk="C24_P", div_close_dt="2027-01-10")])
    c.expect("L7", "snp.div_", "C24_1", [hub(RUN("L4"), "L4", div_time_zone_time_zone_rf_sk="MSK", parent_div_sk="C24_P2", div_close_dt="2027-01-10")])


def c25():
    c = case(id="C25", title="div_.reg_ord parsing variants (int cast into BIGINT column)",
             rules=["H03-prep", "H05-hash"], changes=[], tables=["div_"],
             summary="reg_ord = int(part after the last '/') of the registration number; 'ПАО Сбербанк' takes the whole "
                     "number; '-' means NULL. 3.0 writes the int into the BIGINT column (matchSchemas by name). "
                     "Known 4.0-baseline defect: reg_ord is NULL for every row (hooks-design section 8).")
    variants = [("C25_1", "1481/7", None, "7", "1481/7"),
                ("C25_2", "1481/007", None, "7", "1481/007"),
                ("C25_3", "1481/abc", None, None, "1481/abc"),
                ("C25_4", "-", None, None, None),
                ("C25_5", "1481", "ПАО Сбербанк", "1481", "1481"),
                ("C25_6", "1481/99999999999", None, None, "1481/99999999999"),
                ("C25_7", None, None, None, None),
                ("C25_8", "1481/ 5", None, "5", "1481/ 5")]
    for key, regnum, name, reg_ord, full in variants:
        kw = dict(requisitesdivision_registrationnum=regnum)
        if name:
            kw["fullname"] = name
        if key == "C25_1":
            kw["at"] = {"L3": dict(requisitesdivision_registrationnum="1481/8")}
        c.div(key, **kw)
        c.expect("L0", "snp.div_", key, [hub(RUN("L0"), "L0", reg_ord=reg_ord, full_reg_num=full)],
                 note="regnum %r -> reg_ord %s" % (regnum, reg_ord))
    c.expect("L3", "diff.div_", "C25_1", [hleg("R", "U", RUN("L3"), reg_ord="8", full_reg_num="1481/8"),
                                         hleg("U", "U", RUN("L0"), reg_ord="7", full_reg_num="1481/7")])


def c26():
    c = case(id="C26", title="terr_div_type hub: new type at INCR, old type stays, NULL type (NULL key)",
             rules=["H13-key-dedup", "H07-snp-incr", "H12-deletes", "R13"], changes=["NH-hash-essa-xxhash-concat"],
             tables=["terr_div_type", "div_type"],
             summary="C26_1 switches type A->B at L3: B is a NEW hub row, A stays. C26_2 has divisiontype00 NULL: the hub "
                     "gets one row with NULL key (sid/sk/div_type_code NULL) at INIT; at INCR 3.0 groups NULL keys in the "
                     "window -> EQUALS -> the row is never touched again (section 7.2 'NULL keys').")
    c.div("C26_1", divisiontype00="C26_A", at={"L3": dict(divisiontype00="C26_B")})
    c.div("C26_2", divisiontype00=None)
    c.owns(None, "NULL natural key (divisiontype00 NULL of C26_2)")
    c.expect("L0", "snp.terr_div_type", None, [hub(RUN("L0"), "L0", div_type_code=None)])
    c.expect("L7", "snp.terr_div_type", None, [hub(RUN("L0"), "L0", div_type_code=None)])
    c.expect("L1", "diff.terr_div_type", None, [])
    c.expect("L3", "diff.terr_div_type", "C26_B", [hleg("R", "I", RUN("L3"), div_type_code="C26_B"), hleg("U", "D", RUN("L3"), div_type_code="C26_B")])
    c.expect("L3", "snp.terr_div_type", "C26_A", [hub(RUN("L0"), "L0", div_type_code="C26_A")])
    c.expect("L3", "snp.div_type", "C26_1", [ver(MIN, d("L3", -2), RUN("L3"), "L3", div_type_terr_div_type_sk="C26_A"),
                                            ver(d("L3", -1), MAX, RUN("L3"), "L3", div_type_terr_div_type_sk="C26_B")])
    c.expect("L7", "snp.div_type", "C26_2", [ver(MIN, MAX, RUN("L0"), "L0", div_type_terr_div_type_sk=None)])


def c27():
    c = case(id="C27", title="time_zone_rf hub: attribute change, USZ1+KALT collapse, NULL abbreviation, vanish",
             rules=["H13-key-dedup", "H07-snp-incr", "H08-diff-legs-incr", "H02-input-is-full-snapshot"],
             changes=["NH-undo-keep-validfrom"], tables=["time_zone_rf"],
             summary="USZ1 is renamed to KALT; with KALT present and identical attributes distinct leaves one row. "
                     "A row without abbreviation is filtered out. code is INT in the source and STRING in the target. "
                     "C27_B vanishes at L5 and stays.")
    c.owns("KALT", "USZ1/KALT time zone rows are created by this case")
    c.tz("C27_A", code=27, abbreviation="C27_A", name00="C27 A", timedesignation="+1", utcformat="+4", at={"L3": dict(utcformat="+5")})
    c.tz("C27_USZ1", code=2, abbreviation="USZ1", name00="Калининградское", timedesignation="-1", utcformat="+2")
    c.tz("C27_KALT", code=2, abbreviation="KALT", name00="Калининградское", timedesignation="-1", utcformat="+2")
    c.tz("C27_noabbr", code=99, abbreviation=None, name00="C27 no abbreviation", timedesignation="0", utcformat="+3")
    c.tz("C27_B", code=28, abbreviation="C27_B", name00="C27 B", timedesignation="+2", utcformat="+5", at={"L5": GONE})
    c.expect("L0", "snp.time_zone_rf", "KALT", [hub(RUN("L0"), "L0", time_zone_abbr_code="KALT", time_zone_code="2")])
    c.expect("L0", "snp.time_zone_rf", "C27_A", [hub(RUN("L0"), "L0", time_zone_code="27", time_zone_wrld_shift_val="+4")])
    c.expect("L3", "diff.time_zone_rf", "C27_A", [hleg("R", "U", RUN("L3"), time_zone_wrld_shift_val="+5"),
                                                 hleg("U", "U", RUN("L0"), time_zone_wrld_shift_val="+4")])
    c.expect("L7", "snp.time_zone_rf", "C27_B", [hub(RUN("L0"), "L0")])
    c.expect("L7", "snp.time_zone_rf", "KALT", [hub(RUN("L0"), "L0")])


for _f in [c05, c24, c25, c26, c27]:
    _f()
