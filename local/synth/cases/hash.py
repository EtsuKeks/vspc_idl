"""Change detection hash corner cases (no separator, NULL vs '')."""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case


def c15():
    c = case(id="C15", title="Hub hash without separator: house_num '12'+idx '3456' == '123'+'456' -> no change in 3.0",
             rules=["H05-hash"], changes=["NH-hash-essa-xxhash-concat", "HPWR-hash-xxhash64-noseparator"], scenario="S4",
             tables=["addr"],
             summary="addr hash = xxhash64(concat of columns in DDL order, no separator); house_num and idx_num are "
                     "adjacent. Moving a digit across the border keeps the concatenation -> 3.0 sees EQUALS and keeps "
                     "the old values (house '12', idx '3456', ctl from L1).")
    c.div("C15_1", address_locationaddress="C15 fact", address_addrfact_addrhouse_housenumbers="12",
          address_addrfact_postcode="3456",
          at={"L3": dict(address_addrfact_addrhouse_housenumbers="123", address_addrfact_postcode="456")})
    k = addr_key(src="C15 fact")
    row = [hub(RUN("L1"), "L1", house_num="12", idx_num="3456")]
    c.expect("L1", "snp.addr", k, row, note="L1 change is legitimate (idx NULL at INIT)")
    c.expect("L3", "snp.addr", k, row)
    c.expect("L3", "diff.addr", k, [])


def c16():
    c = case(id="C16", title="'|' inside values: house '1|2'+idx '3' vs '1'+'2|3' -> a real change for 3.0",
             rules=["H05-hash"], changes=["NH-hash-essa-xxhash-concat"], scenario="S4", tables=["addr"],
             summary="3.0 concatenation without separator: '1|23' vs '12|3' differ -> NOTEQUALS. A hash that joins "
                     "columns with '|' would see '1|2|3' twice and miss the change.")
    c.div("C16_1", address_locationaddress="C16 fact", address_addrfact_addrhouse_housenumbers="1|2",
          address_addrfact_postcode="3",
          at={"L3": dict(address_addrfact_addrhouse_housenumbers="1", address_addrfact_postcode="2|3")})
    k = addr_key(src="C16 fact")
    c.expect("L3", "snp.addr", k, [hub(RUN("L3"), "L3", house_num="1", idx_num="2|3")])
    c.expect("L3", "diff.addr", k, [hleg("R", "U", RUN("L3"), house_num="1", idx_num="2|3"),
                                    hleg("U", "U", RUN("L1"), house_num="1|2", idx_num="3")])


def c17():
    c = case(id="C17", title="NULL vs '' in a hub (time_zone_rf.time_zone_name): equal for 3.0",
             rules=["H05-hash"], changes=["NH-hash-essa-xxhash-concat"], scenario="S4", tables=["time_zone_rf"],
             summary="name00 NULL at INIT, '' at L3 (3.0 hash renders NULL as '' -> EQUALS, row keeps NULL and L0 ctl), "
                     "real name at L5 -> (R,U) new + (U,U) with the stored NULL and RUN@L0.")
    c.tz("C17_TZ", code=17, abbreviation="C17_TZ", name00=None, timedesignation="+1", utcformat="+4",
         at={"L3": dict(name00=""), "L5": dict(name00="C17 zone")})
    c.expect("L0", "snp.time_zone_rf", "C17_TZ", [hub(RUN("L0"), "L0", time_zone_name=None, time_zone_code="17")])
    c.expect("L3", "snp.time_zone_rf", "C17_TZ", [hub(RUN("L0"), "L0", time_zone_name=None)])
    c.expect("L3", "diff.time_zone_rf", "C17_TZ", [])
    c.expect("L5", "diff.time_zone_rf", "C17_TZ", [hleg("R", "U", RUN("L5"), time_zone_name="C17 zone"),
                                                  hleg("U", "U", RUN("L0"), time_zone_name=None)])


def c18():
    c = case(id="C18", title="NULL vs '' in div_ (short_div_code) and div_h (div_name): equal for 3.0",
             rules=["H05-hash", "R03"], changes=["NH-hash-essa-xxhash-concat", "HPWR-equals-retention"], scenario="S4",
             tables=["div_", "div_h"],
             summary="shortdivcode and fullname are not nullif'ed by the INCR preselect. NULL->'' at L3 changes nothing "
                     "in 3.0; a real short code at L5 changes div_ only.")
    r = c.div("C18_1", fullname=None, shortdivcode=None,
              at={"L3": dict(fullname="", shortdivcode=""), "L5": dict(shortdivcode="05")})
    c.hist(r, "2026-03-03 10:00:00", 1)
    c.expect("L3", "snp.div_h", "C18_1", [ver("2026-03-03", MAX, START, "L0", div_name=None)])
    c.expect("L3", "diff.div_h", "C18_1", [])
    c.expect("L3", "snp.div_", "C18_1", [hub(RUN("L0"), "L0", short_div_code=None)])
    c.expect("L3", "diff.div_", "C18_1", [])
    c.expect("L5", "diff.div_", "C18_1", [hleg("R", "U", RUN("L5"), short_div_code="05"), hleg("U", "U", RUN("L0"), short_div_code=None)])
    c.expect("L5", "diff.div_h", "C18_1", [])


for _f in [c15, c16, c17, c18]:
    _f()
