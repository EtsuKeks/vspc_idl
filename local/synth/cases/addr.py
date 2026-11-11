"""Addr / addr_h: address keys, INIT vs INCR preselect differences."""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case


def c13():
    c = case(id="C13", title="addr.idx_num: NULL at INIT (preselect drops fact postcode), filled at the first INCR",
             rules=["S2-chgdivh-init", "S3-chgdivh-incr", "H05-hash", "H08-diff-legs-incr", "H14-ctl-semantics-summary"],
             changes=["NH-undo-keep-validfrom", "NH-hash-essa-xxhash-concat"], tables=["addr", "addr_h"],
             summary="INIT chg_div_h does not project address_addrfact_postcode, INCR does. So the fact address gets "
                     "idx_num at L1: (R,U) new value with RUN@L1 and (U,U) old value with the old ctl_validfrom RUN@L0. "
                     "addr_h (no postcode column) does not change.")
    c.div("C13_1", address_locationaddress="C13 fact", address_addrfact_postcode="101013",
          address_addrfact_addrcity_cityname="C13 city")
    k = addr_key(src="C13 fact")
    c.expect("L0", "snp.addr", k, [hub(RUN("L0"), "L0", idx_num=None, city_name="C13 city", src_txt="C13 fact")])
    c.expect("L1", "snp.addr", k, [hub(RUN("L1"), "L1", idx_num="101013", city_name="C13 city")])
    c.expect("L1", "diff.addr", k, [hleg("R", "U", RUN("L1"), idx_num="101013"), hleg("U", "U", RUN("L0"), idx_num=None)])
    c.expect("L1", "diff.addr_h", k, [])
    c.expect("L1", "snp.addr_h", k, [ver(MIN, MAX, RUN("L0"), "L0", standard_addr_txt="C13 fact")])


def c14():
    c = case(id="C14", title="Legal address: INIT trims/lowers the FIAS code, INCR does not -> address key drifts",
             rules=["S2-chgdivh-init", "S3-chgdivh-incr", "R08", "H02-input-is-full-snapshot"],
             changes=[], tables=["addr", "addr_h", "div_"],
             summary="FIAS ' C14-FIAS ' (spaces, upper case). INIT: durty = lower(trim) = 'c14-fias', joins gar -> gold "
                     "fias_code. INCR: durty = lower(raw) = ' c14-fias ', join on the raw value fails -> fias_code NULL. "
                     "The key contains durty, so L1 creates a NEW addr/addr_h row; the INIT row stays; div_.legal_addr_sk "
                     "moves to the new key.")
    c.div("C14_1", address_legaladdrdoc="C14 legal", address_addrfact_fiascode=" C14-FIAS ", address_postcode="101014")
    c.ref("gar", "c14-fias")
    k0, k1 = addr_key(durty="c14-fias", src="C14 legal"), addr_key(durty=" c14-fias ", src="C14 legal")
    c.expect("L0", "snp.addr", k0, [hub(RUN("L0"), "L0", fias_code="c14-fias", fias_code_addr_sk=NOTNULL, idx_num="101014")])
    c.expect("L1", "snp.addr", k0, [hub(RUN("L0"), "L0", fias_code="c14-fias", fias_code_addr_sk=NOTNULL, idx_num="101014")])
    c.expect("L1", "snp.addr", k1, [hub(RUN("L1"), "L1", fias_code=None, fias_code_addr_sk=None, idx_num="101014")])
    c.expect("L1", "diff.addr", k1, [hleg("R", "I", RUN("L1")), hleg("U", "D", RUN("L1"))])
    c.expect("L1", "snp.addr_h", k1, [ver(MIN, MAX, RUN("L1"), "L1")])
    c.expect("L1", "snp.div_", "C14_1", [hub(RUN("L1"), "L1", legal_addr_sk=k1)])
    c.expect("L1", "diff.div_", "C14_1", [hleg("R", "U", RUN("L1"), legal_addr_sk=k1), hleg("U", "U", RUN("L0"), legal_addr_sk=k0)])


def c19():
    c = case(id="C19", title="Fact address moves: new addr/addr_h key, old one stays open; div_h gets a new version",
             rules=["R08", "H02-input-is-full-snapshot", "R07", "R11"], changes=["HPWR-new-key-start-1900"],
             tables=["addr", "addr_h", "div_h"],
             summary="addr and addr_h are insert-only in practice (key = address text). div_h.fact_addr_sk changes -> "
                     "normal close/open.")
    r = c.div("C19_1", address_locationaddress="C19 old", at={"L3": dict(address_locationaddress="C19 new")})
    c.hist(r, "2026-07-07 10:00:00", 1)
    ko, kn = addr_key(src="C19 old"), addr_key(src="C19 new")
    c.expect("L3", "snp.div_h", "C19_1", [ver("2026-07-07", d("L3", -2), RUN("L3"), "L3", fact_addr_sk=ko),
                                         ver(d("L3", -1), MAX, RUN("L3"), "L3", fact_addr_sk=kn)])
    c.expect("L3", "diff.addr", kn, [hleg("R", "I", RUN("L3")), hleg("U", "D", RUN("L3"))])
    c.expect("L3", "snp.addr", ko, [hub(RUN("L0"), "L0")])
    c.expect("L3", "snp.addr_h", kn, [ver(MIN, MAX, RUN("L3"), "L3")])
    c.expect("L3", "snp.addr_h", ko, [ver(MIN, MAX, RUN("L0"), "L0")])
    c.expect("L3", "diff.addr_h", ko, [])


def c20():
    c = case(id="C20", title="INIT addr/addr_h contain addresses of historical d_ versions",
             rules=["R06", "R10", "S2-chgdivh-init"], changes=[], tables=["addr", "addr_h", "div_h"],
             summary="INIT chg_div_h holds the whole history, so the old fact address of the first version is loaded "
                     "into addr and addr_h too. INCR reads only the current snapshot; the old address is never touched.")
    r = c.div("C20_1", address_locationaddress="C20 now")
    c.hist(r, "2026-05-20 10:00:00", 1, address_locationaddress="C20 hist")
    kh, kn = addr_key(src="C20 hist"), addr_key(src="C20 now")
    c.expect("L0", "snp.div_h", "C20_1", [ver("2026-05-20", d("L0", -1), START, "L0", fact_addr_sk=kh),
                                         ver(d("L0"), MAX, START, "L0", fact_addr_sk=kn)])
    for k in (kh, kn):
        c.expect("L0", "snp.addr", k, [hub(RUN("L0"), "L0")])
        c.expect("L0", "snp.addr_h", k, [ver(MIN, MAX, RUN("L0"), "L0")])
    c.expect("L1", "diff.addr", kh, [])
    c.expect("L7", "snp.addr", kh, [hub(RUN("L0"), "L0")])


for _f in [c13, c14, c19, c20]:
    _f()
