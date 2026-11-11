"""Chg_div_h preselect inputs: reference flags, d_ history at INIT only, attribute parsing."""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case


def c21():
    c = case(id="C21", title="Reference flags: gar_fias row (deleted_flag NULL = valid) becomes deleted",
             rules=["S3-chgdivh-incr", "H05-hash", "H08-diff-legs-incr"], changes=["NH-undo-keep-validfrom"],
             tables=["addr"],
             summary="Gold fias_code comes from usl_gar_fias_idl_snp.addr where NOT coalesce(deleted_flag,false) AND NOT "
                     "coalesce(dirty_flag,false). At L3 the row is deleted -> fias_code/fias_code_addr_* become NULL "
                     "(key uses the dirty code and does not change) -> (R,U)+(U,U).")
    c.div("C21_1", address_legaladdrdoc="C21 legal", address_addrfact_fiascode="c21-fias")
    c.ref("gar", "c21-fias", deleted=None, at={"L3": dict(deleted_flag=True)})
    k = addr_key(durty="c21-fias", src="C21 legal")
    c.expect("L0", "snp.addr", k, [hub(RUN("L0"), "L0", fias_code="c21-fias", fias_code_addr_dp_code="gar_fias_idl", fias_code_addr_sk=NOTNULL)])
    c.expect("L3", "snp.addr", k, [hub(RUN("L3"), "L3", fias_code=None, fias_code_addr_dp_code=None, fias_code_addr_sk=None)])
    c.expect("L3", "diff.addr", k, [hleg("R", "U", RUN("L3"), fias_code=None), hleg("U", "U", RUN("L0"), fias_code="c21-fias")])


def c22():
    c = case(id="C22", title="Reference flags: okato2 row becomes dirty -> legal address key changes",
             rules=["S3-chgdivh-incr", "R08"], changes=[], tables=["addr", "addr_h", "div_"],
             summary="gold okato is part of the address key. dirty_flag NULL = valid at INIT; dirty=true at L5 -> new "
                     "key '...|NULL|c22oktmo': new addr and addr_h rows, div_.legal_addr_sk moves.")
    c.div("C22_1", address_legaladdrdoc="C22 legal", address_addrfact_okatocode="c22okato", address_addrfact_oktmocode="c22oktmo")
    c.ref("okato", "c22okato", dirty=None, at={"L5": dict(dirty_flag=True)})
    c.ref("oktmo", "c22oktmo")
    k1, k2 = addr_key(src="C22 legal", okato="c22okato", oktmo="c22oktmo"), addr_key(src="C22 legal", oktmo="c22oktmo")
    c.expect("L0", "snp.addr_h", k1, [ver(MIN, MAX, RUN("L0"), "L0", address_okato2_sk=NOTNULL, address_oktmo2_sk=NOTNULL)])
    c.expect("L5", "snp.addr_h", k2, [ver(MIN, MAX, RUN("L5"), "L5", address_okato2_sk=None, address_oktmo2_sk=NOTNULL)])
    c.expect("L5", "snp.addr", k2, [hub(RUN("L5"), "L5", okato2_sk=None)])
    c.expect("L5", "snp.addr", k1, [hub(RUN("L0"), "L0", okato2_sk=NOTNULL)])
    c.expect("L5", "diff.div_", "C22_1", [hleg("R", "U", RUN("L5"), legal_addr_sk=k2), hleg("U", "U", RUN("L0"), legal_addr_sk=k1)])


def c23():
    c = case(id="C23", title="Reference flags: pay_sys_prtcpnt row (BIC) deleted -> div_h new version on the 15th",
             rules=["S3-chgdivh-incr", "R07", "R11"], changes=["HPWR-close-date-loadDate-2"], tables=["div_h"],
             summary="requisitesdivision_bic is taken from the reference (deleted rows excluded). At L6 the BIC row is "
                     "deleted -> pay_sys_prtcpnt_sk NULL -> close [.., D-2] + open [D-1, MAX].")
    r = c.div("C23_1", requisitesdivision_bic="C23BIC")
    c.hist(r, "2026-09-09 10:00:00", 1)
    c.ref("pay", "C23BIC", at={"L6": dict(deleted_flag=True)})
    c.expect("L0", "snp.div_h", "C23_1", [ver("2026-09-09", MAX, START, "L0", pay_sys_prtcpnt_sk=NOTNULL, pay_sys_prtcpnt_dp_code="rdm_idl_spark")])
    c.expect("L6", "snp.div_h", "C23_1", [
        ver("2026-09-09", d("L6", -2), RUN("L6"), "L6", pay_sys_prtcpnt_sk=NOTNULL, pay_sys_prtcpnt_dp_code="rdm_idl_spark", p1month="202702"),
        ver(d("L6", -1), MAX, RUN("L6"), "L6", pay_sys_prtcpnt_sk=None, pay_sys_prtcpnt_dp_code=None, p1month="999912")])


def c29():
    c = case(id="C29", title="d_ rows that appear after INIT are ignored (INCR reads only the snapshot)",
             rules=["R11", "S3-chgdivh-incr"], changes=[], tables=["div_h"],
             summary="At L3 a new d_ row with another name appears; the snapshot is unchanged -> nothing happens.")
    r = c.div("C29_1", fullname="C29 A")
    c.hist(r, "2026-04-01 10:00:00", 1)
    c.hist(r, "2027-01-14 12:00:00", 9, start="L3", fullname="C29 from d_ after INIT")
    c.expect("L3", "snp.div_h", "C29_1", [ver("2026-04-01", MAX, START, "L0", div_name="C29 A")])
    c.expect("L3", "diff.div_h", "C29_1", [])


def c31():
    c = case(id="C31", title="INIT chain collapse keeps the FIRST version's non-hashed attributes (close date)",
             rules=["R10", "S2-chgdivh-init", "H07-snp-incr"], changes=[], tables=["div_", "div_h"],
             summary="The INIT version hash covers code, name, BIC, address, FIAS, disabled flag - not closedate. Later "
                     "rows that only set closedate (including the snapshot) are collapsed into the first one, so div_ "
                     "gets div_close_dt NULL at INIT and the real date only at L1.")
    r = c.div("C31_1", fullname="C31 A", closedate="2026-06-01 00:00:00")
    c.hist(r, "2026-02-10 10:00:00", 1, closedate=None)
    c.hist(r, "2026-06-10 10:00:00", 2)
    c.expect("L0", "snp.div_h", "C31_1", [ver("2026-02-10", MAX, START, "L0")])
    c.expect("L0", "snp.div_", "C31_1", [hub(RUN("L0"), "L0", div_close_dt=None)])
    c.expect("L1", "diff.div_", "C31_1", [hleg("R", "U", RUN("L1"), div_close_dt="2026-06-01"), hleg("U", "U", RUN("L0"), div_close_dt=None)])
    c.expect("L1", "diff.div_h", "C31_1", [])


def c32():
    c = case(id="C32", title="serving_isdisabledequip (string) -> boolean flag; change at L3",
             rules=["S2-chgdivh-init", "S3-chgdivh-incr", "R07"], changes=[], tables=["div_h"],
             summary="Flag = (serving_isdisabledequip = 1): '1' true, '0' false, ' 1' (space) cast to 1 -> true, "
                     "'true' -> NULL -> false, NULL -> false. C32_1 history '1' then snapshot '0' gives two INIT "
                     "versions. C32_3 'true'->'1' at L3 is a real change (false -> true).")
    r = c.div("C32_1", serving_isdisabledequip="0")
    c.hist(r, "2026-10-10 10:00:00", 1, serving_isdisabledequip="1")
    c.div("C32_2", serving_isdisabledequip=" 1")
    c.div("C32_3", serving_isdisabledequip="true", at={"L3": dict(serving_isdisabledequip="1")})
    c.div("C32_4", serving_isdisabledequip=None)
    c.expect("L0", "snp.div_h", "C32_1", [ver("2026-10-10", d("L0", -1), START, "L0", disabled_access_equipment_flag="true"),
                                         ver(d("L0"), MAX, START, "L0", disabled_access_equipment_flag="false")])
    c.expect("L1", "snp.div_h", "C32_2", [ver(d("L0"), MAX, START, "L0", disabled_access_equipment_flag="true")])
    c.expect("L0", "snp.div_h", "C32_4", [ver(d("L0"), MAX, START, "L0", disabled_access_equipment_flag="false")])
    c.expect("L3", "snp.div_h", "C32_3", [ver(d("L0"), d("L3", -2), RUN("L3"), "L3", disabled_access_equipment_flag="false"),
                                         ver(d("L3", -1), MAX, RUN("L3"), "L3", disabled_access_equipment_flag="true")],
             note="INIT version [%s,..] closes at D-2=%s (one-day version, start == end)" % (d("L0"), d("L3", -2)))


for _f in [c21, c22, c23, c29, c31, c32]:
    _f()
