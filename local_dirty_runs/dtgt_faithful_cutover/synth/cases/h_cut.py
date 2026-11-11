"""Chain dcut: 3.0 produces the target dirt itself from duplicated source rows (C0..C2, 3.0 only, same day like DEV),
4.0 takes over from the 3.0 state at C3. SQL seeds only for shapes 3.0 cannot produce from sources."""
from model import GONE, copy_rows
from cases import case

H, DT, DV = "usl_vspc_idl_snp.div_h", "usl_vspc_idl_snp.div_type", "usl_vspc_idl_snp.div_"
OPEN = "end_dt = date'9999-12-31'"
K = range(1, 5)


def _sk(sid):
    return "div_sk in (select div_sk from usl_vspc_idl_snp.div_ where sid = '%s')" % sid


def ct1():
    c = case(id="CT1", chain="dcut", title="source duplicate (other short code + name), kept through C4",
             summary="Two m_sfs rows per key: #a and #b differ in shortdivcode and fullname, both present C0..C4.")
    for k in K:
        key = "CT1_%d" % k
        c.div(key, label=key + "#a")
        c.div(key, label=key + "#b", shortdivcode="02", fullname="%s other" % key)


def ct2():
    c = case(id="CT2", chain="dcut", title="source duplicate, #b GONE at C3 (source clean after cutover)",
             summary="Like CT1; #b disappears from the source at C3.")
    for k in K:
        key = "CT2_%d" % k
        c.div(key, label=key + "#a")
        c.div(key, label=key + "#b", shortdivcode="02", fullname="%s other" % key, at={"C3": GONE})


def ct3():
    c = case(id="CT3", chain="dcut", title="source duplicate differing only in divisiontype00, kept",
             summary="Two m_sfs rows per key with types CT3_TA / CT3_TB, all else equal.")
    for k in K:
        key = "CT3_%d" % k
        c.div(key, label=key + "#a", divisiontype00="CT3_TA")
        c.div(key, label=key + "#b", divisiontype00="CT3_TB")


def ct4():
    c = case(id="CT4", chain="dcut", title="two divisiontype00 values, #b GONE at C3",
             summary="Like CT3; #b (type CT4_TB) disappears at C3.")
    for k in K:
        key = "CT4_%d" % k
        c.div(key, label=key + "#a", divisiontype00="CT4_TA")
        c.div(key, label=key + "#b", divisiontype00="CT4_TB", at={"C3": GONE})


def ct5():
    c = case(id="CT5", chain="dcut", title="source duplicate, #a changes payload at C3 (changed key over inherited twin)",
             summary="Like CT1; at C3 #a gets shortdivcode 05 and a new name.")
    for k in K:
        key = "CT5_%d" % k
        c.div(key, label=key + "#a", at={"C3": dict(shortdivcode="05", fullname="%s new" % key)})
        c.div(key, label=key + "#b", shortdivcode="02", fullname="%s other" % key)


def b14():
    c = case(id="B14", chain="dcut", title="seeded div_ twin of a key that is GONE from the source at C1",
             summary="B14_1: identical twin (ctl_loading 13000) seeded before C1; B14_2: twin with short_div_code 99. "
                     "Both keys are absent from the source from C1 on.")
    c.div("B14_1", at={"C1": GONE})
    c.div("B14_2", at={"C1": GONE})
    c.sql("C1", "insert into %s select * from %s where sid = 'B14_1'" % (DV, DV))
    c.sql("C1", copy_rows(DV, DV, "sid = 'B14_2'", "snp.div_", short_div_code="'99'"))


def b21l():
    c = case(id="B21L", chain="dcut", title="second OPEN div_h version seeded before C2 under C1's loading 13001",
             summary="Copy of the open version, other div_name, ctl_loading 13001, ctl_validfrom C1+5s. B21L_1 stays "
                     "unchanged, B21L_2 changes its name at C3 (after cutover).")
    for k, at in (("B21L_1", None), ("B21L_2", {"C3": dict(fullname="B21L new")})):
        c.div(k, fullname="B21L name", at=at)
        c.sql("C2", copy_rows(H, H, _sk(k) + " and " + OPEN, "snp.div_h", "p1month='999912'",
                              div_name="'B21L other wave'", ctl_loading="13001",
                              ctl_validfrom="timestamp'2027-01-13 12:20:05'"))


def b22l():
    c = case(id="B22L", chain="dcut", title="second div_type type on the same interval seeded before C2 under 13001",
             summary="B22L_1/B22L_3 get a second open div_type row with type B22L_TB (hub row via B22L_2); "
                     "B22L_3 changes its type to B22L_TC at C3.")
    c.div("B22L_1", divisiontype00="B22L_TA")
    c.div("B22L_2", divisiontype00="B22L_TB")
    c.div("B22L_3", divisiontype00="B22L_TA", at={"C3": dict(divisiontype00="B22L_TC")})
    tb = "(select max(terr_div_type_sk) from usl_vspc_idl_snp.terr_div_type where sid = 'B22L_TB')"
    for k in ("B22L_1", "B22L_3"):
        c.sql("C2", copy_rows(DT, DT, _sk(k) + " and " + OPEN, "snp.div_type", "p1month='999912'",
                              div_type_terr_div_type_sk=tb, ctl_loading="13001",
                              ctl_validfrom="timestamp'2027-01-13 12:20:05'"))


for _f in [ct1, ct2, ct3, ct4, ct5, b14, b21l, b22l]:
    _f()
