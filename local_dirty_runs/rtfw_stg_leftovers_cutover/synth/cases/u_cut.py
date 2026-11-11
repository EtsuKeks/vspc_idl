"""U: cutover over RT-FW staging leftovers (audit G1). Leftovers themselves are seeded by the PRESQL_DIR pre-step
(stand.sh), not by case SQL, so the same chain can run with and without them."""
from model import GONE
from cases import case


def u11():
    c = case(id="U11", chain="dg1", title="G1: keys with real activity while stg.div__diff / div_h_diff hold leftovers",
             summary="U11_1 name changes at U1, U2, U3; U11_2 short code at U2; U11_3 stable. The leftover rows reuse "
                     "the div_sk of U11_1/U11_2 (seeded by the pre-step).")
    c.div("U11_1", fullname="U11 v0", at={"U1": dict(fullname="U11 v1"), "U2": dict(fullname="U11 v2"),
                                         "U3": dict(fullname="U11 v3")})
    c.div("U11_2", at={"U2": dict(shortdivcode="02")})
    c.div("U11_3")


def u12():
    c = case(id="U12", chain="dg1", title="G1: emptied div_type loader cache before a returner (cutover)",
             summary="U12_1 vanishes at U1 (3.0 closes div_type and caches the end), the cache is emptied before U3, "
                     "U12_1 returns at U3.")
    c.div("U12_1", at={"U1": GONE, "U3": {}})
    c.sql("U3", "/*opt*/insert overwrite table usl_vspc_idl_stg.loader_cache_div_type select cast(null as bigint), "
                "cast(null as date) where 1 = 0")


for _f in [u11, u12]:
    _f()
