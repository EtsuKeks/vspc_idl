"""H: source carries two rows per fulldivcode that differ only in divisiontype00 (src_dup_divtype scenario).
divisiontype00 is not read by chg_div_h, so div_/div_h stay clean; only div_type (and terr_div_type) see the pair."""
from model import GONE
from cases import case


def ty():
    c = case(id="TY", chain="ddev", deterministic=False,
             title="B2b/B3/F4/B4 (source): two types per fulldivcode in the snapshot",
             summary="TY_1..TY_8: two snapshot rows per code, types TY_TA and TY_TB, all other columns equal "
                     "(insertion order flipped on even k). TY_9: TA from D0, TB added at D1. TY_10: pair at D0, "
                     "TB gone from D1. TY_11: new code at D1 with both types.")
    for k in range(1, 9):
        key = "TY_%d" % k
        first, second = ("TY_TA", "TY_TB") if k % 2 else ("TY_TB", "TY_TA")
        c.div(key, label=key + "#a", divisiontype00=first)
        c.div(key, label=key + "#b", divisiontype00=second)
    c.div("TY_9", label="TY_9#a", divisiontype00="TY_TA")
    c.div("TY_9", label="TY_9#b", divisiontype00="TY_TB", start="D1")
    c.div("TY_10", label="TY_10#a", divisiontype00="TY_TA")
    c.div("TY_10", label="TY_10#b", divisiontype00="TY_TB", at={"D1": GONE})
    c.div("TY_11", label="TY_11#a", divisiontype00="TY_TA", start="D1")
    c.div("TY_11", label="TY_11#b", divisiontype00="TY_TB", start="D1")


for _f in [ty]:
    _f()
