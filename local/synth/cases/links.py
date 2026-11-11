"""Div_div_relation (successors)."""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case


def c28():
    c = case(id="C28", title="div_div_relation: successor type filter, unknown successor, new and vanished relations",
             rules=["L1-link-source", "L3-link-init", "L5-link-incr-outcomes", "L6-link-effective", "L7-link-incr-diff"],
             changes=["HPWR-new-key-start-1900"], tables=["div_div_relation"],
             summary="Only TRIM(lower(type))='правопреемник' passes (' Правопреемник ', 'ПРАВОПРЕЕМНИК' pass; 'Иное' "
                     "does not). Unknown successor id -> successor_sid NULL -> key 'C28_3|NULL'. Duplicate successor rows "
                     "collapse. New relation at L3 -> [1900,MAX] (R,I)+(U,D). Relation removed at L5 stays open.")
    for k in ("C28_1", "C28_2", "C28_3"):
        c.div(k)
    c.succ("C28_1", "C28_2", " Правопреемник ", idx=0, at={"L5": GONE})
    c.succ("C28_1", "C28_2", " Правопреемник ", idx=1, at={"L5": GONE})
    c.succ("C28_2", "C28_3", "Иное")
    c.succ("C28_3", "C28_X", "правопреемник")
    c.succ("C28_2", "C28_1", "ПРАВОПРЕЕМНИК", start="L3")
    c.expect("L0", "snp.div_div_relation", "C28_1|C28_2",
             [ver(MIN, MAX, RUN("L0"), "L0", source_div_sk="C28_1", successor_div_sk="C28_2", deleted_flag="false")])
    c.expect("L0", "snp.div_div_relation", "C28_3|NULL", [ver(MIN, MAX, RUN("L0"), "L0", source_div_sk="C28_3", successor_div_sk=None)])
    c.expect("L0", "snp.div_div_relation", "C28_2|C28_3", [])
    c.expect("L3", "snp.div_div_relation", "C28_2|C28_1", [ver(MIN, MAX, RUN("L3"), "L3", source_div_sk="C28_2", successor_div_sk="C28_1")])
    c.expect("L3", "diff.div_div_relation", "C28_2|C28_1", [leg("R", "I", MIN, MAX, RUN("L3")), leg("U", "D", MIN, MAX, RUN("L3"))])
    c.expect("L5", "diff.div_div_relation", "C28_1|C28_2", [])
    c.expect("L7", "snp.div_div_relation", "C28_1|C28_2", [ver(MIN, MAX, RUN("L0"), "L0")])


for _f in [c28]:
    _f()
