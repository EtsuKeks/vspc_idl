"""Audit 1.3, theme hubs2 (SVOD DV-21 DV-27 DV-45 DV-49 DV-56 DV-59 DV-60 DV-61 DV-62).

Only DV-27 is a data scenario on this stand; the others need CTL parameters per load, a Spark conf / logical-env knob
in the runners, a recording SK mock, metrics or ИФТ (see the phase 1.3 report, per_dv).
"""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case


def j01():
    c = case(id="J01", chain="rerunnull",
             title="DV-27: INCR rerun (same loadingId) after the INCR only re-stamped the NULL group of terr_div_type",
             rules=["H13-key-dedup", "H07-snp-incr"], changes=[], tables=["terr_div_type"], deterministic=False,
             known40="phase 3 (j0164-j0205): no guard, the J2 rerun runs like 3.0; same as 3.0 (DV-27)",
             summary="DV-27. divisiontype00 NULL (J01_1) and '' (J01_2): two terr_div_type rows with NULL sk at INIT "
                     "(as C35). J1 (INCR, same snapshot): the NULL-pk window of 3.0 sees four rows with equal hash "
                     "(E/slave/loading/SnpDataLoader.java:120-133, NULL -> ''), keeps only EQUALS rn=2 = a NEW row, "
                     "re-stamped with J1 (E/slave/loading/DataLoaderHubDiff.java:75-80) and writes no diff leg "
                     "(:127-129). Nobody else in this chain touches terr_div_type, so after J1 no terr_div_type row "
                     "has ctl_loading=J1 in diff. J2 = J1 again with the same loadingId: window orders desc(ctl_loading), "
                     "asc(status) (SnpDataLoader.java:179-204) -> NEW, NEW, OLD, all EQUALS -> again one row (rn=2, NEW), "
                     "re-stamped by J2, no diff; the 3.0 load is SUCCESS (no replay guard in 3.0). Which of sid NULL / '' "
                     "survives is a window tie (not checked, as in C35). A difference = 4.0 load J2 FAILED.")
    # J01_0: an ordinary type so that the SK init of terr_div_type sees a non-empty sid (stand SK mock: no non-empty sid in
    # the batch -> sk 0 for every row at INIT, then NULL sk at INCR -> two partitions; first 1.3 run showed exactly that)
    c.div("J01_0")
    c.div("J01_1", divisiontype00=None)
    c.div("J01_2", divisiontype00="")
    c.owns(None, "NULL terr_div_type_sk of J01_1/J01_2 (divisiontype00 NULL and '')", table="terr_div_type")
    c.expect("J0", "snp.terr_div_type", None, [hub(RUN("J0"), "J0"), hub(RUN("J0"), "J0")])
    c.expect("J1", "snp.terr_div_type", None, [hub(RUN("J1"), "J1")])
    c.expect("J1", "diff.terr_div_type", None, [])
    c.expect("J2", "snp.terr_div_type", None, [hub(RUN("J2"), "J1")],
             note="rerun with the same loadingId: one row again, re-stamped by the rerun (ctl_loading shows the first label)")
    c.expect("J2", "diff.terr_div_type", None, [])
    for k in ("J01_1", "J01_2"):
        c.expect("J2", "snp.div_", k, [hub(RUN("J0"), "J0")])
        c.expect("J2", "diff.div_", k, [])


for _f in [j01]:
    _f()
