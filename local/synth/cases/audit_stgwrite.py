"""Audit 1.3, theme "stgwrite" (stg write, publication, HDFS/HMS, physical layer): DV-10 DV-23 DV-24 DV-25 DV-30 DV-31
DV-34 DV-47 DV-48 DV-52 DV-58 DV-65 DV-68 DV-75 DV-76. Case letter W.

Cases here:
  * W01 (DV-65): p1month of a div_h version that ends before 1900-01-01 (INIT from the d_ history), chain samepk.
  * W02 (DV-76): loadingId above 2^31-1 on INIT and INCR, NEW chain "bigid" (loads F0, F1; see the 1.3 report).

No case for the other candidates on purpose: they live in the physical layer (files, footers, HMS objects, ACL), need
fault injection / YARN / a parallel reader, a per-load CTL parameter or log_level != ESSA, or state prepared in HDFS and
the metastore between loads, none of which the DSL or the comparator (rows of 16 targets) can express. What is
missing per candidate, and whether an SQL/HDFS step between loads would do instead of engine work, is in the 1.3
report of this theme.
"""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case


def w01():
    c = case(id="W01", chain="samepk", title="DV-65: div_h version ending before 1900-01-01 gets p1month=189912 (INIT from d_ history)",
             rules=["R10", "S2-chgdivh-init"], changes=[], tables=["div_h"],
             summary="DV-65. INIT builds div_h from d_ rows (ctl_do='R', snapshot_ LIKE '%DIF%') plus the snapshot: "
                     "start_dt = ctl_validfrom, end_dt = lead(start_dt) - 1 (3.0 ChgDivHSql.java:177,245; no cut-off "
                     "on the INIT d_ read: VspcTransformConfiguration.getInitCutOffDate is used only by partial INIT, "
                     "E/slave/loading/trgt/BaseDataLoader.java:88-97). The INIT row keeps ctl_validfrom = start_dt "
                     "(DataLoaderHistMix.java:108-110) and p1month = concat(year(end_dt), lpad(month(end_dt),2,'0')) "
                     "without any lower bound (DataLoaderHistMix.java:118, E/utils/Expression.java:42-44,65-72): the "
                     "version [1899-11-01, 1899-12-31] lands in partition 189912. The vendor SectionBy of 4.0 lifts "
                     "end_dt <= 1900-01-01 to 1900-01-01 (V/dto/SectionBy.java:162-166) -> 190001 if the candidate is real. "
                     "189912 is below every INCR rewrite zone (HistMovingGenerator cut point), so K1/K2 leave it as is (cut point = current month - 1, E/slave/move/HistMovingGenerator.java:31-32). "
                     "d_ times are 12:00 so that the Moscow LMT offset of 1899 (+2:30:17) cannot move the calendar day. "
                     "Kept out of chain main on purpose: an exotic pre-1900 date must not risk the broad catalog.")
    r = c.div("W01_1", fullname="W01 now")
    c.hist(r, "1899-11-01 12:00:00", 1, fullname="W01 v1899")
    c.hist(r, "1900-01-01 12:00:00", 2, fullname="W01 v1900")
    rows = [ver("1899-11-01", "1899-12-31", START, "K0", div_name="W01 v1899", p1month="189912"),
            ver("1900-01-01", d("K0", -1), START, "K0", div_name="W01 v1900", p1month="202701"),
            ver(d("K0"), MAX, START, "K0", div_name="W01 now", p1month="999912")]
    c.expect("K0", "snp.div_h", "W01_1", rows,
             note="DV-65: the 1899 version ends 1899-12-31 -> p1month 189912 in 3.0 (4.0 by code: 190001)")
    c.expect("K0", "diff.div_h", "W01_1", [], note="INIT writes no diff")
    c.expect("K2", "snp.div_h", "W01_1", rows,
             note="no change at K1/K2; 189912 is below the rewrite zone and 202701 is rewritten unchanged")
    c.expect("K2", "diff.div_h", "W01_1", [])


def w02():
    # chain "bigid" (new, to be added to model.CHAINS by the coordinator):
    #   F0 INIT 2027-01-13 10:00:00 loadingId 2147483648 by both; F1 INCR 2027-01-14 10:00:00 loadingId 2147483649 by both.
    # Dates are literals: d('F0') needs the chain in model.CHAINS.
    q0, q1_m1, q1_m2 = "2027-01-13", "2027-01-13", "2027-01-12"
    c = case(id="W02", chain="bigid", title="DV-76: loadingId above 2^31-1 (2147483648/2147483649) on INIT and INCR (chain bigid)",
             rules=["R10", "R07", "R11"], changes=[], tables=["div_h", "div_"],
             summary="DV-76. 3.0 carries loadingId as long end to end (Run30 -> AppConfiguration Long, "
                     "E/master/app/forApp/AppConfiguration.java:26, E/slave/templates/TransformConfiguration.java:31; "
                     "ctl_loading = lit(long) in O/impl/loader/DataLoaderHistMix.java:59,87,105,110; target DDL BIGINT), "
                     "so ctl_loading = 2147483648 at F0 and 2147483649 at F1 in snp and diff. "
                     "4.0 keeps loadingId as int: AppContextLoadingParams.getLoadingId() is Integer (K/AppContextLoadingParams.java:12), "
                     "the real coordinator does parseInt(args[0]) (C/CoordinatorImpl.java:131) and the stand stub the same "
                     "(L/java/v40/.../StubCoordinator.java:35), SnapshotChanges takes int (V/dto/SnapshotChanges.java:51). "
                     "Expected on the stand if the candidate is real: 4.0 fails at start (NumberFormatException) instead of "
                     "a silent overflow; 3.0 PASSes. Caveat: on prom 3.0 with log_level != ESSA would also fail, but only "
                     "after publishing, in RtFwMetaWrapper.finishPublish (Integer.valueOf, E/ctlmeta/RtFwMetaWrapper.java:39); "
                     "the stand runs 3.0 with log_level=ESSA (EssaMetaWrapper no-op, Run30.java:33). "
                     "The INIT version starts at the d_ date (collapsed with the snapshot, like C02), so the F1 change "
                     "the day after INIT closes it at D-2 normally.")
    r = c.div("W02_1", fullname="W02 A", at={"F1": dict(fullname="W02 B")})
    c.hist(r, "2026-05-05 10:00:00", 1)
    c.expect("F0", "snp.div_h", "W02_1", [ver("2026-05-05", MAX, START, "F0", div_name="W02 A", p1month="999912")],
             note="ctl_loading = 2147483648 (shown as F0)")
    c.expect("F0", "snp.div_", "W02_1", [hub(RUN("F0"), "F0")])
    c.expect("F1", "snp.div_h", "W02_1", [
        ver("2026-05-05", q1_m2, RUN("F1"), "F1", div_name="W02 A", p1month="202701"),
        ver(q1_m1, MAX, RUN("F1"), "F1", div_name="W02 B", p1month="999912")],
        note="ctl_loading = 2147483649 (F1) on both rows; INIT date %s" % q0)
    c.expect("F1", "diff.div_h", "W02_1", [
        leg("R", "I", q1_m1, MAX, RUN("F1"), div_name="W02 B"),
        leg("U", "D", q1_m1, MAX, RUN("F1"), div_name="W02 B"),
        leg("R", "U", "2026-05-05", q1_m2, RUN("F1"), div_name="W02 A"),
        leg("U", "U", "2026-05-05", MAX, RUN("F1"), div_name="W02 A")],
        note="diff partition ctl_loading=2147483649")
    c.expect("F1", "snp.div_", "W02_1", [hub(RUN("F0"), "F0")],
             note="name change does not touch the hub: the F0 row with ctl_loading 2147483648 survives")


for _f in [w01, w02]:
    _f()
