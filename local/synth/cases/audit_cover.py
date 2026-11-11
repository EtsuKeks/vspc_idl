"""Coverage gaps of compat and the vendor path (COVERAGE_1.2): branches no chain reached. Phase 1.3, letter Q.

Most uncovered vendor branches (HistoricalPeriodWithRewriteChangesCalculator, AbstractChangesCalculator, PreparationStage,
PublicationStage) are unreachable for VSPC (CompatHooks refuses fullSlice/reInit/collapsePeriods, fixed metadata, man
schema, disabled asserts) or need CTL parameters / target DDL drift / fault injection; see the 1.3 report of letter Q.
Here: only the branch a chain of plain loads can open.
"""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case


def q01():
    c = case(id="Q01", chain="sameid",
             title="DV-18: INCR with the loadingId of the preceding INIT (snp holds L, diff does not)",
             known40="phase 3 (j0164-j0205): no guard, Q1 runs like 3.0; same as 3.0 (DV-18)",
             rules=["H06-record-status", "H08-diff-legs-incr", "R13"], changes=[], tables=["div_type"],
             summary="Q0 INIT and Q1 INCR share loadingId 9650, so before Q1 the snp already holds rows with ctl_loading=L "
                     "and the diff holds none (INIT clears it). 3.0 has no such check: equal ctl_loading is ordered by "
                     "record status, NEW before OLD (E/slave/loading/SnpDataLoader.java:179-181), so Q01_1 changing its "
                     "type is an ordinary NOTEQUALS change of HistSnp: the INIT version [1900, MAX] closes at D-2 with the "
                     "run's ctl_validfrom, the new version opens at D-1 "
                     "(E/slave/loading/DataLoaderHistSnp.java:44-47,82-86,103-108,117-119), legs (R,I)+(U,D) new, "
                     "(R,U) closed + (U,U) old with end MAX (DataLoaderHistSnp.java:122,139-153). ctl_loading of Q1 rows "
                     "shows the first label of the id (Q0).")
    c.known40 = ("by code, not observed: Q1 FAILS at the first published table whose snp holds ctl_loading=9650 "
                 "(PublishPreconditions.java:77 'уже в snp, а diff этой загрузки нет'); no Q1 dump for 4.0")
    c.div("Q01_1", divisiontype00="Q01_TA", at={"Q1": dict(divisiontype00="Q01_TB")})
    c.expect("Q0", "snp.div_type", "Q01_1", [ver(MIN, MAX, RUN("Q0"), "Q0", div_type_terr_div_type_sk="Q01_TA")])
    c.expect("Q0", "diff.div_type", "Q01_1", [], note="INIT writes no diff (DataLoaderHistSnp.java:125-127)")
    c.expect("Q1", "snp.div_type", "Q01_1",
             [ver(MIN, d("Q1", -2), RUN("Q1"), "Q0", div_type_terr_div_type_sk="Q01_TA", p1month="202701"),
              ver(d("Q1", -1), MAX, RUN("Q1"), "Q0", div_type_terr_div_type_sk="Q01_TB", p1month="999912")],
             note="same loadingId: ctl_loading is dumped as the first label of 9650 (Q0)")
    c.expect("Q1", "diff.div_type", "Q01_1",
             [leg("R", "I", d("Q1", -1), MAX, RUN("Q1"), div_type_terr_div_type_sk="Q01_TB"),
              leg("U", "D", d("Q1", -1), MAX, RUN("Q1"), div_type_terr_div_type_sk="Q01_TB"),
              leg("R", "U", MIN, d("Q1", -2), RUN("Q1"), div_type_terr_div_type_sk="Q01_TA"),
              leg("U", "U", MIN, MAX, RUN("Q1"), div_type_terr_div_type_sk="Q01_TA")],
             note="ordinary change legs: same-id INCR after INIT is not special for 3.0")


for _f in [q01]:
    _f()
