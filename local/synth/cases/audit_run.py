"""Audit 1.3, theme "run" (launch, CTL, Kerberos, environment): DV-16 DV-22 DV-26 DV-29 DV-32 DV-33 DV-37..DV-44
DV-66 DV-67 DV-69..DV-74. Case letter M.

No case is defined here on purpose:
  * DV-16 (max_increment_depth_days -> startingBusinessDate of the vendor HPWR on INCR) is already checked by C01, C02,
    C07, C08 (chain main, max_increment_depth_days=10 in java/ctl40.properties): versions with start_dt older than
    D-10 / D-40 are closed and reopened on L3..L7, including L4/L6/L7 after gaps > 10 days. By code the bound cannot
    act: every INCR row of a period table starts at D-1 >= today-midd (4.0 transformations), the end is 9999-12-31,
    and the snp read filter of the vendor (getSnpGeqFilters) is replaced by the compat zone before use
    (ProcessingStage shadow :126-142, PeriodRewriteCompatCalculator.getMinBusinessDateUpdateFilters).
  * every other candidate of the theme needs a per-load CTL parameter (the stand has one fixed set: Run30
    AppConfiguration / java/ctl40.properties), a recording CTL / KAP.META mock (StubCoordinator and the 3.0 CtlHelper
    are no-op stubs: completeApplication, statval, lock/unlock, dummy loadings never run), fault injection, the real
    launcher / spawned application / spark-submit / Kerberos (the stand runs both versions in-process), or IFT.
    What is missing per candidate is in the 1.3 report of this theme.
"""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case  # noqa: F401


def n05():
    c = case(id="N05", chain="midnight", deterministic=False,
             title="DV-05: INCRs at 23:58 (N12) and 23:59:50 (N13) cross midnight; each step takes its own 'today'",
             tables=["div_h", "div_type", "addr_h", "div_div_relation"],
             summary="N12 starts 2027-01-31 23:58. 3.0 takes the date per step: start_dt = D-1 and closures D-2 from the "
                     "transformer's now() (O/constant/VspcConstant.java:19-21, O/impl/loader/DataLoaderHistMix.java:42,85), "
                     "rewrite zone from current_date() of the loader and LocalDate.now() of the mover "
                     "(DataLoaderHistMix.java:68-69, E/slave/move/HistMovingGenerator.java:31), VANISH from current_date() "
                     "(E/slave/loading/DataLoaderHistSnp.java:106), stg.chg_div_h.start_dt from current_date() (ChgDivHSql.java:43). "
                     "4.0 takes the date the same way per step. Which step falls after 00:00 depends on the speed of each "
                     "version, so dates of a table can differ by one day between versions; the date of each table must match "
                     "the time of its own step. N13: midnight 10 s after the start, before div_h/div_type of both versions, so both "
                     "take 2027-03-01 there.")
    c.div("N05_1", at={"N11": dict(fullname="N05_1 v2"), "N12": dict(fullname="N05_1 v3"), "N13": dict(fullname="N05_1 v4")})
    c.div("N05_2", at={"N12": dict(divisiontype00="N05_T2"), "N13": dict(divisiontype00="N05_T3")})
    c.div("N05_3", at={"N12": GONE})
    c.div("N05_4", start="N12", at={"N13": GONE})
    c.div("N05_5", address_locationaddress="N05 old", at={"N12": dict(address_locationaddress="N05 new")})
    c.succ("N05_1", "N05_2", "правопреемник")
    c.succ("N05_4", "N05_1", "правопреемник", start="N12")


for _f in [n05]:
    _f()
