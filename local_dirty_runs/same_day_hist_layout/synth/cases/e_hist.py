"""E: technical history (audit E1-E2): garbage dates, deep d_ history from 2023 spread over many months,
duplicate (fulldivcode, start_dt) in the d_ history."""
from cases import case


def e11():
    c = case(id="E11", chain="dsrc", title="E1: garbage dates - close 0201-01-19, close < open, open 1997 / close 2023",
             summary="E11_1 closedate 0201-01-19 (year 201), E11_2 opened 2023-05-01 and closed 2020-01-01, E11_3 open "
                     "1997-05-20 / close 2023-12-22. At A1 E11_1's close date becomes 0202-02-02 and E11_2's open date "
                     "1899-12-31.")
    c.div("E11_1", closedate="0201-01-19 00:00:00", at={"A1": dict(closedate="0202-02-02 00:00:00")})
    c.div("E11_2", opendate="2023-05-01 00:00:00", closedate="2020-01-01 00:00:00", at={"A1": dict(opendate="1899-12-31 00:00:00")})
    c.div("E11_3", opendate="1997-05-20 00:00:00", closedate="2023-12-22 00:00:00")


def e21():
    c = case(id="E21", chain="dsrc", title="E2: deep d_ history (2023 versions, end 2023-10-08) spread over many p1month months",
             summary="E21_1 has d_ versions from 2023-06-20 (closed 2023-10-08) through 2026: the INIT spreads them over "
                     "p1month 202310, 202311, 202312, 202401, 202503, 202608, 202701 and 999912. Name changes at A1 and A3 "
                     "(1st of month). DEV: div_h min validfrom 2023-06-20, min end_dt 2023-10-08.")
    r = c.div("E21_1", fullname="E21 now", at={"A1": dict(fullname="E21 A1"), "A3": dict(fullname="E21 A3")})
    for i, vf in enumerate(["2023-06-20 10:00:00", "2023-10-09 10:00:00", "2023-11-15 10:00:00", "2023-12-20 10:00:00",
                            "2024-01-22 10:00:00", "2025-03-03 10:00:00", "2026-08-08 10:00:00"]):
        c.hist(r, vf, i + 1, fullname="E21 v%d" % i)


def e22():
    c = case(id="E22", chain="dsrc", title="E1: duplicate (fulldivcode, start_dt) in the d_ history", deterministic=False,
             summary="Two d_ rows of E22_1 with the SAME ctl_validfrom and different names, and two on one day at different "
                     "hours. DEV: 63 duplicate (fulldivcode, start_dt) groups in chg_div_h.")
    r = c.div("E22_1", fullname="E22 now", at={"A3": dict(fullname="E22 A3")})
    c.hist(r, "2026-04-04 10:00:00", 1, fullname="E22 twin a")
    c.hist(r, "2026-04-04 10:00:00", 2, fullname="E22 twin b")
    c.hist(r, "2026-06-06 09:00:00", 3, fullname="E22 morning")
    c.hist(r, "2026-06-06 18:00:00", 4, fullname="E22 evening")


for _f in [e11, e21, e22]:
    _f()
