"""H: same-day INIT+INCR replay of the DEV timeline (audit section 1, B3, F4, B2a): d_ history since 2023,
INIT and two INCRs on one calendar day, then an INCR on the 1st of the month. No target-side SQL."""
from cases import case

LEGS = ["2023-06-20 10:00:00", "2023-10-15 10:00:00", "2023-12-10 10:00:00", "2024-01-22 10:00:00"]


def h1():
    c = case(id="HL", chain="dday", title="same-day INIT+INCR: INIT closures at D-1, INCR opens at D-1",
             summary="HL_1..5: m_ name differs from the last d_ leg (INIT closes it at D-1); HL_6..10: equal. "
                     "D1 changes HL_1, HL_2, HL_6, HL_11, HL_N, HL_A(address); D2 changes HL_3, HL_7, HL_11; D3 changes HL_4. "
                     "HL_N has no d_ history; HL_11 changes twice the same day.")
    chg = {1: ["D1"], 2: ["D1"], 6: ["D1"], 3: ["D2"], 7: ["D2"], 4: ["D3"], 11: ["D1", "D2"]}
    for i in range(1, 12):
        k = "HL_%d" % i
        now = ("HL_%d now" % i) if (i <= 5 or i == 11) else ("HL_%d v3" % i)
        at = {ld: dict(fullname="HL_%d %s" % (i, ld)) for ld in chg.get(i, [])}
        r = c.div(k, fullname=now, at=at)
        for j, vf in enumerate(LEGS):
            c.hist(r, vf, j + 1, fullname="HL_%d v%d" % (i, j))
    c.div("HL_N", fullname="HL_N now", at={"D1": dict(fullname="HL_N D1")})
    c.div("HL_A", fullname="HL_A now", address_locationaddress="HL_A fact 1", address_addrfact_fiascode="hl-a-fias",
          at={"D1": dict(address_locationaddress="HL_A fact 2")})


for _f in [h1]:
    _f()
