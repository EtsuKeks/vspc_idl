"""H: ESSA MAX_TRY retry / same-loadingId relaunch (scenario essa_retry_rerun). Changes at the 2nd load only."""
from cases import case


def _keys(c, cid, ld):
    c.div("%s_1" % cid, fullname="%s A1" % cid, at={ld: dict(fullname="%s B1" % cid)})
    c.div("%s_2" % cid, fullname="%s A2" % cid, at={ld: dict(fullname="%s B2" % cid)})
    c.div("%s_3" % cid, fullname="%s A3" % cid, at={ld: dict(fullname="%s B3" % cid)})
    c.div("%s_4" % cid, at={ld: dict(shortdivcode="02")})
    c.div("%s_5" % cid, divisiontype00="%s_TA" % cid, at={ld: dict(divisiontype00="%s_TB" % cid)})
    c.div("%s_6" % cid)


def h11():
    c = case(id="H11", chain="dretry", title="same-loadingId relaunch (R2 = R1 again)",
             summary="H11_1..3 change fullname at R1, H11_4 shortdivcode, H11_5 divisiontype, H11_6 unchanged; "
                     "R2 relaunches loading 13001 with unchanged sources.")
    _keys(c, "H11", "R1")


def h12():
    c = case(id="H12", chain="dretryc", title="MAX_TRY retry control / failure injection at Q1",
             summary="Same shapes as H11 on the chain without relaunch; Q1 gets the injected failure in F1/F2.")
    _keys(c, "H12", "Q1")


for _f in [h11, h12]:
    _f()
