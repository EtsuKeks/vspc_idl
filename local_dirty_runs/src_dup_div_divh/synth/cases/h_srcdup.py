"""H: several snapshot rows per fulldivcode that differ in a div_ / div_h column (scenario src_dup_div_divh)."""
from model import GONE
from cases import case

N = 4


def _pair(c, k, a, b, at_b=None, start_b=None):
    """Payload A and B for one key; on even k the B row gets the lower label (physical order flipped)."""
    odd = int(k.split("_")[1]) % 2
    la, lb = (k + "#a", k + "#b") if odd else (k + "#b", k + "#a")
    c.div(k, label=la, **a)
    c.div(k, label=lb, at=at_b, start=start_b, **b)


def sd(cid, title, a, b, n=N, **kw):
    c = case(id=cid, chain="ddev", deterministic=False, title=title, summary=title)
    for i in range(1, n + 1):
        _pair(c, "%s_%d" % (cid, i), a, b, **kw)


sd("SD1", "2 m_ rows, differ in shortdivcode only (div_)", dict(shortdivcode="01", fullname="SD1"), dict(shortdivcode="02", fullname="SD1"))
sd("SD2", "2 m_ rows, differ in fullname only (div_h)", dict(fullname="SD2 A"), dict(fullname="SD2 B"))
sd("SD3", "2 m_ rows, differ in shortdivcode and fullname", dict(shortdivcode="01", fullname="SD3 A"), dict(shortdivcode="02", fullname="SD3 B"))
sd("SD4", "clean at D0, copy B appears at D1", dict(shortdivcode="01", fullname="SD4 A"), dict(shortdivcode="02", fullname="SD4 B"), start_b="D1")
sd("SD7", "like SD3, copy B gone at D2", dict(shortdivcode="01", fullname="SD7 A"), dict(shortdivcode="02", fullname="SD7 B"), at_b={"D2": GONE})

c = case(id="SD5", chain="ddev", deterministic=False, title="control: 3 variants", summary="shortdivcode 01/02/03")
for i in (1, 2):
    for j, sc in enumerate(("01", "02", "03")):
        c.div("SD5_%d" % i, label="SD5_%d#%d" % (i, j), shortdivcode=sc, fullname="SD5")
c = case(id="SD6", chain="ddev", deterministic=False, title="control: copies differ only in ctl_loading (not read)", summary="distinct collapses")
for i in (1, 2):
    c.div("SD6_%d" % i, label="SD6_%d#a" % i, ctl_loading="1", fullname="SD6")
    c.div("SD6_%d" % i, label="SD6_%d#b" % i, ctl_loading="2", fullname="SD6")
