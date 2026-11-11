"""B: duplicate keys - div_ twins, double open div_h versions, two div_type types on one interval, overlapping
div_h intervals, non-canonical diff legs (audit B1-B4)."""
from model import GONE, copy_rows, div_sk_of
from cases import case

H, DT = "usl_vspc_idl_snp.div_h", "usl_vspc_idl_snp.div_type"
OPEN = "end_dt = date'9999-12-31'"


def b11():
    c = case(id="B11", chain="dtgt", title="B1: exact PK twin rows in snp.div_ (same sk, same sid)",
             summary="Seeded before T1: every div_ row of B11_1..B11_3 is duplicated. B11_1 then stays unchanged "
                     "(T1..T3), B11_2 changes its short code at T1, B11_3 at T2 (same day). DEV: 12 such twins.")
    for k in ("B11_1", "B11_2", "B11_3"):
        c.div(k, at={"T1": dict(shortdivcode="02")} if k == "B11_2" else {"T2": dict(shortdivcode="03")} if k == "B11_3" else None)
        c.sql("T1", "insert into usl_vspc_idl_snp.div_ select * from usl_vspc_idl_snp.div_ where sid = '%s'" % k)


def b12():
    c = case(id="B12", chain="dsrc", title="B1 (source): two byte-identical snapshot rows, later changed together",
             summary="Two identical sfsdivision rows for B12_1 (all columns equal); at A1 both change the name.")
    c.div("B12_1", label="B12_1#a", at={"A1": dict(fullname="B12 B")})
    c.div("B12_1", label="B12_1#b", at={"A1": dict(fullname="B12 B")})


def b21():
    c = case(id="B21", chain="dtgt", title="B2: two OPEN div_h versions of one key (same start, other wave, other payload)",
             summary="Seeded before T1: a second open version [start, 9999-12-31] of the INIT version with another "
                     "div_name, ctl_loading 900 and ctl_validfrom 2026-10-16 12:00 (an older wave). B21_1 stays unchanged, "
                     "B21_2 changes its name at T1, B21_3 at T3 (1st of month). DEV: 7 keys.")
    for k, at in (("B21_1", None), ("B21_2", {"T1": dict(fullname="B21 new")}), ("B21_3", {"T3": dict(fullname="B21 new")})):
        r = c.div(k, fullname="B21 name", at=at)
        c.hist(r, "2026-05-05 10:00:00", 1)
        c.sql("T1", copy_rows(H, H, div_sk_of(k) + " and " + OPEN, "snp.div_h", "p1month='999912'",
                              div_name="'B21 other wave'", ctl_loading="900", ctl_validfrom="timestamp'2026-10-16 12:00:00'"))


def b22():
    c = case(id="B22", chain="dtgt", title="B2: div_type with two types on the same interval",
             summary="Seeded before T1: B22_1 and B22_3 get a second open div_type row [1900, 9999] with type B22_TB "
                     "(the hub row exists through B22_2). B22_1 stays unchanged, B22_3 changes its type to B22_TC at T1. "
                     "DEV: 6 groups.")
    c.div("B22_1", divisiontype00="B22_TA")
    c.div("B22_2", divisiontype00="B22_TB")
    c.div("B22_3", divisiontype00="B22_TA", at={"T1": dict(divisiontype00="B22_TC")})
    tb = "(select max(terr_div_type_sk) from usl_vspc_idl_snp.terr_div_type where sid = 'B22_TB')"
    for k in ("B22_1", "B22_3"):
        c.sql("T1", copy_rows(DT, DT, div_sk_of(k), "snp.div_type", "p1month='999912'",
                              div_type_terr_div_type_sk=tb, ctl_loading="900", ctl_validfrom="timestamp'2026-10-16 12:00:00'"))


def b31():
    c = case(id="B31", chain="dtgt", title="B3: overlapping div_h intervals (3 extra open versions, one from 1900)",
             summary="Seeded before T1: besides the INIT versions, open versions [1900-01-01, MAX] 'v0', [2026-04-01, MAX] "
                     "'v2', [2026-07-01, MAX] 'v3' (prev end 9999-12-31 never closed). B31_1 changes its name at T1, B31_2 "
                     "stays unchanged, B31_3 changes at T3. DEV: 6477 overlapping pairs at 6321 keys.")
    for k, at in (("B31_1", {"T1": dict(fullname="B31 new")}), ("B31_2", None), ("B31_3", {"T3": dict(fullname="B31 new")})):
        r = c.div(k, fullname="B31 name", at=at)
        c.hist(r, "2026-02-02 10:00:00", 1)
        for start, name in (("1900-01-01", "v0"), ("2026-04-01", "v2"), ("2026-07-01", "v3")):
            c.sql("T1", copy_rows(H, H, div_sk_of(k) + " and " + OPEN, "snp.div_h", "p1month='999912'",
                                  div_name="'B31 %s'" % name, start_dt="date'%s'" % start, ctl_loading="900",
                                  ctl_validfrom="timestamp'2026-10-16 12:00:00'"))


def b41():
    c = case(id="B41", chain="dtgt", title="B4: non-canonical legs already in diff (R-only pair in div_, a duplicated leg in div_h)",
             summary="Seeded before T1 under the old loading 900: two (R,U) legs without (U,U) for B41_1 in diff.div_, "
                     "the same (R,I) leg twice in diff.div_h. B41_1 changes its short code at T1 and T3. Diff is "
                     "append-only and never read: the junk must survive identically. DEV: 7 R-only sids, 36 duplicate legs.")
    c.div("B41_1", at={"T1": dict(shortdivcode="02"), "T3": dict(shortdivcode="03")})
    for _ in range(2):
        c.sql("T1", copy_rows("usl_vspc_idl_diff.div_", "usl_vspc_idl_snp.div_", "sid = 'B41_1'", "diff.div_",
                              "ctl_loading=900, ctl_do='R'", ctl_action="'U'"))
        c.sql("T1", copy_rows("usl_vspc_idl_diff.div_h", H, div_sk_of("B41_1"), "diff.div_h",
                              "ctl_loading=900, ctl_do='R'", ctl_action="'I'"))


def b42():
    c = case(id="B42", chain="dsrc", title="B4 (source): unchanged payload over two INCRs vs one field changed per wave",
             summary="B42_1 never changes (A1, A2: no legs expected). B42_2 changes its short code at A1 and again at "
                     "A2 (same day), B42_3 changes its name at A1 and back at A2.")
    c.div("B42_1")
    c.div("B42_2", at={"A1": dict(shortdivcode="02"), "A2": dict(shortdivcode="03")})
    c.div("B42_3", fullname="B42 A", at={"A1": dict(fullname="B42 B"), "A2": dict(fullname="B42 A")})


for _f in [b11, b12, b21, b22, b31, b41, b42]:
    _f()
