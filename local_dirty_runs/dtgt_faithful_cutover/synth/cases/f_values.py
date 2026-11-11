"""F: odd values (audit F1-F4): spaces in codes and names, long strings, one name on many keys and the literal
'null' as a name, p1month layouts (everything in 999912 vs versions spread over months)."""
from model import copy_rows, div_sk_of
from cases import case

H = "usl_vspc_idl_snp.div_h"


def f11():
    c = case(id="F11", chain="dsrc", title="F1: leading/trailing/double spaces in codes, names and type codes",
             summary="fulldivcode ' F11_1' and 'F11_2 ', name with double inner spaces (len 87), type codes 'F11_T ' and "
                     "' F11_T' next to 'F11_T'. At A1 a name gains a trailing space only, a type code loses its space.")
    long_name = ("F11  Отделение  №8590/07777  " + "x" * 87)[:87]
    c.div(" F11_1", fullname=long_name, divisiontype00="F11_T ")
    c.div("F11_2 ", fullname=" F11 name ", divisiontype00=" F11_T", at={"A1": dict(fullname=" F11 name  ")})
    c.div("F11_3", divisiontype00="F11_T ", at={"A1": dict(divisiontype00="F11_T")})


def f21():
    c = case(id="F21", chain="dsrc", title="F2: very long address strings (>100 characters)",
             summary="Fact and legal address texts of 130 / 150 characters; at A1 the fact address changes in character 120.")
    fact = ("F21 fact " + "улица Длинная дом 1 " * 10)[:130]
    c.div("F21_1", address_locationaddress=fact, address_legaladdrdoc=("F21 legal " + "проспект " * 20)[:150],
          at={"A1": dict(address_locationaddress=fact[:119] + "Z" + fact[120:])})


def f31():
    c = case(id="F31", chain="dsrc", title="F3: one name on many keys; the literal string 'null' as a name",
             summary="F31_1..F31_3 are named 'null' (the string), F31_4..F31_8 share '(800)5555550', F31_9 is named 'NULL'. "
                     "At A1 F31_1 becomes '(800)5555550' and F31_4 becomes 'null'.")
    for i in range(1, 10):
        name = "null" if i <= 3 else "(800)5555550" if i <= 8 else "NULL"
        at = {"A1": dict(fullname="(800)5555550")} if i == 1 else {"A1": dict(fullname="null")} if i == 4 else None
        c.div("F31_%d" % i, fullname=name, at=at)


def f41():
    c = case(id="F41", chain="dtgt", title="F4: p1month layouts - closed versions in their months vs all in 999912",
             summary="Seeded before T1: F41_1 gets closed div_h versions [2026-08-05, 2026-11-09] in p1month 202611 and "
                     "[2026-11-10, 2027-01-12] in 202701 (spread layout); F41_2 the same versions but stored in 999912 "
                     "(DEV layout: everything in 999912). Both change their name at T1 and at T3 (1st of month: rewrite "
                     "zone 999912 + 202702 + 202701).")
    for k in ("F41_1", "F41_2"):
        c.div(k, fullname="F41 now", at={"T1": dict(fullname="F41 T1"), "T3": dict(fullname="F41 T3")})
        for start, end, month in (("2026-08-05", "2026-11-09", "202611"), ("2026-11-10", "2027-01-12", "202701")):
            c.sql("T1", copy_rows(H, H, div_sk_of(k), "snp.div_h", "p1month='%s'" % (month if k == "F41_1" else "999912"),
                                  start_dt="date'%s'" % start, end_dt="date'%s'" % end, div_name="'F41 %s'" % start,
                                  ctl_loading="900", ctl_validfrom="timestamp'2026-10-16 12:00:00'"))


for _f in [f11, f21, f31, f41]:
    _f()
