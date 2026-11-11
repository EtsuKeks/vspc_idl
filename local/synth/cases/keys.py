"""Data anomalies: duplicate natural keys, NULL / '' keys, key collisions, overflow."""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case, DV77


def c33():
    c = case(id="C33", accepted=DV77, accept=["div_type:C33_3"], chain="keys", title="Duplicates in one batch (INIT): exact / differing in unread columns / differing type",
             rules=["H13-key-dedup", "R16", "H09-init"], changes=[], tables=["div_", "div_h", "div_type"],
             deterministic=False,
             known40="DV-01: 4.0 does not dedup either; C33_3 must match 3.0 at X0 and fall into a 3.0 outcome at X1/X2 (window 3.0).",
             summary="C33_1: two identical snapshot rows -> one row everywhere (distinct). C33_2: same fulldivcode, "
                     "different id only -> one row. C33_3: same fulldivcode, two different types -> 3.0 INIT writes "
                     "TWO div_type rows with the same PK (no dedup in the loader); 4.0 the same (DV-01). At X1 the C33_3 duplicate is still there and 3.0's result is NON-DETERMINISTIC (R16): no "
                     "expectation; 4.0 runs the same 3.0 window.")
    c.div("C33_1", label="C33_1#a")
    c.div("C33_1", label="C33_1#b")
    c.div("C33_2", label="C33_2#a", id="C33_2a")
    c.div("C33_2", label="C33_2#b", id="C33_2b")
    c.div("C33_3", label="C33_3#a", divisiontype00="C33_TA")
    c.div("C33_3", label="C33_3#b", divisiontype00="C33_TB")
    for k in ("C33_1", "C33_2"):
        c.expect("X0", "snp.div_", k, [hub(RUN("X0"), "X0")])
        c.expect("X0", "snp.div_type", k, [ver(MIN, MAX, RUN("X0"), "X0")])
        c.expect("X1", "snp.div_type", k, [ver(MIN, MAX, RUN("X0"), "X0")])
        c.expect("X2", "snp.div_type", k, [ver(MIN, MAX, RUN("X0"), "X0")])
    c.expect("X0", "snp.div_type", "C33_3", [ver(MIN, MAX, RUN("X0"), "X0", div_type_terr_div_type_sk="C33_TA"),
                                            ver(MIN, MAX, RUN("X0"), "X0", div_type_terr_div_type_sk="C33_TB")])
    c.expect("X0", "snp.div_h", "C33_3", [ver(d("X0"), MAX, START, "X0")])


def c34():
    c = case(id="C34", chain="keys", title="NULL fulldivcode: NULL keys in div_, div_h, div_type",
             rules=["H13-key-dedup", "R04"], changes=["HPWR-vanish-divtype"], tables=["div_", "div_h", "div_type"],
             summary="A snapshot row without fulldivcode produces rows with NULL div_sk in div_, div_h and div_type. "
                     "At X1/X2 (same snapshot) 3.0's window groups the NULL keys together -> EQUALS -> nothing changes.")
    c.div("C34_1", fulldivcode=None, fullname="C34 no code")
    c.owns(None, "NULL fulldivcode of C34_1")
    c.expect("X0", "snp.div_", None, [hub(RUN("X0"), "X0", code=None)])
    c.expect("X0", "snp.div_h", None, [ver(d("X0"), MAX, START, "X0", div_name="C34 no code")])
    c.expect("X0", "snp.div_type", None, [ver(MIN, MAX, RUN("X0"), "X0")])
    for ld in ("X1", "X2"):
        for t in ("div_", "div_h", "div_type"):
            c.expect(ld, "diff." + t, None, [])
        c.expect(ld, "snp.div_type", None, [ver(MIN, MAX, RUN("X0"), "X0")])


def c35():
    c = case(id="C35", chain="keys", title="terr_div_type: divisiontype00 NULL and '' at once (two NULL-sk hub rows)",
             rules=["H13-key-dedup", "H07-snp-incr"], changes=["NH-hash-essa-xxhash-concat"], tables=["terr_div_type"], deterministic=False,
             summary="INIT: NULL gives sid NULL, '' gives sid '' -> two hub rows with NULL sk. Every INCR on the same snapshot: "
                     "all four rows hash alike, 3.0 keeps ONE row (rn=2), re-stamped with that INCR (ctl_validfrom, ctl_loading), "
                     "and writes no diff. Which one (sid NULL or '') is a window tie: not determined in 3.0 or 4.0, not checked.")
    c.div("C35_1", divisiontype00=None)
    c.div("C35_2", divisiontype00="")
    c.owns(None, "NULL terr_div_type_sk of C35_1/C35_2", table="terr_div_type")
    c.expect("X0", "snp.terr_div_type", None, [hub(RUN("X0"), "X0"), hub(RUN("X0"), "X0")])
    c.expect("X1", "snp.terr_div_type", None, [hub(RUN("X1"), "X1")])
    c.expect("X1", "diff.terr_div_type", None, [])
    c.expect("X2", "snp.terr_div_type", None, [hub(RUN("X2"), "X2")])
    c.expect("X2", "diff.terr_div_type", None, [])


def c36():
    c = case(id="C36", accepted=DV77, accept=["time_zone_rf:KALT"], chain="keys", title="rfzonetime duplicates: USZ1 'Kal-A' and KALT 'Kal-B' both become KALT",
             rules=["H13-key-dedup", "R16"], changes=[], tables=["time_zone_rf"], deterministic=False,
             summary="USZ1 is renamed to KALT; with different attributes distinct keeps both -> 3.0 INIT writes TWO "
                     "time_zone_rf rows with the same key. Every INCR: 1 or 2 rows are left (rn=2 survives only if it hashes "
                     "equal to the old row next to it in the window), re-stamped with that INCR. No INCR expectation. 4.0: same (DV-01).")
    c.owns("KALT", "USZ1/KALT time zone rows of C36")
    c.tz("C36_USZ1", code=1, abbreviation="USZ1", name00="C36 Kal-A", timedesignation=-1, utcformat="+2")
    c.tz("C36_KALT", code=2, abbreviation="KALT", name00="C36 Kal-B", timedesignation=-1, utcformat="+2")
    c.expect("X0", "snp.time_zone_rf", "KALT", [hub(RUN("X0"), "X0", time_zone_name="C36 Kal-A", time_zone_code="1"),
                                                hub(RUN("X0"), "X0", time_zone_name="C36 Kal-B", time_zone_code="2")])
    for ld in ("X1", "X2"):
        c.note(ld, "1 or 2 KALT rows left (window tie), re-stamped by the INCR")


def c37():
    c = case(id="C37", accepted=DV77, accept=["div_:C37_1"], chain="keys", title="Same fulldivcode twice with different short codes (div_ duplicates)",
             rules=["H13-key-dedup", "R16", "H09-init"], changes=[], tables=["div_", "div_h"], deterministic=False,
             summary="Two snapshot rows C37_1 with shortdivcode 1 and 2: 3.0 INIT keeps ONE div_ row (which one is "
                     "arbitrary; the audit's 'two rows' does not hold for div_) and one div_h row. INCR: both rows reach div_, 4.0 the same (DV-01).")
    c.div("C37_1", label="C37_1#a", shortdivcode=1)
    c.div("C37_1", label="C37_1#b", shortdivcode=2)
    c.expect("X0", "snp.div_", "C37_1", [hub(RUN("X0"), "X0")], note="ONE row; which short code wins is arbitrary")
    c.expect("X0", "snp.div_h", "C37_1", [ver(d("X0"), MAX, START, "X0")])


def c38():
    c = case(id="C38", chain="keys", title="Source type drift: shortdivcode and rfzonetime.timedesignation are INT in the source (chain keys)",
             rules=["S2-chgdivh-init", "S3-chgdivh-incr"], changes=[], tables=["div_"],
             summary="The chain declares m_sfs.shortdivcode and rfzonetime.timedesignation INT (the latter is visible in C36's "
                     "time_zone_rf rows; parity is checked by the comparator). 3.0 casts to the chg_div_h DDL type (string): "
                     "short_div_code '123', change to 124 at X1 -> (R,U)+(U,U). The run summary also shows the "
                     "stg.chg_div_h schema per load: start_dt stays DATE (D-16).")
    c.div("C38_1", shortdivcode=123, at={"X1": dict(shortdivcode=124)})
    c.expect("X0", "snp.div_", "C38_1", [hub(RUN("X0"), "X0", short_div_code="123")])
    c.expect("X1", "diff.div_", "C38_1", [hleg("R", "U", RUN("X1"), short_div_code="124"), hleg("U", "U", RUN("X0"), short_div_code="123")])


def c39():
    c = case(id="C39", chain="keys", title="div_.reg_ord beyond int32 (3.0 casts to INT -> NULL)",
             rules=["H03-prep"], changes=[], tables=["div_"],
             summary="'1481/3000000000' -> NULL, '1481/2147483647' -> 2147483647 (int max), 'ПАО Сбербанк' with "
                     "'99999999999' -> NULL; full_reg_num keeps the text.")
    for key, regnum, name, ro in [("C39_1", "1481/3000000000", None, None), ("C39_2", "1481/2147483647", None, "2147483647"),
                                  ("C39_3", "99999999999", "ПАО Сбербанк", None)]:
        kw = dict(fullname=name) if name else {}
        c.div(key, requisitesdivision_registrationnum=regnum, **kw)
        c.expect("X0", "snp.div_", key, [hub(RUN("X0"), "X0", reg_ord=ro, full_reg_num=regnum)])


def c40():
    c = case(id="C40", accepted=DV77, accept=["div_div_relation:C40_A|C40_B|C40_C", "div_div_relation:C40_X|NULL"], chain="keys", title="div_div_relation key collisions: '|' inside fulldivcode, literal 'NULL' fulldivcode",
             rules=["L1-link-source", "L3-link-init", "L5-link-incr-outcomes"], changes=[], tables=["div_div_relation"],
             deterministic=False,
             known40="observed 2026-11-09 (v40_fix_classes): DIFFERENT (D-30 open). X1: 4.0 gives both colliding new rows start 1900 "
                     "and leaves both INIT 'C40_X|NULL' rows untouched; 3.0 gives 1900 + D-1 and collapses 'C40_X|NULL' to one row "
                     "[D-1, MAX]; X2: 3.0 relabels again, 4.0 does nothing.",
             summary="Link key = source|successor with 'NULL' for a missing successor. 'C40_A|C40_B'->'C40_C' and "
                     "'C40_A'->'C40_B|C40_C' collide on 'C40_A|C40_B|C40_C' (both new at X1): 3.0 gives one row start 1900 "
                     "and one start D-1 (which source gets which is not guaranteed; only dates are checked); at X2 "
                     "1 or 2 rows are left (window tie). 'C40_X'->unknown and 'C40_X'->division 'NULL' collide on 'C40_X|NULL' "
                     "already at INIT: two rows; every INCR leaves 1 or 2 rows (window tie). No such INCR expectation.")
    c.owns("NULL", "division with the literal fulldivcode 'NULL' (C40)")
    for k in ("C40_A|C40_B", "C40_A", "C40_B|C40_C", "C40_C", "C40_X", "NULL"):
        c.div(k, divisiontype00="C40_T")
    c.succ("C40_A|C40_B", "C40_C", "правопреемник", start="X1")
    c.succ("C40_A", "C40_B|C40_C", "правопреемник", start="X1")
    c.succ("C40_X", "C40_none", "правопреемник")
    c.succ("C40_X", "NULL", "правопреемник")
    k = "C40_A|C40_B|C40_C"
    c.expect("X0", "snp.div_div_relation", "C40_X|NULL", [ver(MIN, MAX, RUN("X0"), "X0"), ver(MIN, MAX, RUN("X0"), "X0")])
    c.expect("X1", "snp.div_div_relation", k, [dict(start_dt=MIN, end_dt=MAX), dict(start_dt=d("X1", -1), end_dt=MAX)])
    c.note("X1", "C40_X|NULL: 1 or 2 rows left (window tie)")
    c.note("X2", "C40_A|C40_B|C40_C and C40_X|NULL: 1 or 2 rows left (window tie)")



def n01():
    c = case(id="N01", chain="nullkeys", title="NULL fulldivcode whose name and type change at INCR, then disappears",
             rules=["H13-key-dedup", "R13", "R07", "R11"], changes=["HPWR-vanish-divtype", "HPWR-close-date-loadDate-2"],
             tables=["div_", "div_h", "div_type"],
             summary="ONE division without fulldivcode. N1: name and type change - div_h of the NULL key closes at D-2 "
                     "and opens [D-1, MAX] with the new name, div_type the same with the new type; div_ gets a SECOND NULL-key "
                     "row ((R,I)+(U,D), the INIT row stays). N2: gone - div_type closes at today-1, div_h and div_ keep "
                     "their rows.")
    c.div("N01_1", fulldivcode=None, fullname="N01 A", divisiontype00="N01_T1",
          at={"N1": dict(fullname="N01 B", divisiontype00="N01_T2"), "N2": GONE})
    c.owns(None, "NULL fulldivcode of N01_1")
    c.expect("N0", "snp.div_", None, [hub(RUN("N0"), "N0", code=None)])
    c.expect("N0", "snp.div_h", None, [ver(d("N0"), MAX, START, "N0", div_name="N01 A")])
    c.expect("N0", "snp.div_type", None, [ver(MIN, MAX, RUN("N0"), "N0", div_type_terr_div_type_sk="N01_T1")])
    h = [ver(d("N0"), d("N1", -2), RUN("N1"), "N1", div_name="N01 A"), ver(d("N1", -1), MAX, RUN("N1"), "N1", div_name="N01 B")]
    c.expect("N1", "snp.div_h", None, h)
    c.expect("N1", "diff.div_h", None, [leg("R", "I", d("N1", -1), MAX, RUN("N1"), div_name="N01 B"),
                                        leg("U", "D", d("N1", -1), MAX, RUN("N1"), div_name="N01 B"),
                                        leg("R", "U", d("N0"), d("N1", -2), RUN("N1"), div_name="N01 A"),
                                        leg("U", "U", d("N0"), MAX, RUN("N1"), div_name="N01 A")])
    c.expect("N1", "diff.div_", None, [hleg("R", "I", RUN("N1")), hleg("U", "D", RUN("N1"))],
             note="the changed NULL-key hub row is inserted as NEW, the INIT row stays: two NULL-key div_ rows")
    c.expect("N1", "snp.div_type", None, [ver(MIN, d("N1", -2), RUN("N1"), "N1", div_type_terr_div_type_sk="N01_T1"),
                                          ver(d("N1", -1), MAX, RUN("N1"), "N1", div_type_terr_div_type_sk="N01_T2")])
    c.expect("N2", "snp.div_type", None, [ver(MIN, d("N1", -2), RUN("N1"), "N1", div_type_terr_div_type_sk="N01_T1"),
                                          ver(d("N1", -1), d("N2", -1), RUN("N2"), "N2", div_type_terr_div_type_sk="N01_T2")])
    c.expect("N2", "diff.div_type", None, [leg("R", "U", d("N1", -1), d("N2", -1), RUN("N2")), leg("U", "U", d("N1", -1), MAX, RUN("N2"))])
    c.expect("N2", "snp.div_h", None, h)
    c.expect("N2", "snp.div_", None, [hub(RUN("N0"), "N0", code=None), hub(RUN("N1"), "N1", code=None)])


for _f in [c33, c34, c35, c36, c37, c38, c39, c40, n01]:
    _f()
