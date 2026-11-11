"""Audit 4.3 dirty paths, theme perf (ids X..): load from dirt (DV-49, DV-52, DV-54, DV-46, DV-57, DV-30), chains dgrow and dtwincut."""
# Audit 4.3 (dirty paths), theme perf (ids X..): load from dirt (class B) - DV-49, DV-52, DV-54, DV-46, DV-57, DV-30
# and SVOD_dirty "Нагрузка от грязи". Two NEW chains with dirt inflated by the catalog itself (hundreds to thousands of
# rows): the stand only shows seconds per load (timings.tsv) and table sizes (dumps), so these chains exist to compare
# 3.0 vs 4.0 time and table growth on dirty input; row expectations are minimal (sample keys, stable projections).
#
#   dgrow     INIT + 4 INCR, both versions, halves in parallel: 600 churning keys with d_ history and addresses (X01),
#             300 keys x3 exact duplicate snapshot rows (X02), 300 keys vanishing/returning twice (X03), a NULL-fulldivcode
#             group of 100 rows renamed on every INCR (X04: 3.0 keeps ONE open NULL version, 4.0 by code +100 per INCR, DV-81).
#   dtwincut  3.0-only INIT writes the dirty target (div_type twins, time_zone_rf twins, NULL/'' div_ pair, 500 plain keys),
#             then both run an INCR with EMPTY sfsdivision (DV-57) and an INCR where everything returns at once (mass
#             div_type returners, DV-46). Cutover chain: halves run one after the other, so timings are not disturbed
#             by the other half (the only honest timing mode of the stand, SVOD_dirty gap 5).
#
# Paths in summaries: 3.0 = vspc_idl_old/src/main/java/ru/sberbank/bigdata/dffess/vspc/, ESSA = essa-common-lib
# .../essa/common/.
#
# THE CHAINS ARE NOT IN model.py YET. To connect this file add to CHAINS in synth/model.py (loadingIds 20400-20499,
# labels G10-G14 and M0-M2 are not used by other chains):
#
#     "dgrow": Chain("Dirt accumulated over 4 INCRs at volume (perf, DV-49/DV-54/DV-46/DV-81): churning keys with history, "
#                    "exact duplicate snapshot rows, mass vanish/return, a renamed NULL-key group.",
#                    "INIT with 1900 snapshot rows + 1200 d_ rows; 4 INCRs on consecutive days", _days("G1", 20400, [
#         ("INIT", "2027-01-13 10:00", "INIT: 1900 snapshot rows (300 x3 exact duplicates, 100 NULL-key rows), 1200 d_ rows."),
#         ("INCR", "2027-01-14 10:00", "150 renames, 300 keys vanish, the NULL group renamed."),
#         ("INCR", "2027-01-15 10:00", "150 renames, 300 keys return at once, the NULL group renamed."),
#         ("INCR", "2027-01-16 10:00", "150 renames, 300 keys vanish again, the NULL group renamed."),
#         ("INCR", "2027-01-17 10:00", "150 renames, 300 keys return again, the NULL group renamed."),
#     ])),
#     "dtwincut": Chain("3.0 writes a dirty target (div_type / time_zone_rf / NULL-key div_ twins), 4.0 takes over: "
#                       "empty-sfsdivision INCR over twins (DV-57), then every division returns at once (DV-46).",
#                       "INIT 3.0 with twins -> INCR empty sfsdivision -> INCR everything returns", [
#         Load("M0", "INIT", "2027-01-13 10:00:00", 20450, "INIT on 3.0 only: 3.0 itself writes twins; the 4.0 half starts from this state.", "30"),
#         Load("M1", "INCR", "2027-01-14 10:00:00", 20451, "INCR: sfsdivision empty, rfzonetime unchanged (twins stay in the input)."),
#         Load("M2", "INCR", "2027-01-15 10:00:00", 20452, "INCR: all 802 snapshot rows return at once."),
#     ]),
#
# Expected wall time on the VM (from final_* runs: 150-230 s per load and version): dgrow 5 loads x 2 parallel halves
# ~17-19 min; dtwincut 3 (3.0) + 2 (4.0) sequential loads ~13-17 min.
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case, DV77

N01, N02, N03, K04 = 600, 300, 300, 100     # dgrow volumes
N11, N12, N13 = 500, 150, 40                 # dtwincut volumes
INCRS = ("G11", "G12", "G13", "G14")

_HISTMIX = ("3.0 impl/loader/DataLoaderHistMix.java:66 (only 999912 merged), :68-69 (current+previous month as is), "
            ":75-81 (kept rows), :84-89 (closure at D-2, dropped when end < start), :126-139 (legs)")
_WINDOW = "ESSA slave/loading/SnpDataLoader.java:181 (window by PK, desc(ctl_loading), asc(record_status)), :195-204"
_HISTSNP = ("ESSA slave/loading/DataLoaderHistSnp.java:80-96 (returner start = cached end + 1), :101-108 (vanish closes "
            "at current_date-1), :118-119 (dropped when end < start); slave/loading/LoaderCache.java:96-114")


def _k(cid, i):
    return "%s_%04d" % (cid, i)


def x01():
    c = case(id="X01", chain="dgrow", title="perf DV-49: 600 keys with d_ history and addresses, 150 renames per INCR (history grows in 202701)",
             rules=["R10", "L8-histmix-init", "R07", "R11", "L9-histmix-incr"], changes=["HPWR-close-date-loadDate-2"],
             tables=["div_h", "div_", "div_type", "addr", "addr_h"],
             summary="Volume and churn for timing (DV-49: compat window + 999912 read 6-10 times per INCR in 4.0, once in 3.0; "
                     "DV-52: rebalance by p1month). 600 divisions, each with a fact address and two d_ versions (2026-03-10 "
                     "other name, 2026-06-01 = snapshot): INIT div_h [2026-03-10, 2026-05-31] (202605) + [2026-06-01, MAX] "
                     "(3.0 tables/chgdivh/ChgDivHSql.java:177,207,245,294-297; ChgDivHTransformer.java:24). Key i is renamed "
                     "at V((i-1)%4+1): open version closed at D-2 into 202701, new [D-1, MAX], 4 legs (" + _HISTMIX + "). "
                     "Hub div_, div_type, addr, addr_h untouched by a rename (3.0 tables/div/DivTransformer.java:34-88 has no "
                     "name; AddrHTransformer.java:35-85). After G14: div_h 1800 rows (600 in 999912), addr 600, addr_h 600. "
                     "Expectations on 5 sample keys; the rest is for timings.tsv and table sizes.")
    samples = (1, 2, 3, 4, N01)
    for i in range(1, N01 + 1):
        k = _k(c.id, i)
        cl = INCRS[(i - 1) % 4]
        r = c.div(k, address_locationaddress="%s fact" % k, at={cl: dict(fullname="%s b" % k)})
        # d_ is read by INIT only (ChgDivHSql.java:211-290): gone from G11 on to keep the INCR seeds small
        c.hist(r, "2026-03-10 10:00:00", 1, fullname="%s a" % k, at={"G11": GONE})
        c.hist(r, "2026-06-01 10:00:00", 2, at={"G11": GONE})
        if i not in samples:
            continue
        v1 = ver("2026-03-10", "2026-05-31", START, "G10", div_name="%s a" % k, p1month="202605")
        cur = ver("2026-06-01", MAX, START, "G10", div_name="%s name" % k, p1month="999912")
        after = [v1, ver("2026-06-01", d(cl, -2), RUN(cl), cl, div_name="%s name" % k, p1month="202701"),
                 ver(d(cl, -1), MAX, RUN(cl), cl, div_name="%s b" % k, p1month="999912")]
        c.expect("G10", "snp.div_h", k, [v1, cur], note="the snapshot equals the last d_ version -> start = its ctl_validfrom")
        c.expect("G10", "diff.div_h", k, [])
        seen = False
        for ld in INCRS:
            seen = seen or ld == cl
            c.expect(ld, "snp.div_h", k, after if seen else [v1, cur])
            c.expect(ld, "diff.div_h", k, [
                leg("R", "I", d(cl, -1), MAX, RUN(cl), div_name="%s b" % k),
                leg("U", "D", d(cl, -1), MAX, RUN(cl), div_name="%s b" % k),
                leg("R", "U", "2026-06-01", d(cl, -2), RUN(cl), div_name="%s name" % k),
                leg("U", "U", "2026-06-01", MAX, RUN(cl), div_name="%s name" % k)] if ld == cl else [])
        a = addr_key(src="%s fact" % k)
        c.expect("G14", "snp.div_", k, [hub(RUN("G10"), "G10")], note="a rename does not touch the hub")
        c.expect("G14", "snp.div_type", k, [ver(MIN, MAX, RUN("G10"), "G10")])
        c.expect("G14", "snp.addr", a, [hub(RUN("G10"), "G10")])
        c.expect("G14", "snp.addr_h", a, [ver(MIN, MAX, RUN("G10"), "G10")])


def x02():
    c = case(id="X02", chain="dgrow", title="perf DV-54: 300 keys, each THREE identical snapshot rows on every load",
             rules=["H13-key-dedup", "R07"], changes=[], tables=["div_", "div_h", "div_type"],
             summary="Dirt in the input only (DV-54: 4.0 keeps the SSK draft <t>_mapped with the duplicates cached until the "
                     "end of the run; the stand sees only the time). 3.0 collapses exact duplicates before the loaders "
                     "(3.0 tables/chgdivh/ChgDivHSql.java:252 UNION at INIT, ChgDivHTransformer.java:24 distinct at INCR, "
                     "DivTypeTransformer.java:42, DivTransformer.java:62,88, DivHTransformer.java:54,78): one row per key "
                     "everywhere, every INCR EQUALS -> no legs (" + _WINDOW + "). Same as C33_1, at volume.")
    for i in range(1, N02 + 1):
        k = _k(c.id, i)
        for t in "abc":
            c.div(k, label="%s#%s" % (k, t))
        if i not in (1, N02):
            continue
        for ld in ("G10", "G14"):
            c.expect(ld, "snp.div_", k, [hub(RUN("G10"), "G10")])
            c.expect(ld, "snp.div_h", k, [ver(d("G10"), MAX, START, "G10")])
            c.expect(ld, "snp.div_type", k, [ver(MIN, MAX, RUN("G10"), "G10")])
        for ld in INCRS:
            for t in ("div_", "div_h", "div_type"):
                c.expect(ld, "diff." + t, k, [])


def x03():
    c = case(id="X03", chain="dgrow", title="perf DV-46/DV-28: 300 keys vanish (G11), return at once (G12), vanish (G13), return (G14)",
             rules=["R13", "R14", "R02", "H02-input-is-full-snapshot"], changes=["HPWR-vanish-divtype", "HPWR-returner-start-date"],
             tables=["div_type", "div_h", "div_"],
             summary="Mass returners (DV-46: 4.0 broadcast(cand) of returners + scan of the whole closed div_type history, "
                     "PRC:245-259; 3.0 reads stg.loader_cache_div_type). 3.0: vanish closes div_type at current_date-1, "
                     "return reopens at cached end + 1 (" + _HISTSNP + "); div_h and the hub keep the key (C06 pattern, two "
                     "cycles). div_type after G14: 3 rows per key (900), two closed in 202701.")
    for i in range(1, N03 + 1):
        k = _k(c.id, i)
        c.div(k, at={"G11": GONE, "G12": {}, "G13": GONE, "G14": {}})
        if i not in (1, N03):
            continue
        a = ver(MIN, d("G11", -1), RUN("G11"), "G11", p1month="202701")
        b = ver(d("G11"), d("G13", -1), RUN("G13"), "G13", p1month="202701")
        c.expect("G10", "snp.div_type", k, [ver(MIN, MAX, RUN("G10"), "G10")])
        c.expect("G11", "snp.div_type", k, [a], note="vanish closes at today-1")
        c.expect("G11", "diff.div_type", k, [leg("R", "U", MIN, d("G11", -1), RUN("G11")), leg("U", "U", MIN, MAX, RUN("G11"))])
        c.expect("G12", "snp.div_type", k, [a, ver(d("G11"), MAX, RUN("G12"), "G12", p1month="999912")],
                 note="returner: start = cached end + 1 = %s (not D-1)" % d("G11"))
        c.expect("G12", "diff.div_type", k, [leg("R", "I", d("G11"), MAX, RUN("G12")), leg("U", "D", d("G11"), MAX, RUN("G12"))])
        c.expect("G13", "snp.div_type", k, [a, b])
        c.expect("G13", "diff.div_type", k, [leg("R", "U", d("G11"), d("G13", -1), RUN("G13")), leg("U", "U", d("G11"), MAX, RUN("G13"))])
        c.expect("G14", "snp.div_type", k, [a, b, ver(d("G13"), MAX, RUN("G14"), "G14", p1month="999912")],
                 note="second return: start = %s (the G13 closure + 1)" % d("G13"))
        c.expect("G14", "diff.div_type", k, [leg("R", "I", d("G13"), MAX, RUN("G14")), leg("U", "D", d("G13"), MAX, RUN("G14"))])
        for ld in INCRS:
            c.expect(ld, "snp.div_h", k, [ver(d("G10"), MAX, START, "G10")], note="vanish/return never touch div_h")
            c.expect(ld, "diff.div_h", k, [])
        c.expect("G14", "snp.div_", k, [hub(RUN("G10"), "G10")])


def x04():
    c = case(id="X04", accepted=DV77, accept=["div_h:\\N"], chain="dgrow", title="perf DV-81/DV-54: NULL-fulldivcode group of 100 rows, every row renamed on every INCR",
             rules=["H13-key-dedup", "R04", "R07", "R11", "L9-histmix-incr"], changes=["HPWR-close-date-loadDate-2"],
             tables=["div_h", "div_type", "div_"], deterministic=False,
             known40="by code (SVOD_dirty DV-81, not run): the NULL key goes past window30 (PERIOD_NULL_KEY, "
                     "W/compat/period/PeriodRewriteCompatCalculator.java:198-201,224-227): each of the 100 rows opens [D-1, MAX] with "
                     "PK (NULL, D-1), the previous open NULL versions are closed at D-2 (G11: dropped, start > D-2): div_h NULL rows "
                     "100 / 200 / 300 / 400 after G11..G14 against 1 in 3.0",
             summary="Big NULL group (SVOD_dirty Нагрузка п.4: the only unbounded snp growth). INIT: the 100 rows share "
                     "start_dt = current_date in the chg_div_h window, all but one get end = start-1 and are dropped "
                     "(3.0 tables/chgdivh/ChgDivHSql.java:177,288,297): ONE div_h row [G10, MAX]; which name survives is a tie "
                     "(DV-78, not checked). Every INCR: 100 inputs (distinct names, ChgDivHTransformer.java:24) + 1 stored row "
                     "in ONE window partition (" + _WINDOW + "): rn1 input NOTEQUALS -> kept [D-1, MAX]; rn2 input closes at "
                     "D-2 < D-1 -> dropped; the stored row (rn 101) is dropped without legs (" + _HISTMIX + "). So 3.0 keeps "
                     "exactly one NULL div_h row [D-1, MAX] with (R,I)+(U,D) per INCR; which name - not checked. div_type: "
                     "one type for all rows -> one NULL row, EQUALS every INCR (DivTypeTransformer.java:42; C34). div_ NULL "
                     "row: checked at INIT only (C34 and N01 disagree on an INCR re-insert).")
    c.owns(None, "div_sk NULL of the X04 NULL-key group")
    for j in range(1, K04 + 1):
        at = {ld: dict(fullname="X04 n%03d %s" % (j, ld)) for ld in INCRS}
        c.div(None, label="X04_null_%03d" % j, fullname="X04 n%03d G10" % j, at=at)
    c.expect("G10", "snp.div_h", None, [ver(d("G10"), MAX, START, "G10")], note="one of the 100 names survives (tie)")
    c.expect("G10", "snp.div_", None, [hub(RUN("G10"), "G10", code=None)])
    c.expect("G10", "snp.div_type", None, [ver(MIN, MAX, RUN("G10"), "G10")])
    for ld in INCRS:
        c.expect(ld, "snp.div_h", None, [ver(d(ld, -1), MAX, RUN(ld), ld)],
                 note="3.0: one open NULL version, the previous one dropped without legs; 4.0 (DV-81): +100 per INCR")
        c.expect(ld, "diff.div_h", None, [leg("R", "I", d(ld, -1), MAX, RUN(ld)), leg("U", "D", d(ld, -1), MAX, RUN(ld))])
        c.expect(ld, "snp.div_type", None, [ver(MIN, MAX, RUN("G10"), "G10")])
        c.expect(ld, "diff.div_type", None, [])
        c.note(ld, "X04 div_ NULL row: not asserted (C34 EQUALS vs N01 re-insert)")


# ---------------------------------------------------------------- dtwincut: 3.0 writes the dirt, 4.0 takes over


def x11():
    c = case(id="X11", chain="dtwincut", title="perf DV-57/DV-46: 500 plain keys with addresses: empty sfsdivision INCR, then all return at once",
             rules=["R19", "H11-empty-incr", "R13", "R14"], changes=["HPWR-vanish-divtype", "HPWR-returner-start-date"],
             tables=["div_type", "div_h", "div_", "addr", "addr_h"],
             summary="Background volume of the cutover chain. M0 (3.0 only) INIT. M1: sfsdivision empty - 3.0 still runs every "
                     "loader (ESSA slave/check/EmptyChecker.java:12-16): div_type closes ALL 650 open keys at today-1 with "
                     "(R,U)+(U,U) (SVOD_dirty п.9: the empty INCR is the heaviest 3.0 INCR), hubs/div_h/addr_h unchanged "
                     "(E01 pattern). M2: all 500 return at once: div_type reopens at cached end + 1 = M1 date (" + _HISTSNP + "): "
                     "500 + 300 returners in one batch for 4.0's broadcast(cand) (DV-46).")
    for i in range(1, N11 + 1):
        k = _k(c.id, i)
        c.div(k, address_locationaddress="%s fact" % k, at={"M1": GONE, "M2": {}})
        if i not in (1, N11):
            continue
        a = ver(MIN, d("M1", -1), RUN("M1"), "M1", p1month="202701")
        c.expect("M0", "snp.div_type", k, [ver(MIN, MAX, RUN("M0"), "M0")])
        c.expect("M1", "snp.div_type", k, [a])
        c.expect("M1", "diff.div_type", k, [leg("R", "U", MIN, d("M1", -1), RUN("M1")), leg("U", "U", MIN, MAX, RUN("M1"))])
        c.expect("M2", "snp.div_type", k, [a, ver(d("M1"), MAX, RUN("M2"), "M2", p1month="999912")],
                 note="returner: start = closed end + 1 = %s" % d("M1"))
        c.expect("M2", "diff.div_type", k, [leg("R", "I", d("M1"), MAX, RUN("M2")), leg("U", "D", d("M1"), MAX, RUN("M2"))])
        ak = addr_key(src="%s fact" % k)
        for ld in ("M1", "M2"):
            c.expect(ld, "snp.div_h", k, [ver(d("M0"), MAX, START, "M0")])
            c.expect(ld, "diff.div_h", k, [])
            c.expect(ld, "snp.div_", k, [hub(RUN("M0"), "M0")])
            c.expect(ld, "diff.div_", k, [])
            c.expect(ld, "snp.addr", ak, [hub(RUN("M0"), "M0")])
            c.expect(ld, "snp.addr_h", ak, [ver(MIN, MAX, RUN("M0"), "M0")])


def x12():
    c = case(id="X12", accepted=DV77, accept=["div_type:" + _k("X12", i) for i in range(1, N12 + 1)], chain="dtwincut", title="perf DV-49/DV-77: 150 div_type twins written by the 3.0 INIT (two types per key)",
             rules=["H13-key-dedup", "R16", "H09-init"], changes=[], tables=["div_type", "div_h", "terr_div_type"],
             deterministic=False,
             summary="Target dirt from 3.0 itself (C33_3 / G01_6 pattern at volume): each key has two snapshot rows with "
                     "different divisiontype00; div_type has no dedup (3.0 tables/divtype/DivTypeTransformer.java:39-42) -> "
                     "M0 writes TWO div_type rows [MIN, MAX] with one PK per key (300 rows, 300 terr_div_type rows); div_h one "
                     "row (the type is not in chg_div_h, UNION at ChgDivHSql.java:252). M1 (empty) and M2 (both types "
                     "back) go through the 3.0 window over the twins (" + _WINDOW + "; " + _HISTSNP + "): the outcome is a "
                     "tie (R16, DV-77, G01_6 gave 2 or 1 rows) - no M1/M2 expectations; the case is there for the time of "
                     "window30 over 150 twin groups and for the div_type size after M2.")
    for i in range(1, N12 + 1):
        k = _k(c.id, i)
        for t in "AB":
            c.div(k, label="%s#%s" % (k, t), divisiontype00="%s%s" % (k, t), at={"M1": GONE, "M2": {}})
        if i not in (1, N12):
            continue
        c.expect("M0", "snp.div_type", k, [ver(MIN, MAX, RUN("M0"), "M0", div_type_terr_div_type_sk=k + "A"),
                                           ver(MIN, MAX, RUN("M0"), "M0", div_type_terr_div_type_sk=k + "B")],
                 note="duplicate PK (div_sk, start_dt) written by 3.0")
        c.expect("M0", "snp.div_h", k, [ver(d("M0"), MAX, START, "M0")])
        c.expect("M0", "snp.terr_div_type", k + "A", [hub(RUN("M0"), "M0")])
    for ld in ("M1", "M2"):
        c.note(ld, "X12 div_type twins: window tie in 3.0 (R16/DV-77), not asserted")


def x13():
    c = case(id="X13", accepted=DV77, accept=["time_zone_rf:X13_TZ%02d" % j for j in range(1, N13 + 1)], chain="dtwincut", title="perf DV-57/DV-77: 40 time_zone_rf twins (one abbreviation, two codes) from the 3.0 INIT",
             rules=["H13-key-dedup", "R16"], changes=[], tables=["time_zone_rf"], deterministic=False,
             summary="Hub twins in snp (H05 G0 pattern at volume): rfzonetime has two rows per abbreviation with different "
                     "code/name, distinct keeps both (3.0 tables/timezone/TimeZoneTransformer.java:30-42), the INIT writes "
                     "both (ESSA slave/loading/DataLoaderHubDiff.java:102-108): 80 rows, 40 duplicate PKs. rfzonetime is "
                     "unchanged at M1/M2: the 3.0 window leaves 1 or 2 rows per key, re-stamped (C36, tie) - not asserted. "
                     "With twins in the hub snp the 4.0 empty-INCR marker and window30 run on every INCR (DV-57, DV-49).")
    for j in range(1, N13 + 1):
        ab = "X13_TZ%02d" % j
        c.tz(ab + "a", code=13000 + 2 * j, abbreviation=ab, name00="X13 %02d A" % j, timedesignation="0", utcformat="+3")
        c.tz(ab + "b", code=13001 + 2 * j, abbreviation=ab, name00="X13 %02d B" % j, timedesignation="0", utcformat="+3")
        if j in (1, N13):
            c.expect("M0", "snp.time_zone_rf", ab, [hub(RUN("M0"), "M0", time_zone_name="X13 %02d A" % j, time_zone_code=str(13000 + 2 * j)),
                                                   hub(RUN("M0"), "M0", time_zone_name="X13 %02d B" % j, time_zone_code=str(13001 + 2 * j))])
    for ld in ("M1", "M2"):
        c.note(ld, "X13 time_zone_rf twins: 1 or 2 rows per key (tie), not asserted")


def x14():
    c = case(id="X14", accepted=DV77, accept=["div_:\\N"], chain="dtwincut", title="perf DV-57/DV-07: NULL-key div_ twins (fulldivcode NULL and '') before the empty INCR",
             rules=["H11-empty-incr", "R19", "H13-key-dedup"], changes=["HPWR-vanish-divtype"], tables=["div_", "div_type"],
             deterministic=False,
             known40="by code (SVOD_dirty DV-57): the NULL group of 2 rows in snp.div_ makes the empty-INCR marker "
                     "(W/compat/CompatHooks.java:57-66) run the full vendor + compat pipeline at M1; content side = H06 (DV-07)",
             summary="H06 on the cutover chain: M0 (3.0) writes two div_ and two div_type rows with div_sk NULL (fulldivcode "
                     "NULL and '' fall into different chg_div_h window partitions, 3.0 tables/chgdivh/ChgDivHSql.java:177,207). "
                     "M1 (sfsdivision empty): 3.0 runs the loaders anyway (ESSA slave/check/EmptyChecker.java:12-16); div_: rn1 OLD "
                     "kept, rn2 NOTEQUALS dropped with (U,U) (subbranch '0'||NULL vs '0', DivTransformer.java:42; ESSA "
                     "DataLoaderHubDiff.java:75-80); div_type: rn1 closed today-1, rn2 EQUALS stays open (" + _HISTSNP + "). "
                     "Which sid survives is a tie (not checked). M2: both return - not asserted.")
    c.div(None, label="X14_null", fullname="X14 no code", at={"M1": GONE, "M2": {}})
    c.div("", label="X14_empty", fullname="X14 no code", at={"M1": GONE, "M2": {}})
    c.owns(None, "div_sk NULL of X14_null / X14_empty (fulldivcode NULL and '')")
    c.owns("", "fulldivcode '' of X14_empty", table="chg_div_h")
    c.expect("M0", "snp.div_", None, [hub(RUN("M0"), "M0"), hub(RUN("M0"), "M0")])
    c.expect("M0", "snp.div_type", None, [ver(MIN, MAX, RUN("M0"), "M0"), ver(MIN, MAX, RUN("M0"), "M0")])
    c.expect("M1", "snp.div_", None, [hub(RUN("M0"), "M0")], note="3.0: one NULL-key row left")
    c.expect("M1", "diff.div_", None, [hleg("U", "U", RUN("M0"))], note="3.0: (U,U) of the dropped row")
    c.expect("M1", "snp.div_type", None, [ver(MIN, d("M1", -1), RUN("M1"), "M1"), ver(MIN, MAX, RUN("M0"), "M0")],
             note="3.0: rn1 closed today-1, rn2 (EQUALS) stays open")
    c.expect("M1", "diff.div_type", None, [leg("R", "U", MIN, d("M1", -1), RUN("M1")), leg("U", "U", MIN, MAX, RUN("M1"))])
    c.note("M2", "X14 NULL group returns: not asserted")


for _f in [x01, x02, x03, x04, x11, x12, x13, x14]:
    _f()
