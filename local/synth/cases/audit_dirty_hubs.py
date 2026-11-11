"""Audit 4.3 (dirty paths), theme hubs: hub twins and NULL-key groups that 3.0 itself writes into the target, then
4.0 continues from the 3.0 root (DV-01, DV-03, DV-07, DV-57, DV-77). Case letter A.

How the dirty target is made: the first loads of a chain run on 3.0 only (by="30"); the 4.0 half starts from a copy
of the 3.0 root after them (as in chain `cutover`), so both versions run the following INCRs over the same twins.
Two NEW chains (the coordinator adds them to CHAINS; labels H and V, loadingIds 20000-20004 and 20050-20054):
  hubtwins  H0 INIT (3.0) -> H1 INCR (3.0: new keys arrive twice, 3.0 keeps both rows) -> H2 INCR (4.0's first INCR
            over the 3.0 twins) -> H3 INCR -> H4 INCR with every source empty. Cases A01-A04.
  hubnull   V0 INIT (3.0: NULL-key pairs in div_/terr_div_type, time zone twins) -> V1 INCR every source empty
            (4.0's first INCR) -> V2/V3 INCR with a 20-row group without fulldivcode -> V4 one row. Cases A05-A07.
            Kept apart: the NULL div_sk group is one key per chain, and the NULL-key PERIOD path of 4.0 (DV-81)
            must not stop the twins chain.

3.0 on this stand (checked before writing, 33-34 dumps of chain keys and 15-22 of cutover/emptynull in
colab/dispatcher/results/*/target/runs and local/target/runs): hub window ties are stable here - C36 X1/X2 Kal-B
32/32, C37 X1 given its X0 row 34/34, H05 G1 (U,U)='H05 A' 18/18, H06 U1 sid '' 15/15 - because NEW rows come in the
physical order of the source file (insert order = (case id, label)) and OLD rows in the order of the snp file. The
INIT tie of stg.chg_div_h is NOT stable (C37 X0 short code '2' in 20 runs, '1' in 14: DV-78), so every twin here is
created by an INCR, never by an INIT of one fulldivcode with two rows. By code the ties are not determined
(E/slave/loading/SnpDataLoader.java:181 orders only by desc(ctl_loading), asc(record_status)), so expectations are
only projections that hold for every order; the stand branch is in the notes.

3.0 window per hub pk (E/slave/loading/SnpDataLoader.java:179-205, selection and legs
E/slave/loading/DataLoaderHubDiff.java:75-80,127-144, hash :116-133 with NULL -> ''): NEW rows first; a NEW row
compares with lead, an OLD row with lag. Consequences used below, valid for any order of equal rows:
  * twins {x,y} in snp, input z (z differs from both): {z@L}, (R,U) z + (U,U) of the rn2 OLD row with its own vf;
  * twins in snp, key absent from the input: one row with the old stamps, one (U,U); exact (hash-equal) twins stay;
  * >= 3 distinct NEW rows in one group (any number of OLD rows): one row (rn1 NEW) and (R,U)+(U,U), both @L;
  * 2 distinct NEW rows and no OLD row (new key twice): both kept, (R,U)+(U,I)+(U,D) (H01);
  * after an INCR whose input has >= 2 rows for the key, every surviving row carries that INCR's stamps.
Hub pk: VspcMetadataKeeper.java:20,23,24 (div_sk, terr_div_type_sk, time_zone_rf_sk), no excludeHashColumns.

Not expressible on this stand (-> IFT): DV-56 (two ctl_validfrom stamps in one load are inside the 3 h
class RUN@Lk of the comparator), DV-63 (the stand SK mock never returns Long.MIN_VALUE), DV-21 (debug_sk_off is a
CTL parameter of 4.0), the cost side of DV-57 (no event log on the stand) -> IFT.

Paths: E/ = essa-common-lib 2.2.6 (.../essa/common/), O/ = 3.0 vspc_idl_old (.../dffess/vspc/), W/ = 4.0 (.../dffess/vspc/).
"""
from model import (Case, GONE, MIN, MAX, NOTNULL, NULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case, DV77

_WIN = "E/slave/loading/SnpDataLoader.java:179-205, E/slave/loading/DataLoaderHubDiff.java:75-80,127-144"
_HCC = "W/compat/hub/HubCompatCalculator.java:115-136,175-190 (dupKeys -> window30, ties by POS/IPOS/HN)"


def _new_twin_legs(ld):
    return [hleg("R", "U", RUN(ld)), hleg("U", "I", RUN(ld)), hleg("U", "D", RUN(ld))]


def _gone_h4(at=None):
    """Every row of chain hubtwins is absent at H4 (every source empty)."""
    a = dict(at or {})
    a["H4"] = GONE
    return a


def _tz(c, label, abbr, code, name, start=None, at=None):
    return c.tz(label, start=start, at=at, code=code, abbreviation=abbr, name00=name, timedesignation="0", utcformat="+3")


def a01():
    c = case(id="A01", accepted=DV77, accept=["div_:A01_4", "div_:A01_5"], chain="hubtwins",
             title="DV-03/DV-77: div_ twins written by 3.0 (new key twice at H1), then 4.0's first INCR with 5 input variants",
             rules=["H13-key-dedup", "R16", "H06-record-status", "H07-snp-incr", "H08-diff-legs-incr"],
             changes=["NH-undo-keep-validfrom"], tables=["div_"], deterministic=False,
             known40="not run. By code: snp pk with > 1 row -> dupKeys -> window30 with the 3.0 selection (" + _HCC + "); "
                     "A01_1/2/4 do not depend on the order and should match; A01_3 (stamps, legs) and A01_5 depend on POS "
                     "= scan order of the snp file (DV-77)",
             summary="H1 (3.0 only): A01_1..A01_5 arrive twice with short codes 01/02; INCR chg_div_h is the raw snapshot "
                     "with distinct only (O/tables/chgdivh/ChgDivHSql.java:12-87, ChgDivHTransformer.java:24, "
                     "O/tables/div/DivTransformer.java:62,88), both rows NEW with lead NULL / differing -> 3.0 keeps both "
                     "(" + _WIN + "): twins with one div_sk and equal ctl_loading in snp. H2 = 4.0's first INCR over them: "
                     "A01_1 input '03' -> {03@H2}, (R,U) 03 + (U,U) of the rn2 twin with RUN@H1 (any order). A01_2 gone "
                     "-> rn1 OLD kept with H1 stamps, rn2 NOTEQUALS -> (U,U). A01_3 input '01' -> one row '01' whatever "
                     "the OLD order; stamps and legs are the OLD-OLD tie: rn2=OLD 01 gives EQUALS rn2 (old stamps, no legs), "
                     "rn2=OLD 02 gives {01@H2}, (R,U)+(U,U). A01_4 input 01/02/03 (3 NEW) -> one row (rn1) and (R,U)+(U,U) "
                     "@H2 at H2 and again at H3. A01_5 input 01/02 again (C37 steady state): 1 or 2 rows, (R,U) [+(U,U)] - "
                     "tie, only H4 is asserted: after H3 every row carries H3 stamps, the empty H4 leaves exactly one. "
                     "A01_0 is a plain control key.")
    c.div("A01_0", at=_gone_h4())
    variants = {  # key: (at of #a, at of #b, start of #c or None)
        "A01_1": ({"H2": GONE}, {"H2": GONE}, "H2"),
        "A01_2": ({"H2": GONE}, {"H2": GONE}, None),
        "A01_3": (_gone_h4(), {"H2": GONE}, None),
        "A01_4": (_gone_h4(), _gone_h4(), "H2"),
        "A01_5": (_gone_h4(), _gone_h4(), None),
    }
    for k, (at_a, at_b, start_c) in variants.items():
        c.div(k, label=k + "#a", start="H1", shortdivcode="01", at=at_a)
        c.div(k, label=k + "#b", start="H1", shortdivcode="02", at=at_b)
        if start_c:
            c.div(k, label=k + "#c", start=start_c, shortdivcode="03", at=_gone_h4())
        c.expect("H0", "snp.div_", k, [])
        c.expect("H1", "snp.div_", k, [hub(RUN("H1"), "H1", short_div_code="01"), hub(RUN("H1"), "H1", short_div_code="02")],
                 note="3.0 keeps both NEW rows of the new key (H01 mechanism): twins in the target")
        c.expect("H1", "diff.div_", k, _new_twin_legs("H1"))
    c.expect("H2", "snp.div_", "A01_0", [hub(RUN("H0"), "H0")])
    c.expect("H2", "diff.div_", "A01_0", [])
    # A01_1: input differs from both twins
    c.expect("H2", "snp.div_", "A01_1", [hub(RUN("H2"), "H2", short_div_code="03")])
    c.expect("H2", "diff.div_", "A01_1", [hleg("R", "U", RUN("H2"), short_div_code="03"),
                                         hleg("U", "U", RUN("H1"), short_div_code=NOTNULL)],
             note="exactly one (U,U) (rn2 OLD, its own ctl_validfrom); which twin is in it is the OLD-OLD tie")
    # A01_2: key gone
    c.expect("H2", "snp.div_", "A01_2", [hub(RUN("H1"), "H1")], note="one twin left (rn1 OLD); which short code is the tie")
    c.expect("H2", "diff.div_", "A01_2", [hleg("U", "U", RUN("H1"))])
    # A01_3: input = one of the twins
    c.expect("H2", "snp.div_", "A01_3", [dict(short_div_code="01")],
             note="one row '01' in both branches; its ctl_* (H1 or H2) and the legs (none or (R,U) 01 + (U,U) 02) are the tie")
    c.note("H2", "A01_3 diff.div_: none (rn2 = OLD 01) or (R,U) 01 + (U,U) 02 (rn2 = OLD 02) - OLD-OLD tie, not asserted")
    # A01_4: three variants in the input
    c.expect("H2", "snp.div_", "A01_4", [hub(RUN("H2"), "H2")], note=">= 3 distinct NEW rows: only rn1 survives")
    c.expect("H2", "diff.div_", "A01_4", [hleg("R", "U", RUN("H2")), hleg("U", "U", RUN("H2"))],
             note="(U,U) is the rn2 NEW row (stamped by the load); the third NEW and both OLD twins drop without legs")
    c.note("H2", "A01_5: 4 rows NEW 01/02 + OLD 01/02 - snp 1 or 2 rows @H2, (R,U) [+ (U,U)]: NEW-NEW and OLD-OLD tie (DV-77), "
                 "not asserted; on this stand NEW come in label order (#a 01 first)")
    # H3: second wave over the state each version wrote
    c.expect("H3", "snp.div_", "A01_1", [hub(RUN("H2"), "H2", short_div_code="03")])
    c.expect("H3", "diff.div_", "A01_1", [])
    c.expect("H3", "snp.div_", "A01_2", [hub(RUN("H1"), "H1")])
    c.expect("H3", "diff.div_", "A01_2", [])
    c.expect("H3", "snp.div_", "A01_3", [dict(short_div_code="01")])
    c.expect("H3", "diff.div_", "A01_3", [], note="one stored row equal to the input: EQUALS rn2, no legs")
    c.expect("H3", "snp.div_", "A01_4", [hub(RUN("H3"), "H3")])
    c.expect("H3", "diff.div_", "A01_4", [hleg("R", "U", RUN("H3")), hleg("U", "U", RUN("H3"))],
             note="3 NEW over 1 OLD: the key churns on every INCR in 3.0")
    c.note("H3", "A01_5: again 1 or 2 rows @H3 (tie, not asserted)")
    # H4: every source empty
    for k, row in (("A01_1", hub(RUN("H2"), "H2", short_div_code="03")), ("A01_2", hub(RUN("H1"), "H1")),
                   ("A01_4", hub(RUN("H3"), "H3"))):
        c.expect("H4", "snp.div_", k, [row])
        c.expect("H4", "diff.div_", k, [])
    c.expect("H4", "snp.div_", "A01_3", [dict(short_div_code="01")])
    c.expect("H4", "diff.div_", "A01_3", [])
    c.expect("H4", "snp.div_", "A01_5", [hub(RUN("H3"), "H3")],
             note="1 row left either way: a single row stays OLD, two differing rows -> rn1 kept, rn2 NOTEQUALS dropped")
    c.note("H4", "A01_5 diff.div_: none (1 row after H3) or one (U,U) RUN@H3 (2 rows after H3) - not asserted")


def a02():
    c = case(id="A02", accepted=DV77, accept=["time_zone_rf:A02_3"], chain="hubtwins",
             title="DV-03/DV-77: time_zone_rf twins written by 3.0 (new abbreviation twice at H1), then 4.0's first INCR",
             rules=["H13-key-dedup", "R16", "H06-record-status", "H07-snp-incr", "H08-diff-legs-incr"],
             changes=["NH-undo-keep-validfrom"], tables=["time_zone_rf"], deterministic=False,
             known40="not run. By code: dupKeys -> window30 (" + _HCC + "); A02_1/A02_2 order-independent; A02_3 is the "
                     "C36 steady state, where 4.0 resolves NEW-NEW by POS of the copy in snp (DV-77)",
             summary="Same mechanism as A01 on time_zone_rf. TimeZoneTransformer keeps both rows of one abbreviation with "
                     "different code/name00 (distinct, O/tables/timezone/TimeZoneTransformer.java:30-42); a new "
                     "abbreviation that arrives twice at H1 keeps both rows (" + _WIN + "). H2 (4.0's first INCR): A02_1 "
                     "gets one new row (third name) -> one row @H2, (R,U) new + (U,U) of one twin with RUN@H1 (as H05 on "
                     "the cutover chain, here the INCR runs on 4.0 too); A02_2 is gone -> one twin left with H1 stamps "
                     "+ (U,U). A02_3 keeps both rows in the input (C36 steady state): outcome is the tie, only H4 is "
                     "asserted. A02_0 is a plain control zone.")
    _tz(c, "A02_0", "A02_0", 20, "A02 zero", at=_gone_h4())
    for i, (at_ab, start_c) in {1: ({"H2": GONE}, "H2"), 2: ({"H2": GONE}, None), 3: (_gone_h4(), None)}.items():
        k = "A02_%d" % i
        _tz(c, k + "#a", k, 20 + 10 * i + 1, "A02 %d a" % i, start="H1", at=at_ab)
        _tz(c, k + "#b", k, 20 + 10 * i + 2, "A02 %d b" % i, start="H1", at=at_ab)
        if start_c:
            _tz(c, k + "#c", k, 20 + 10 * i + 3, "A02 %d c" % i, start=start_c, at=_gone_h4())
        c.expect("H1", "snp.time_zone_rf", k, [hub(RUN("H1"), "H1", time_zone_name="A02 %d a" % i, time_zone_code=str(20 + 10 * i + 1)),
                                               hub(RUN("H1"), "H1", time_zone_name="A02 %d b" % i, time_zone_code=str(20 + 10 * i + 2))])
        c.expect("H1", "diff.time_zone_rf", k, _new_twin_legs("H1"))
    c.expect("H0", "snp.time_zone_rf", "A02_0", [hub(RUN("H0"), "H0", time_zone_name="A02 zero")])
    c.expect("H2", "snp.time_zone_rf", "A02_0", [hub(RUN("H0"), "H0", time_zone_name="A02 zero")])
    c.expect("H2", "diff.time_zone_rf", "A02_0", [])
    c1 = hub(RUN("H2"), "H2", time_zone_name="A02 1 c", time_zone_code="33")
    c.expect("H2", "snp.time_zone_rf", "A02_1", [c1])
    c.expect("H2", "diff.time_zone_rf", "A02_1", [hleg("R", "U", RUN("H2"), time_zone_name="A02 1 c"),
                                                 hleg("U", "U", RUN("H1"), time_zone_name=NOTNULL)],
             note="exactly one (U,U); which twin (a or b) is the OLD-OLD tie")
    c.expect("H2", "snp.time_zone_rf", "A02_2", [hub(RUN("H1"), "H1")])
    c.expect("H2", "diff.time_zone_rf", "A02_2", [hleg("U", "U", RUN("H1"))])
    c.note("H2", "A02_3: NEW a/b + OLD a/b - 1 or 2 rows @H2, (R,U) [+ (U,U)] (tie, DV-77); stand: NEW in label order")
    c.expect("H3", "snp.time_zone_rf", "A02_1", [c1])
    c.expect("H3", "diff.time_zone_rf", "A02_1", [])
    c.expect("H3", "snp.time_zone_rf", "A02_2", [hub(RUN("H1"), "H1")])
    c.expect("H3", "diff.time_zone_rf", "A02_2", [])
    c.note("H3", "A02_3: again 1 or 2 rows @H3 (tie, not asserted)")
    c.expect("H4", "snp.time_zone_rf", "A02_1", [c1])
    c.expect("H4", "diff.time_zone_rf", "A02_1", [])
    c.expect("H4", "snp.time_zone_rf", "A02_2", [hub(RUN("H1"), "H1")])
    c.expect("H4", "diff.time_zone_rf", "A02_2", [])
    c.expect("H4", "snp.time_zone_rf", "A02_3", [hub(RUN("H3"), "H3")], note="exactly one row left after the empty INCR")


def a03():
    c = case(id="A03", accepted=DV77, accept=["div_:A03_1"], chain="hubtwins",
             title="DV-01/DV-77 (C37-like): existing div_ key, the input carries it twice from H1 on (3.0 writes the first wave)",
             rules=["H13-key-dedup", "R16", "H06-record-status", "H07-snp-incr"], changes=[], tables=["div_"],
             deterministic=False,
             known40="not run. 4.0 starts at H2 from whatever 3.0 left after H1 (1 or 2 rows); NEW-NEW and OLD-OLD ties "
                     "by POS/IPOS (" + _HCC + "), so 4.0 may hold another branch than 3.0 (DV-77, C37 X1: 19 of 28 keys)",
             summary="H0 (3.0 INIT): A03_1 with short code 01, one snapshot row (an INIT with two rows of one fulldivcode "
                     "is the DV-78 tie of stg.chg_div_h, O/tables/chgdivh/ChgDivHSql.java:177,207,297). H1 (3.0): the "
                     "snapshot has A03_1 with 01 and 02: NEW 01/02 + OLD 01. Order [01,02,OLD01]: {01@H1}, (R,U) 01 + "
                     "(U,U) 02; order [02,01,OLD01]: {02,01}@H1, (R,U) 02 (" + _WIN + ") - on this stand the first one "
                     "(C37 X1 with X0='1', 14 of 14 runs; NEW in source order). H2 (first 4.0 INCR) and H3: same input, "
                     "1 or 2 rows, every survivor stamped by that INCR. H4 (every source empty): exactly one row with H3 "
                     "stamps. C33-like dirt (one fulldivcode, two types, equal short codes) is not a hub case: distinct "
                     "collapses it to one div_ row (O/tables/div/DivTransformer.java:62,88).")
    c.div("A03_1", label="A03_1#a", shortdivcode="01", at=_gone_h4())
    c.div("A03_1", label="A03_1#b", shortdivcode="02", start="H1", at=_gone_h4())
    c.expect("H0", "snp.div_", "A03_1", [hub(RUN("H0"), "H0", short_div_code="01")])
    c.note("H1", "A03_1 (3.0 only): stand branch expected {01@H1}, (R,U) 01 + (U,U) 02; by code also {01,02}@H1, (R,U) 02")
    c.note("H2", "A03_1: 1 or 2 rows @H2 over the 3.0 state (tie, DV-77)")
    c.note("H3", "A03_1: 1 or 2 rows @H3 (tie)")
    c.expect("H4", "snp.div_", "A03_1", [hub(RUN("H3"), "H3")])


def a04():
    c = case(id="A04", accepted=DV77, accept=["div_:A04_1", "time_zone_rf:A04_TZ"], chain="hubtwins",
             title="DV-57: empty INCR (every source empty) over div_ and time_zone_rf twins created at H3",
             rules=["H11-empty-incr", "R19", "H06-record-status", "H07-snp-incr", "H08-diff-legs-incr"],
             changes=["NH-undo-keep-validfrom"], tables=["div_", "time_zone_rf"], deterministic=False,
             known40="not run. Empty input: emptyIncrMarker sees a bk with > 1 row in snp and sends a marker row past the "
                     "vendor's early return (W/compat/CompatHooks.java:57-66), the marker is dropped again in HCC:107 and "
                     "the group goes through window30; expected to match 3.0 (row count, one (U,U)); the full pipeline "
                     "on every empty INCR is the cost side of DV-57 (not measured here)",
             summary="H3 (both versions): A04_1 (short codes 01/02) and zone A04_TZ (codes 81/82) arrive twice as new "
                     "keys -> both rows kept, (R,U)+(U,I)+(U,D) (" + _WIN + "). H4: every source empty; 3.0 still runs "
                     "every hub loader (E/slave/check/EmptyChecker.java:11-16): the window runs over snp alone, rn1 OLD "
                     "(lag NULL) is kept with its H3 stamps, rn2 is NOTEQUALS -> dropped with one (U,U) carrying its own "
                     "ctl_validfrom (E/slave/loading/DataLoaderHubDiff.java:75-80,127-135). Which twin survives is the "
                     "OLD-OLD tie (not checked).")
    c.div("A04_1", label="A04_1#a", start="H3", shortdivcode="01", at=_gone_h4())
    c.div("A04_1", label="A04_1#b", start="H3", shortdivcode="02", at=_gone_h4())
    _tz(c, "A04_TZ#a", "A04_TZ", 81, "A04 a", start="H3", at=_gone_h4())
    _tz(c, "A04_TZ#b", "A04_TZ", 82, "A04 b", start="H3", at=_gone_h4())
    c.expect("H3", "snp.div_", "A04_1", [hub(RUN("H3"), "H3", short_div_code="01"), hub(RUN("H3"), "H3", short_div_code="02")])
    c.expect("H3", "diff.div_", "A04_1", _new_twin_legs("H3"))
    c.expect("H3", "snp.time_zone_rf", "A04_TZ", [hub(RUN("H3"), "H3", time_zone_code="81"), hub(RUN("H3"), "H3", time_zone_code="82")])
    c.expect("H3", "diff.time_zone_rf", "A04_TZ", _new_twin_legs("H3"))
    c.expect("H4", "snp.div_", "A04_1", [hub(RUN("H3"), "H3")], note="3.0 collapses the twins on the empty INCR")
    c.expect("H4", "diff.div_", "A04_1", [hleg("U", "U", RUN("H3"))])
    c.expect("H4", "snp.time_zone_rf", "A04_TZ", [hub(RUN("H3"), "H3")])
    c.expect("H4", "diff.time_zone_rf", "A04_TZ", [hleg("U", "U", RUN("H3"))])


def a05():
    c = case(id="A05", accepted=DV77, accept=["div_:\\N"], chain="hubnull",
             title="DV-07/DV-57/DV-77: div_ NULL-key group from the 3.0 INIT, empty INCR on 4.0, then 20 rows without fulldivcode on every INCR",
             rules=["H13-key-dedup", "H09-init", "H11-empty-incr", "R19", "H06-record-status", "H07-snp-incr",
                    "H08-diff-legs-incr"],
             changes=["NH-undo-keep-validfrom"], tables=["div_"], deterministic=False,
             known40="not run. V1: snp has 2 NULL-pk rows -> emptyIncrMarker (W/compat/CompatHooks.java:57-66), "
                     "HUB_NULL_KEY_30 window (W/compat/hub/HubCompatCalculator.java:112-132); V2-V4: the NULL group goes "
                     "through the same window (counts are order-independent, expected to match). div_h/div_type of the "
                     "NULL key take the PERIOD_NULL_KEY path (DV-81) and may differ; not asserted here",
             summary="V0 (3.0 INIT): one row with fulldivcode NULL and one with '' (same name): the INIT window of chg_div_h "
                     "partitions by fulldivcode (O/tables/chgdivh/ChgDivHSql.java:177,207) -> one row each -> two div_ "
                     "rows with div_sk NULL (sid NULL and ''; stand SK mock: NULL/'' sid -> NULL sk). V1 (every source "
                     "empty, 4.0's first INCR): rn1 OLD kept with V0 stamps; rn2 differs (subbranch_code '0'||NULL = NULL "
                     "vs '0', O/tables/div/DivTransformer.java:42) -> NOTEQUALS -> dropped + (U,U) (as H06, here 4.0 runs "
                     "over the 3.0 rows). V2, V3: 20 snapshot rows without fulldivcode (10 NULL, 10 '', short codes "
                     "11-30, one name, one type) - DEV has ~15.5 thousand stg.chg_div_h rows above its distinct "
                     "fulldivcodes (DV-07), the 3.0 outcome does not depend on the group size once it has >= 3 distinct "
                     "rows: one partition (div_sk NULL), rn1 NEW kept, rn2 NEW NOTEQUALS -> (U,U), the rest drop without "
                     "legs (" + _WIN + "): one row and (R,U)+(U,U) stamped by the load, on EVERY INCR. V4: one row "
                     "without fulldivcode, short code 99 (differs from all stored) -> {99@V4}, (R,U) 99 + (U,U) of the "
                     "V3 survivor. Which row survives V1-V3 is the window tie (not checked).")
    c.owns(None, "div_sk NULL of the A05 rows without fulldivcode (NULL and '')")
    c.owns("", "fulldivcode '' of the A05 rows", table="chg_div_h")
    c.div(None, label="A05_null", fullname="A05 no code", at={"V1": GONE})
    c.div("", label="A05_empty", fullname="A05 no code", at={"V1": GONE})
    for i in range(10):
        c.div(None, label="A05_n%02d" % i, fullname="A05 no code", shortdivcode="%02d" % (11 + i), start="V2", at={"V4": GONE})
        c.div("", label="A05_e%02d" % i, fullname="A05 no code", shortdivcode="%02d" % (21 + i), start="V2", at={"V4": GONE})
    c.div(None, label="A05_last", fullname="A05 no code", shortdivcode="99", start="V4")
    c.expect("V0", "snp.div_", None, [hub(RUN("V0"), "V0", sid=None), hub(RUN("V0"), "V0", sid="")],
             note="3.0 INIT: one NULL-key row per fulldivcode partition (NULL and '')")
    c.expect("V1", "snp.div_", None, [hub(RUN("V0"), "V0")], note="one of the pair left; which sid is the tie")
    c.expect("V1", "diff.div_", None, [hleg("U", "U", RUN("V0"))])
    for ld in ("V2", "V3"):
        c.expect(ld, "snp.div_", None, [hub(RUN(ld), ld)], note="20 NEW rows in the NULL group: only rn1 survives")
        c.expect(ld, "diff.div_", None, [hleg("R", "U", RUN(ld)), hleg("U", "U", RUN(ld))],
                 note="(U,U) is the rn2 NEW row; 18 NEW and the stored row drop without legs")
    c.expect("V4", "snp.div_", None, [hub(RUN("V4"), "V4", short_div_code="99", sid=None)])
    c.expect("V4", "diff.div_", None, [hleg("R", "U", RUN("V4"), short_div_code="99"),
                                       hleg("U", "U", RUN("V3"), short_div_code=NOTNULL)])


def a06():
    c = case(id="A06", chain="hubnull",
             title="DV-07/DV-57: terr_div_type NULL group (divisiontype00 NULL and '') from the 3.0 INIT, empty INCR on 4.0, return",
             rules=["H13-key-dedup", "H09-init", "H11-empty-incr", "R19", "H05-hash", "H06-record-status", "H07-snp-incr"],
             changes=["NH-hash-essa-xxhash-concat"], tables=["terr_div_type"], deterministic=False,
             known40="not run. V1: marker path (W/compat/CompatHooks.java:57-66) with hash-equal NULL rows; window30 keeps "
                     "OLD rn1 + EQUALS rn2 (H3.arb A3); expected to match",
             summary="V0 (3.0 INIT): A06_1 with divisiontype00 NULL and A06_2 with '' -> two terr_div_type rows with NULL sk "
                     "(distinct keeps both, O/tables/terrdivtype/TerrDivTypeTransformer.java:31-47; INIT writes every row, "
                     "E/slave/loading/DataLoaderHubDiff.java:105). Their 3.0 hashes are equal (NULL -> '', "
                     "E/slave/loading/SnpDataLoader.java:120-133). V1 (every source empty): rn1 OLD (lag NULL) kept, rn2 "
                     "EQUALS kept -> both rows stay with V0 stamps, no legs. V2: both types back: NEW, NEW, OLD, OLD all "
                     "equal -> only EQUALS rn2 (a NEW row) survives, re-stamped by V2, no legs (as C35 X1); V3, V4 the same "
                     "with one stored row. Which sid survives is the tie (not checked). A05's type A05_T gives the SK init "
                     "a non-empty sid.")
    c.owns(None, "NULL terr_div_type_sk of A06_1/A06_2 (divisiontype00 NULL and '')", table="terr_div_type")
    c.div("A06_1", divisiontype00=None, at={"V1": GONE, "V2": {}})
    c.div("A06_2", divisiontype00="", at={"V1": GONE, "V2": {}})
    c.expect("V0", "snp.terr_div_type", None, [hub(RUN("V0"), "V0"), hub(RUN("V0"), "V0")])
    c.expect("V1", "snp.terr_div_type", None, [hub(RUN("V0"), "V0"), hub(RUN("V0"), "V0")],
             note="hash-equal OLD pair on an empty INCR: OLD rn1 + EQUALS rn2 both stay")
    c.expect("V1", "diff.terr_div_type", None, [])
    for ld in ("V2", "V3", "V4"):
        c.expect(ld, "snp.terr_div_type", None, [hub(RUN(ld), ld)], note="one row, re-stamped by the load, no legs")
        c.expect(ld, "diff.terr_div_type", None, [])


def a07():
    c = case(id="A07", chain="hubnull",
             title="DV-03/DV-57: time_zone_rf twins from the 3.0 INIT, empty INCR on 4.0, then one new row for the zone",
             rules=["H13-key-dedup", "H09-init", "H11-empty-incr", "R19", "H06-record-status", "H07-snp-incr",
                    "H08-diff-legs-incr"],
             changes=["NH-undo-keep-validfrom"], tables=["time_zone_rf"], deterministic=False,
             known40="not run. V1: marker + window30 (W/compat/CompatHooks.java:57-66, " + _HCC + "); V2 the stored "
                     "row is single, the vendor path applies; expected to match",
             summary="V0 (3.0 INIT): two rfzonetime rows with abbreviation A07_TZ, codes 71/72 -> both written (distinct, "
                     "O/tables/timezone/TimeZoneTransformer.java:30-42; INIT E/slave/loading/DataLoaderHubDiff.java:105): "
                     "the C36 duplicate PK, made by 3.0. V1 (every source empty, 4.0's first INCR): rn1 OLD kept with V0 "
                     "stamps, rn2 NOTEQUALS -> (U,U). V2: one row 'A07 C' (code 73) -> NOTEQUALS rn1 kept, (R,U) C + "
                     "(U,U) of the stored row with RUN@V0. V3: unchanged -> EQUALS, no legs.")
    _tz(c, "A07_a", "A07_TZ", 71, "A07 A", at={"V1": GONE})
    _tz(c, "A07_b", "A07_TZ", 72, "A07 B", at={"V1": GONE})
    _tz(c, "A07_c", "A07_TZ", 73, "A07 C", start="V2")
    c.expect("V0", "snp.time_zone_rf", "A07_TZ", [hub(RUN("V0"), "V0", time_zone_name="A07 A", time_zone_code="71"),
                                                  hub(RUN("V0"), "V0", time_zone_name="A07 B", time_zone_code="72")])
    c.expect("V1", "snp.time_zone_rf", "A07_TZ", [hub(RUN("V0"), "V0")], note="which twin survives is the tie")
    c.expect("V1", "diff.time_zone_rf", "A07_TZ", [hleg("U", "U", RUN("V0"))])
    cc = hub(RUN("V2"), "V2", time_zone_name="A07 C", time_zone_code="73")
    c.expect("V2", "snp.time_zone_rf", "A07_TZ", [cc])
    c.expect("V2", "diff.time_zone_rf", "A07_TZ", [hleg("R", "U", RUN("V2"), time_zone_name="A07 C"),
                                                  hleg("U", "U", RUN("V0"), time_zone_name=NOTNULL)])
    c.expect("V3", "snp.time_zone_rf", "A07_TZ", [cc])
    c.expect("V3", "diff.time_zone_rf", "A07_TZ", [])


for _f in [a01, a02, a03, a04, a05, a06, a07]:
    _f()
