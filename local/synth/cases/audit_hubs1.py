"""Audit 1.3, theme hubs1 (SVOD DV-01 DV-02 DV-03 DV-07 DV-08 DV-12 DV-13 DV-18 DV-19 DV-20). Case letter H.

Data scenarios here: DV-01 (H01-H04), DV-03 (H05), DV-07 (H06), DV-08 (H07), DV-13 (H08), DV-20 (H09).
Not expressible on this stand (see the phase 1.3 report, per_dv): DV-02 (real SK registry / CTL workflow name),
DV-12 (SK answer type / DDL type drift), DV-18 (failure between snp and diff publication), DV-19 (metastore object of
stg.chg_div_h, dropped table). Chains nullgrp, degen, emptynull, nonmono, srctype are new (coordinator adds them to
CHAINS; labels A, W, U, Y, T).

Paths: E/ = essa-common-lib 2.2.6 (.../essa/common/), O/ = 3.0 vspc_idl_old (.../dffess/vspc/), W/ = 4.0.
"""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case, DV77


def h01():
    c = case(id="H01", accepted=DV77, accept=["div_:H01_1"], title="DV-01: a NEW fulldivcode arrives twice in one INCR batch with different short codes (div_ hub)",
             rules=["H13-key-dedup", "R16", "H07-snp-incr", "H08-diff-legs-incr"], changes=[], tables=["div_"],
             known40="phase 3 (j0164-j0205): no dedupByKey, the 3.0 hub window (DV-01); same as 3.0 up to the window tie (L7 diff.div_ 01/02, DV-77)",
             summary="DV-01. INCR chg_div_h is the raw snapshot with distinct only (O/tables/chgdivh/ChgDivHSql.java:12-87, "
                     "ChgDivHTransformer.java:24), DivTransformer only distinct (O/tables/div/DivTransformer.java:62,88): two "
                     "div_ rows with one div_sk. Hub window partitionBy(pk) orderBy(desc(ctl_loading), asc(record_status)) "
                     "(E/slave/loading/SnpDataLoader.java:181-204): both NEW, same L -> tie. rn1: lead = rn2 hash, differs -> "
                     "NOTEQUALS rn1 -> kept, (R,U); rn2: lead NULL -> stays NEW -> kept, (U,I) (ctl_do U because rn=2) + (U,D) "
                     "(E/slave/loading/DataLoaderHubDiff.java:75-80,127-144). Both rows stay in snp whatever the order, so the "
                     "projection (both short codes in snp; legs without values) is deterministic; which short code carries "
                     "(R,U) is not checked. The key arrives at L7 (last load of main) so no later INCR re-reads the twin rows "
                     "(that outcome depends on the window order). div_h / div_type get one row (short code is not in them).")
    c.div("H01_1", label="H01_1#a", start="L7", shortdivcode="01")
    c.div("H01_1", label="H01_1#b", start="L7", shortdivcode="02")
    c.expect("L7", "snp.div_", "H01_1", [hub(RUN("L7"), "L7", short_div_code="01"), hub(RUN("L7"), "L7", short_div_code="02")],
             note="3.0 keeps BOTH rows with one div_sk (no dedup); 4.0 one")
    c.expect("L7", "diff.div_", "H01_1", [hleg("R", "U", RUN("L7")), hleg("U", "I", RUN("L7")), hleg("U", "D", RUN("L7"))],
             note="rn1 NOTEQUALS -> (R,U); rn2 NEW -> (U,I)+(U,D); which short code is in which leg is a window tie")


def h02():
    c = case(id="H02", accepted=DV77, accept=["div_type:H02_1"], chain="keys", title="DV-01: one fulldivcode with divisiontype00 NULL and 'H02_T' at INIT (div_type)",
             rules=["H13-key-dedup", "R16", "H09-init"], changes=[], tables=["div_type"], deterministic=False,
             known40="expected by code, not run yet: row_number over partitionBy(div_sid) orderBy(type asc) = NULLS FIRST "
                     "(W/transformations/DivTypeTransformation.java:62-67) keeps the NULL type: one row, type NULL",
             summary="DV-01 (div_type, {NULL,'X'}). DivTypeTransformer selects fulldivcode + divisiontype00 with distinct only "
                     "(O/tables/divtype/DivTypeTransformer.java:33-42); INIT loader writes every input row "
                     "(E/slave/loading/DataLoaderHistSnp.java:126): two [1900, 9999] rows with one PK, type NULL and H02_T. "
                     "Differs from C33_3 (two non-empty types, accepted): here 4.0 keeps the NULL type and loses the real one. "
                     "X1/X2: 3.0 re-reads two open versions plus two input rows - window order decides, no expectation.")
    c.div("H02_1", label="H02_1#null", divisiontype00=None)
    c.div("H02_1", label="H02_1#t", divisiontype00="H02_T")
    c.expect("X0", "snp.div_type", "H02_1", [ver(MIN, MAX, RUN("X0"), "X0", div_type_terr_div_type_sk=None),
                                            ver(MIN, MAX, RUN("X0"), "X0", div_type_terr_div_type_sk="H02_T")],
             note="3.0 INIT: both types; 4.0: one row with the NULL type")
    c.expect("X0", "snp.terr_div_type", "H02_T", [hub(RUN("X0"), "X0", div_type_code="H02_T")])
    for ld in ("X1", "X2"):
        c.note(ld, "H02_1 div_type: two open versions + two input rows, 3.0 outcome depends on the window order (not checked)")


def h03():
    c = case(id="H03", chain="nullgrp", title="DV-01: two divisions without fulldivcode, types H03_A / H03_B, at INIT (div_type NULL group)",
             rules=["H13-key-dedup", "H09-init", "R04"], changes=[], tables=["div_type", "div_", "div_h"],
             known40="expected by code, not run yet: partitionBy(div_sid) puts both NULL fulldivcode rows in one group, rn=1 keeps "
                     "H03_A (W/transformations/DivTypeTransformation.java:62-67): one NULL-key div_type row",
             summary="DV-01 (div_type, NULL group). Both divisions have fulldivcode NULL and the same name; they differ only in "
                     "divisiontype00, which chg_div_h does not read, so chg_div_h / div_ / div_h get one NULL-key row in both "
                     "versions (UNION distinct, O/tables/chgdivh/ChgDivHSql.java:252). div_type: distinct keeps (NULL, H03_A) and "
                     "(NULL, H03_B) (O/tables/divtype/DivTypeTransformer.java:33-42), SK map gives div_sk NULL to both, INIT writes "
                     "both (E/slave/loading/DataLoaderHistSnp.java:126). H03_0 is an ordinary division so that the SK init of div "
                     "sees a non-empty sid (stand SK mock: no non-empty sid -> sk 0). Single INIT load: the next INCR would be a "
                     "window tie in 3.0.")
    c.div("H03_0")
    c.div(None, label="H03_a", fullname="H03 no code", divisiontype00="H03_A")
    c.div(None, label="H03_b", fullname="H03 no code", divisiontype00="H03_B")
    c.owns(None, "NULL fulldivcode of H03_a / H03_b")
    c.expect("A0", "snp.div_type", None, [ver(MIN, MAX, RUN("A0"), "A0", div_type_terr_div_type_sk="H03_A"),
                                          ver(MIN, MAX, RUN("A0"), "A0", div_type_terr_div_type_sk="H03_B")],
             note="3.0: two NULL-key rows; 4.0: one (H03_A)")
    c.expect("A0", "snp.div_", None, [hub(RUN("A0"), "A0", code=None)])
    c.expect("A0", "snp.div_h", None, [ver(d("A0"), MAX, START, "A0", div_name="H03 no code")])


def h04():
    c = case(id="H04", chain="degen", title="DV-01: INIT where every divisiontype00 is NULL or '' (terr_div_type SK init without a key -> sk 0)",
             rules=["H13-key-dedup", "H09-init"], changes=[], tables=["terr_div_type"],
             known40="expected by code, not run yet: both rows get terr_div_type_sk 0 from the same mock, dedupByKey(sk) keeps one "
                     "non-NULL key 0 (W/transformations/TerrDivTypeTransformation.java:40, W/common/BaseTransformation.java:126-132)",
             summary="DV-01 (degenerate INIT). TerrDivTypeTransformer: distinct over (sid, code, sk) keeps NULL and '' "
                     "(O/tables/terrdivtype/TerrDivTypeTransformer.java:31-47); INIT -> SK init "
                     "(E/slave/templates/SkClientWrapper.java:103-110, 128-140); with no non-empty sid in the batch the SK "
                     "client gives sk 0 to every row (stand mock L/java/common/ru/sberbank/bigdata/sl/sk/client/SkClient.java:"
                     "66-71, reproduced from sl-sk-client); INIT hub loader writes both (E/slave/loading/DataLoaderHubDiff.java:105). "
                     "3.0: two terr_div_type rows with terr_div_type_sk = 0 (sid NULL and ''); 4.0: one. No row expectation: the "
                     "comparator maps sk 0 to whichever sid it saw last, so the dump key (NULL or '') is not stable; the "
                     "difference shows as rows30=2 / rows40=1 in terr_div_type. div_type keeps NULL type sk (SK map) in both.")
    c.div("H04_1", divisiontype00=None)
    c.div("H04_2", divisiontype00="")
    c.owns(None, "terr_div_type row of divisiontype00 NULL (sk 0)", table="terr_div_type")
    c.owns("", "terr_div_type row of divisiontype00 '' (sk 0)", table="terr_div_type")
    c.expect("W0", "snp.div_type", "H04_1", [ver(MIN, MAX, RUN("W0"), "W0", div_type_terr_div_type_sk=None)])
    c.expect("W0", "snp.div_", "H04_2", [hub(RUN("W0"), "W0")])
    c.note("W0", "terr_div_type: 3.0 two rows with terr_div_type_sk=0 (sid NULL and ''), 4.0 one (key not checked)")


def h05():
    c = case(id="H05", chain="cutover", title="DV-03: time_zone_rf duplicate PK left by the 3.0 INIT; first INCR changes the zone",
             rules=["H13-key-dedup", "H07-snp-incr", "H08-diff-legs-incr"], changes=["NH-undo-keep-validfrom"],
             tables=["time_zone_rf"],
             known40="expected by code, not run yet (SVOD DV-03): the vendor joins snp x incr by pk without dedup "
                     "(V/strategy/process/changes/AbstractChangesCalculator.java:138-171), one (R,U) per snp row and (U,U) "
                     "multiplied by the compat undo join (W/compat/hub/HubCompatCalculator.java:123-127,145-147); both stored "
                     "rows may stay in snp",
             summary="DV-03. G0 (3.0 INIT): two rfzonetime rows with abbreviation H05_TZ and different code/name; distinct "
                     "keeps both (O/tables/timezone/TimeZoneTransformer.java:30-42), INIT writes both (E/slave/loading/"
                     "DataLoaderHubDiff.java:105): duplicate PK in snp. G1: one row H05_TZ with a NEW name. 3.0 window "
                     "(E/slave/loading/SnpDataLoader.java:181-204): NEW (G1) rn1, OLD rows rn2/rn3. rn1: lead = an OLD hash, "
                     "differs -> NOTEQUALS rn1 -> kept, (R,U); rn2: lag = NEW hash, differs -> NOTEQUALS rn2 -> dropped, (U,U) with "
                     "its own ctl_validfrom; rn3: lag = rn2 hash, the two OLD rows differ -> NOTEQUALS rn3 -> dropped, no leg "
                     "(DataLoaderHubDiff.java:75-80,127-144). Result: one row, one (R,U), one (U,U), whatever the OLD order "
                     "(which old name is in (U,U) is not checked).")
    c.tz("H05_a", code=51, abbreviation="H05_TZ", name00="H05 A", timedesignation="0", utcformat="+3", at={"G1": GONE})
    c.tz("H05_b", code=52, abbreviation="H05_TZ", name00="H05 B", timedesignation="0", utcformat="+3",
         at={"G1": dict(name00="H05 C")})
    c.expect("G0", "snp.time_zone_rf", "H05_TZ", [hub(RUN("G0"), "G0", time_zone_name="H05 A", time_zone_code="51"),
                                                  hub(RUN("G0"), "G0", time_zone_name="H05 B", time_zone_code="52")])
    c.expect("G1", "snp.time_zone_rf", "H05_TZ", [hub(RUN("G1"), "G1", time_zone_name="H05 C", time_zone_code="52")],
             note="3.0 collapses the duplicate PK to the one new row")
    c.expect("G1", "diff.time_zone_rf", "H05_TZ", [hleg("R", "U", RUN("G1"), time_zone_name="H05 C"),
                                                   hleg("U", "U", RUN("G0"), time_zone_name=NOTNULL)],
             note="exactly one (U,U): the rn3 OLD row is dropped without a leg")


def h06():
    c = case(id="H06", chain="emptynull", title="DV-07: INCR with empty sfsdivision over a NULL-key group (fulldivcode NULL and '')",
             rules=["H11-empty-incr", "R19", "H13-key-dedup"], changes=["HPWR-vanish-divtype"], tables=["div_", "div_type"],
             deterministic=False,
             known40="expected by code, not run yet: on empty input the vendor returns before calculateDiff "
                     "(V/PublisherImpl.java:230-235), the compat NULL-group window (W/compat/hub/HubCompatCalculator.java:98-114) "
                     "does not run: div_ keeps both NULL-key rows, no legs; div_type VANISH closes both rows at today-1",
             summary="DV-07. U0 INIT: fulldivcode NULL and '' fall into different partitions of the chg_div_h window "
                     "(O/tables/chgdivh/ChgDivHSql.java:177,207) -> two div_ rows and two div_type rows with div_sk NULL in both "
                     "versions (4.0 dedupByKey leaves NULL keys). U1: sfsdivision empty. 3.0 still runs every loader "
                     "(E/slave/check/EmptyChecker.java:13-16): the window runs over snp alone (E/slave/loading/"
                     "SnpDataLoader.java:181-204). div_: rn1 OLD (lag NULL) kept; rn2 lag differs (subbranch_code '0'||NULL = NULL "
                     "vs '0', O/tables/div/DivTransformer.java:42) -> NOTEQUALS rn2 -> dropped + (U,U) with its own "
                     "ctl_validfrom (E/slave/loading/DataLoaderHubDiff.java:75-80,127-135). div_type: the two NULL rows hash "
                     "alike (no sid column, same type): rn1 OLD -> closed today-1 with (R,U)+(U,U); rn2 EQUALS -> stays open with "
                     "its INIT ctl (E/slave/loading/DataLoaderHistSnp.java:82-86,103-108,119,139-153). Which sid survives is a "
                     "window tie (not checked). H06_1 is an ordinary key so the SK init sees a non-empty sid. div_h of the "
                     "NULL group is not checked.")
    c.div("H06_1", at={"U1": GONE})
    c.div(None, label="H06_null", fullname="H06 no code", at={"U1": GONE})
    c.div("", label="H06_empty", fullname="H06 no code", at={"U1": GONE})
    c.owns(None, "div_sk NULL of H06_null / H06_empty (fulldivcode NULL and '')")
    c.owns("", "fulldivcode '' of H06_empty", table="chg_div_h")
    c.expect("U0", "snp.div_", None, [hub(RUN("U0"), "U0"), hub(RUN("U0"), "U0")])
    c.expect("U0", "snp.div_type", None, [ver(MIN, MAX, RUN("U0"), "U0"), ver(MIN, MAX, RUN("U0"), "U0")])
    c.expect("U1", "snp.div_", None, [hub(RUN("U0"), "U0")], note="3.0: one NULL-key row left; 4.0: two")
    c.expect("U1", "diff.div_", None, [hleg("U", "U", RUN("U0"))], note="3.0: (U,U) of the dropped row; 4.0: no legs")
    c.expect("U1", "snp.div_type", None, [ver(MIN, d("U1", -1), RUN("U1"), "U1"), ver(MIN, MAX, RUN("U0"), "U0")],
             note="3.0: rn1 closed today-1, rn2 (EQUALS) stays open; 4.0: both closed today-1")
    c.expect("U1", "diff.div_type", None, [leg("R", "U", MIN, d("U1", -1), RUN("U1")), leg("U", "U", MIN, MAX, RUN("U1"))])
    c.expect("U1", "snp.div_type", "H06_1", [ver(MIN, d("U1", -1), RUN("U1"), "U1")])
    c.expect("U1", "snp.div_", "H06_1", [hub(RUN("U0"), "U0")])
    c.expect("U1", "diff.div_", "H06_1", [])


def h07():
    c = case(id="H07", chain="rerun", title="DV-08: INCR rerun with the same loadingId after the source changed (div_ hub)",
             rules=["H07-snp-incr", "H08-diff-legs-incr", "R07"], changes=["NH-undo-keep-validfrom"], tables=["div_"],
             known40="phase 3 (j0164-j0205): no publish guard; the R3 rerun publishes like 3.0 (H07_1 gets '02' at R3), same as 3.0 (DV-08)",
             summary="DV-08 (changed input). H07_1 is unchanged until R2 and gets short code '02' only at R3 (= R2 rerun, same "
                     "loadingId 6001). H07_2 is new at R2, so diff.div_ holds ctl_loading=6001 before R3 in both versions. "
                     "3.0 has no replay guard: R3 runs the full ETL (recovery only for loads not in SUCCESS, "
                     "E/master/app/recover/DataProductRecoveryImpl.java:33-38). H07_1: NEW (6001) rn1 vs OLD (6000) rn2, hashes "
                     "differ -> (R,U) new + (U,U) old with RUN@R1 (INIT rerun stamp) (E/slave/loading/SnpDataLoader.java:181-204, "
                     "DataLoaderHubDiff.java:75-80,127-135); the legs are appended to diff ctl_loading=6001 (E/slave/move/"
                     "ClassicMovingGenerator.java:19-25). H07_2: NEW and OLD both 6001, asc(record_status) puts NEW first, "
                     "equal hash -> OLD EQUALS rn2 kept, no hub leg (period tables of H07_2 get the R01-style no-op pairs, "
                     "not checked here). ctl_loading 6001 is shown as R2.")
    c.div("H07_1", at={"R3": dict(shortdivcode="02")})
    c.div("H07_2", start="R2")
    c.expect("R2", "snp.div_", "H07_1", [hub(RUN("R1"), "R0", short_div_code="01")])
    c.expect("R2", "diff.div_", "H07_2", [hleg("R", "I", RUN("R2")), hleg("U", "D", RUN("R2"))])
    c.expect("R3", "snp.div_", "H07_1", [hub(RUN("R3"), "R2", short_div_code="02")],
             note="3.0 publishes the change in the rerun; 4.0 skips (guard)")
    c.expect("R3", "diff.div_", "H07_1", [hleg("R", "U", RUN("R3"), short_div_code="02"),
                                         hleg("U", "U", RUN("R1"), short_div_code="01")])
    c.expect("R3", "snp.div_", "H07_2", [hub(RUN("R2"), "R2")])
    c.expect("R3", "diff.div_", "H07_2", [hleg("R", "I", RUN("R2")), hleg("U", "D", RUN("R2"))],
             note="only the R2 legs: equal input on rerun gives no hub leg")


def h08():
    c = case(id="H08", chain="nonmono", title="DV-13: INCR with a loadingId LOWER than the one stored in snp (non-monotonic loadingId)",
             rules=["H07-snp-incr", "H08-diff-legs-incr", "R13"], changes=[], tables=["div_", "div_type"],
             known40="expected by code, not run yet: 4.0 compares by pk and hash without ctl_loading "
                     "(V/strategy/process/changes/AbstractChangesCalculator.java:138-171): H08_1 (R,U)+(U,U) and one row, "
                     "H08_2 nothing",
             summary="DV-13. Y0 INIT with loadingId 9405, Y1 INCR with loadingId 9400. 3.0 window orders desc(ctl_loading) "
                     "(E/slave/loading/SnpDataLoader.java:181): the stored OLD row (9405) is rn1, lag NULL -> stays OLD -> kept; "
                     "the NEW row (9400) is rn2, lead NULL -> stays NEW -> kept with (U,I)+(U,D) (E/slave/loading/"
                     "DataLoaderHubDiff.java:75-80,127-144). Every hub key, changed (H08_1) or not (H08_2), ends with two rows "
                     "with one PK. div_type (E/slave/loading/DataLoaderHistSnp.java:82-122,139-153): OLD rn1 closed today-1, "
                     "NEW rn2 opens at 1900 (no cache after INIT): two rows with PK (div_sk, 1900) and four legs. "
                     "div_h / terr_div_type are affected the same way and not checked.")
    c.div("H08_1", at={"Y1": dict(shortdivcode="02")})
    c.div("H08_2")
    c.expect("Y0", "snp.div_", "H08_1", [hub(RUN("Y0"), "Y0", short_div_code="01")])
    c.expect("Y1", "snp.div_", "H08_1", [hub(RUN("Y0"), "Y0", short_div_code="01"), hub(RUN("Y1"), "Y1", short_div_code="02")],
             note="3.0 keeps the stored row (higher ctl_loading) AND the new one")
    c.expect("Y1", "diff.div_", "H08_1", [hleg("U", "I", RUN("Y1"), short_div_code="02"), hleg("U", "D", RUN("Y1"), short_div_code="02")])
    c.expect("Y1", "snp.div_", "H08_2", [hub(RUN("Y0"), "Y0"), hub(RUN("Y1"), "Y1")], note="unchanged key duplicated as well")
    c.expect("Y1", "diff.div_", "H08_2", [hleg("U", "I", RUN("Y1")), hleg("U", "D", RUN("Y1"))])
    c.expect("Y1", "snp.div_type", "H08_2", [ver(MIN, d("Y1", -1), RUN("Y1"), "Y1"), ver(MIN, MAX, RUN("Y1"), "Y1")])
    c.expect("Y1", "diff.div_type", "H08_2", [leg("R", "I", MIN, MAX, RUN("Y1")), leg("U", "D", MIN, MAX, RUN("Y1")),
                                             leg("R", "U", MIN, d("Y1", -1), RUN("Y1")), leg("U", "U", MIN, MAX, RUN("Y1"))])


def h09():
    c = case(id="H09", chain="srctype", title="DV-20: d_ sfsdivision.ctl_validfrom is STRING at INIT (stg.chg_div_h start_dt DATE)",
             rules=["S2-chgdivh-init", "H09-init"], changes=[], tables=["div_h"],
             known40="expected by code, not run yet: castDataset with Column.cast (W/transformations/ChgDivHTransformation.java:33-35) "
                     "turns the ISO string into a date, load T0 SUCCESS with the same rows 3.0 would write for a TIMESTAMP source",
             summary="DV-20. The chain declares d_sfs.ctl_validfrom STRING. INIT SQL: 'ctl_validfrom AS start_dt' UNION "
                     "'CURRENT_DATE() AS start_dt' (O/tables/chgdivh/ChgDivHSql.java:245,252,288) widens start_dt to STRING; "
                     "ChgDivHTransformer matches columns by name only (E/slave/app/SparkWrapper.java:104-114) and writes with "
                     "insertInto (O/tables/chgdivh/ChgDivHTransformer.java:30-32, E/slave/app/HiveWriter.java:10) into "
                     "stg.chg_div_h.start_dt DATE: store assignment ANSI (Spark default, not overridden by the stand or ESSA) does "
                     "not allow string -> date -> AnalysisException, the 3.0 load T0 FAILS (status FAILED T0, no 3.0 targets). "
                     "A difference = 4.0 T0 OK. No row expectation: 3.0 truth is the failure; if 3.0 does not fail, div_h of "
                     "H09_1 would be [2026-05-01, D-1] + [D, 9999] in both.")
    r = c.div("H09_1", fullname="H09 now")
    c.hist(r, "2026-05-01 10:00:00", 1, fullname="H09 hist")
    c.note("T0", "3.0: load FAILED in chg_div_h (string -> date under ANSI store assignment); 4.0: SUCCESS")


for _f in [h01, h02, h03, h04, h05, h06, h07, h08, h09]:
    _f()
