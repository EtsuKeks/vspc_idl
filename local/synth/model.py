"""Synthetic-data model: timeline, source DDL, case DSL, SQL rendering.

Nothing here knows about individual cases; cases live in cases.py.
Every value that reaches a source table is set by a case through this DSL, so a source row
can always be traced back to the case (and the case id is part of every natural key).
"""
import datetime as _dt

# --------------------------------------------------------------------------------------------
# Timeline. One list of loads per chain. Business date of a load = date part of its fake time.
# loadingId is passed to both versions unchanged, so ctl_loading can be compared exactly.
# --------------------------------------------------------------------------------------------


class Load:
    def __init__(self, label, mode, faketime, loading_id, why, by="both"):
        self.label, self.mode, self.faketime, self.loading_id, self.why = label, mode, faketime, loading_id, why
        self.by = by   # "30": run by 3.0 only; the 4.0 half of the chain starts from the 3.0 state after it (cutover)
        self.date = _dt.date.fromisoformat(faketime[:10])


class Chain(list):
    """A chain of loads run in order on one stand root per version. `modes` = run modes it covers (CHAINS.md)."""

    def __init__(self, purpose, modes, loads, types=None):
        super().__init__(loads)
        self.purpose, self.modes = purpose, modes
        self.types = types or {}   # source column type overrides for this chain: {"m_sfs.shortdivcode": "int"}


def _days(prefix, first_id, spec):
    """[(mode, 'yyyy-mm-dd HH:MM', why[, by])] -> Loads prefix0.. with consecutive loadingIds."""
    return [Load("%s%d" % (prefix, i), x[0], x[1] + ":00", first_id + i, x[2], *x[3:]) for i, x in enumerate(spec)]


CHAINS = {
    "main": Chain("The broad catalog: every case unless it names another chain; calendar corner cases.",
                  "INIT; INCR next day; 2nd INCR on the same day; INCR after a gap > max_increment_depth_days; "
                  "1st/2nd/15th of month; month boundaries", [
        Load("L0", "INIT", "2027-01-13 10:00:00", 1000, "INIT. d_ history is read only here (R10)."),
        Load("L1", "INCR", "2027-01-14 10:00:00", 1001, "Day after INIT: an INIT version that started today-1 cannot be closed at D-2 (S2)."),
        Load("L2", "INCR", "2027-01-14 16:00:00", 1002, "Second INCR on the same calendar day (S1, R15)."),
        Load("L3", "INCR", "2027-01-15 10:00:00", 1003, "Consecutive day; most 'normal' changes happen here."),
        Load("L4", "INCR", "2027-02-01 10:00:00", 1004, "1st of month, gap 17 days > max_increment_depth_days=10 across Jan->Feb (S6, M1/M2)."),
        Load("L5", "INCR", "2027-02-02 10:00:00", 1005, "2nd of month: closures at D-2 land in the previous month partition 202701."),
        Load("L6", "INCR", "2027-02-15 10:00:00", 1006, "15th of month, gap 13 days > 10."),
        Load("L7", "INCR", "2027-03-02 10:00:00", 1007, "Gap 15 days > 10 across Feb->Mar; partition 202701 is outside the rewrite zone now."),
    ]),
    "samepk": Chain("An incoming version gets the SAME (key, start_dt) as a stored row (day after INIT, 2nd INCR the same day).",
                    "INIT -> INCR next day -> INCR same day", [
        Load("K0", "INIT", "2027-01-13 10:00:00", 4000, "INIT."),
        Load("K1", "INCR", "2027-01-14 10:00:00", 4001, "Day after INIT: new version start D-1 = start of the INIT version (S2)."),
        Load("K2", "INCR", "2027-01-14 16:00:00", 4002, "Second INCR on the same day: new version start D-1 = start of the K1 version (S1)."),
    ]),
    "sameday": Chain("INIT and two INCRs on the SAME calendar day: D-1 and D-2 lie before the INIT date.",
                     "INIT -> INCR same day -> INCR same day", _days("D", 9700, [
        ("INIT", "2027-01-13 09:00", "INIT."),
        ("INCR", "2027-01-13 13:00", "INCR on the INIT day: new versions start D-1 = the day before INIT."),
        ("INCR", "2027-01-13 17:00", "Second INCR on the same day: the D1 versions started D-1 and are relabelled."),
    ])),
    "keys": Chain("Data anomalies: duplicate natural keys, NULL and '' keys, key collisions in div_div_relation, source type drift.",
                  "INIT with anomalies -> INCR on the same / slightly changed snapshot (x2)", _days("X", 3000, [
        ("INIT", "2027-01-13 10:00", "INIT with duplicates, NULL / '' keys, shortdivcode INT in the source."),
        ("INCR", "2027-01-14 10:00", "INCR: same snapshot except the changes the cases name; new successor collisions."),
        ("INCR", "2027-01-15 10:00", "INCR: same snapshot again (stability of NULL keys and collisions)."),
    ]), types={"m_sfs.shortdivcode": "int", "tz.timedesignation": "int"}),
    "nullkeys": Chain("NULL fulldivcode key in the PERIOD tables: a NULL-key division whose name and type change, then vanishes. "
                      "Kept apart: a NULL-key PERIOD change stops a 4.0 load (guard, fix in progress) and would hide the rest of a chain.",
                      "INIT with a NULL key -> INCR changing it -> INCR removing it", _days("N", 9800, [
        ("INIT", "2027-01-13 10:00", "INIT with a division without fulldivcode."),
        ("INCR", "2027-01-15 10:00", "Its name and type change."),
        ("INCR", "2027-01-16 10:00", "It disappears."),
    ])),
    "empty": Chain("INCR with an EMPTY sfsdivision snapshot (S5, D-06), then with every source empty, then the sources return.",
                   "INCR with empty m_ (others full); INCR with all sources empty; INCR after empty INCRs (returners)", _days("E", 2000, [
        ("INIT", "2027-01-13 10:00", "INIT with data in every source."),
        ("INCR", "2027-01-14 10:00", "INCR: sfsdivision snapshot empty, every other source unchanged."),
        ("INCR", "2027-01-15 10:00", "INCR: every source empty."),
        ("INCR", "2027-01-16 10:00", "INCR: the sources of E0 return."),
    ])),
    "emptysrc": Chain("INCR with one non-sfsdivision source empty at a time (each PU input / each reference table).",
                      "INCR with empty rfzonetime / successors; INCR with empty gar_fias addr, okato2, oktmo2, pay_sys_prtcpnt",
                      _days("S", 5000, [
        ("INIT", "2027-01-13 10:00", "INIT with data in every source."),
        ("INCR", "2027-01-14 10:00", "rfzonetime and successors empty (inputs of time_zone_rf and div_div_relation)."),
        ("INCR", "2027-01-15 10:00", "rfzonetime/successors back; gar_fias addr, okato2, oktmo2, pay_sys_prtcpnt empty."),
    ])),
    "cold": Chain("Loads onto EMPTY targets and INIT with empty input (D-05, B8/B9, C5/C6 of the audit).",
                  "INCR on empty targets with empty input; INCR on empty targets with data (no INIT); "
                  "INIT with all sources empty over data; INIT with all sources empty on empty targets", _days("Z", 7000, [
        ("INCR", "2027-01-13 10:00", "INCR, all sources empty, targets empty (never initialised)."),
        ("INCR", "2027-01-14 10:00", "INCR with data onto still-empty targets: everything is NEW."),
        ("INIT", "2027-01-15 10:00", "INIT with every source empty over loaded targets: everything is emptied."),
        ("INIT", "2027-01-16 10:00", "INIT with every source empty on empty targets."),
    ])),
    "emptyinit": Chain("INIT over existing data with some sources empty.",
                       "INIT over data; INIT with empty rfzonetime + successors; INIT with empty m_ but non-empty d_ history; INIT with empty m_ + d_",
                       _days("I", 8000, [
        ("INIT", "2027-01-13 10:00", "INIT with data in every source."),
        ("INIT", "2027-01-14 10:00", "INIT again: rfzonetime and successors empty."),
        ("INIT", "2027-01-15 10:00", "INIT again: sfsdivision snapshot empty, d_ history still there; rfzonetime/successors back."),
        ("INIT", "2027-01-16 10:00", "INIT again: sfsdivision snapshot AND d_ history empty; rfzonetime, successors, references present."),
    ])),
    "partial": Chain("Partial arrival: the whole key set replaced ({A:T} -> {B:T}), then > 100 open versions (73%) vanishing at once (D-07).",
                     "INCR replacing every key; INCR adding 149 keys; INCR where 110 of 150 open keys vanish at once", _days("P", 9000, [
        ("INIT", "2027-01-13 10:00", "INIT with one division A."),
        ("INCR", "2027-01-14 10:00", "A vanishes, B arrives: every open div_type version vanishes."),
        ("INCR", "2027-01-15 10:00", "149 new divisions (150 open keys)."),
        ("INCR", "2027-01-16 10:00", "Only 40 of 150 remain: 110 vanish (73%)."),
    ])),
    "rerun": Chain("Rerun of a COMPLETED load with the same loadingId (CTL retry / manual restart, D-03).",
                   "INIT; INIT rerun with the same loadingId; INCR; INCR rerun with the same loadingId", [
        Load("R0", "INIT", "2027-01-13 10:00:00", 6000, "INIT."),
        Load("R1", "INIT", "2027-01-13 14:00:00", 6000, "The same INIT again (same loadingId, same sources), 4 hours later."),
        Load("R2", "INCR", "2027-01-14 10:00:00", 6001, "INCR with changes, a new key and a vanished key."),
        Load("R3", "INCR", "2027-01-14 14:00:00", 6001, "The same INCR again (same loadingId, same sources), 4 hours later."),
    ]),
    "cutover": Chain("Cutover: INIT on 3.0, the first INCR on 4.0 over the targets 3.0 wrote (same HDFS root and metastore), "
                     "compared with 3.0 doing that INCR itself (D-10, D-15).",
                     "INIT 3.0 -> INCR 4.0 over 3.0 targets", [
        Load("G0", "INIT", "2027-01-13 10:00:00", 9500, "INIT on 3.0 only; the 4.0 half starts from a copy of this state.", "30"),
        Load("G1", "INCR", "2027-01-14 10:00:00", 9501, "INCR on both: 3.0 over its own INIT, 4.0 over the copy of the 3.0 INIT."),
    ]),
    # ---- audit 1.3 (DV candidates); each chain is isolated because a 4.0 failure (expected by code) stops it
    "nullgrp": Chain("Two divisions without fulldivcode with different types at INIT: NULL-key div_type group (DV-01, H03).",
                     "INIT with a NULL-fulldivcode group of two types", [
        Load("A0", "INIT", "2027-01-13 10:00:00", 9200, "INIT."),
    ]),
    "degen": Chain("Degenerate INIT: no division has a non-empty divisiontype00 (terr_div_type SK init without a key, DV-01, H04).",
                   "INIT where every divisiontype00 is NULL or ''", [
        Load("W0", "INIT", "2027-01-13 10:00:00", 9250, "INIT."),
    ]),
    "emptynull": Chain("INIT with a NULL-key group (fulldivcode NULL and ''), then INCR with every source empty (DV-07, H06).",
                       "INIT with a NULL-key group -> INCR with all sources empty", _days("U", 9300, [
        ("INIT", "2027-01-13 10:00", "INIT with fulldivcode NULL and '' plus one ordinary key."),
        ("INCR", "2027-01-14 10:00", "INCR: every source empty."),
    ])),
    "nonmono": Chain("Non-monotonic loadingId: INCR with an id LOWER than the INIT id stored in snp (DV-13, H08).",
                     "INIT id 9405 -> INCR id 9400", [
        Load("Y0", "INIT", "2027-01-13 10:00:00", 9405, "INIT with the higher loadingId."),
        Load("Y1", "INCR", "2027-01-14 10:00:00", 9400, "INCR with a LOWER loadingId than the stored rows."),
    ]),
    "srctype": Chain("d_ sfsdivision.ctl_validfrom declared STRING (DV-20, H09); 3.0 is expected to fail the INIT.",
                     "INIT with a STRING ctl_validfrom in the d_ history", [
        Load("T0", "INIT", "2027-01-13 10:00:00", 9450, "INIT."),
    ], types={"d_sfs.ctl_validfrom": "string"}),
    "rerunnull": Chain("Rerun of an INCR (same loadingId) that only re-stamped the NULL group of terr_div_type (DV-27, J01).",
                       "INIT; INCR; INCR rerun with the same loadingId", [
        Load("J0", "INIT", "2027-01-13 10:00:00", 9600, "INIT."),
        Load("J1", "INCR", "2027-01-14 10:00:00", 9601, "INCR, same snapshot."),
        Load("J2", "INCR", "2027-01-14 14:00:00", 9601, "The same INCR again (same loadingId, same sources), 4 hours later."),
    ]),
    "sameday4": Chain("INIT, four INCRs on one calendar day, one INCR the next day: closed+open div_type versions with one PK (DV-04, T05/T06).",
                      "INIT -> 4 INCRs on one day -> INCR next day", _days("B", 9210, [
        ("INIT", "2027-01-13 10:00", "INIT."),
        # >= 3 h apart: the comparator classes ctl_validfrom as RUN@<first load whose fake time is <= ts < +3 h>
        ("INCR", "2027-01-15 06:00", "1st INCR of the day."),
        ("INCR", "2027-01-15 10:00", "2nd INCR of the day."),
        ("INCR", "2027-01-15 14:00", "3rd INCR of the day."),
        ("INCR", "2027-01-15 18:00", "4th INCR of the day."),
        ("INCR", "2027-01-16 10:00", "INCR the next day."),
    ])),
    "bigid": Chain("loadingId above 2^31-1 on INIT and INCR (DV-76, W02).",
                   "INIT and INCR with loadingId > Integer.MAX_VALUE", [
        Load("F0", "INIT", "2027-01-13 10:00:00", 2147483648, "INIT, loadingId 2^31."),
        Load("F1", "INCR", "2027-01-14 10:00:00", 2147483649, "INCR, loadingId 2^31+1."),
    ]),
    "sameid": Chain("INCR with the loadingId of the preceding INIT: snp holds L, diff does not (guard PublishPreconditions:64-77, Q01).",
                    "INIT -> INCR with the same loadingId", [
        Load("Q0", "INIT", "2027-01-13 10:00:00", 9650, "INIT."),
        Load("Q1", "INCR", "2027-01-14 10:00:00", 9650, "INCR with the SAME loadingId as the INIT."),
    ]),
    # ---- audit 4.3 (dirty paths). Loads marked "30" run on 3.0 only: 3.0 writes the dirt itself, the 4.0 half starts from
    # the 3.0 root after the last of them. Load labels are unique across ALL chains (d() searches every chain);
    # same-day loads are >= 3 h apart (RUN@ classes). Labels are <letter><number> (gen.py --md sorts by s[0], int(s[1:])); all
    # 26 letters are taken, so new chains continue a letter with a new number range: _days("G1", ..) -> G10, G11, ...
    "hubtwins": Chain("Hub twins written by 3.0 itself: H0 INIT and H1 INCR on 3.0 only, in H1 new keys arrive twice and 3.0 keeps "
                      "both rows; 4.0 continues from the 3.0 root with twins in the target and in the input (DV-01/03/77/57; A01-A04).",
                      "INIT 3.0 -> INCR 3.0 (twins) -> INCR both (first 4.0) -> INCR -> INCR with every source empty", _days("H", 20000, [
        ("INIT", "2027-01-13 10:00", "INIT on 3.0 only.", "30"),
        ("INCR", "2027-01-14 10:00", "INCR on 3.0 only: new keys arrive twice, 3.0 writes div_/time_zone_rf twins; the 4.0 half starts after it.", "30"),
        ("INCR", "2027-01-15 10:00", "First INCR of 4.0 over the 3.0 twins, five input variants."),
        ("INCR", "2027-01-16 10:00", "Second wave; new twins (A04)."),
        ("INCR", "2027-01-17 10:00", "INCR with every source empty."),
    ])),
    "hubnull": Chain("NULL-key hub groups written by the 3.0 INIT (fulldivcode NULL and '', divisiontype00 NULL and '', a time zone "
                     "twice); 4.0 continues: an empty INCR, then a 20-row group without fulldivcode on every INCR (DV-07/57/77; A05-A07).",
                     "INIT 3.0 (NULL-key pairs) -> INCR all sources empty (first 4.0) -> INCR x3 with a NULL-key group", _days("V", 20050, [
        ("INIT", "2027-01-13 10:00", "INIT on 3.0 only: NULL-key pairs in div_ and terr_div_type, time_zone_rf twins.", "30"),
        ("INCR", "2027-01-14 10:00", "Every source empty; first INCR of 4.0 over the 3.0 targets."),
        ("INCR", "2027-01-15 10:00", "20 snapshot rows without fulldivcode (10 NULL, 10 ''); types NULL/'' back; one new zone row."),
        ("INCR", "2027-01-16 10:00", "Same snapshot."),
        ("INCR", "2027-01-17 10:00", "One row without fulldivcode left."),
    ])),
    "dhist": Chain("3.0 writes history dirt on two 3.0-only loads (closed+open version with one PK in 999912 from twin m_ rows; "
                   "a div_type key vanishes under 3.0 and fills its loader cache); 4.0 continues after O1 (O04, O05).",
                   "INIT 3.0 -> INCR 3.0 (twin m_ rows, vanish) -> INCR both (first 4.0) -> INCR", [
        Load("O0", "INIT", "2026-11-26 10:00:00", 20100, "INIT on 3.0 only: a d_-only future key.", "30"),
        Load("O1", "INCR", "2026-11-28 10:00:00", 20101, "INCR on 3.0 only: twin m_ rows, a div_type key vanishes.", "30"),
        Load("O2", "INCR", "2026-12-01 10:00:00", 20102, "First 4.0 load: renames, the vanished key returns as a pair."),
        Load("O3", "INCR", "2026-12-02 10:00:00", 20103, "Next INCR."),
    ]),
    "dtwin": Chain("The 3.0 INIT writes twin PKs (div_type, addr_h, div_div_relation) and future partitions; the FIRST INCR (4.0 takes "
                   "over) comes right after it; rn2 closures land in 202611, inside the zone (O06-O08).",
                   "INIT 3.0 with twin PKs -> INCR both (first 4.0) -> INCR", [
        Load("O4", "INIT", "2026-11-20 10:00:00", 20110, "INIT on 3.0 only: twin PKs and future partitions.", "30"),
        Load("O5", "INCR", "2026-12-02 10:00:00", 20111, "First 4.0 load over the 3.0 twins."),
        Load("O6", "INCR", "2026-12-03 10:00:00", 20112, "Next INCR."),
    ]),
    "dtwinold": Chain("Twin PKs as in dtwin with the 3.0 INIT 7 weeks before the first 4.0 INCR: rn2 closures land in 202610, below "
                      "the cut point 202611 (DV-80, O09).",
                      "INIT 3.0 with twin PKs -> INCR both 7 weeks later -> INCR", [
        Load("O7", "INIT", "2026-10-14 10:00:00", 20120, "INIT on 3.0 only: twin PKs.", "30"),
        Load("O8", "INCR", "2026-12-02 10:00:00", 20121, "First 4.0 load; closures go below the zone."),
        Load("O9", "INCR", "2026-12-03 10:00:00", 20122, "Next INCR."),
    ]),
    "dtwinempty": Chain("Twin PKs as in dtwin, then INCRs with every source empty: the window of 3.0 over 999912 alone (DV-79, O13).",
                        "INIT 3.0 with twin PKs -> INCR with all sources empty (first 4.0) -> INCR with all sources empty", [
        Load("C7", "INIT", "2026-11-20 10:00:00", 20150, "INIT on 3.0 only: twin PKs.", "30"),
        Load("C8", "INCR", "2026-12-02 10:00:00", 20151, "First 4.0 load: every source empty."),
        Load("C9", "INCR", "2026-12-03 10:00:00", 20152, "Every source empty again."),
    ]),
    "dnonmono": Chain("DV-13 over 3.0 twins: the first 4.0 INCR has a loadingId LOWER than the 3.0 INIT (O10).",
                      "INIT 3.0 id 20190 -> INCR both id 20180", [
        Load("C0", "INIT", "2026-11-20 10:00:00", 20190, "INIT on 3.0 only, the higher loadingId: twin PKs.", "30"),
        Load("C1", "INCR", "2026-12-02 10:00:00", 20180, "First 4.0 load with a LOWER loadingId than the stored rows."),
    ]),
    "dnull": Chain("DV-81: NULL-fulldivcode PERIOD rows (three NULL divisions on each INCR) over a 3.0 INIT with one NULL division (O11).",
                   "INIT 3.0 with one NULL division -> INCR x2 with three NULL divisions", [
        Load("C2", "INIT", "2026-11-20 10:00:00", 20130, "INIT on 3.0 only: one division without fulldivcode.", "30"),
        Load("C3", "INCR", "2026-12-01 10:00:00", 20131, "First 4.0 load: it is gone, three other NULL divisions arrive."),
        Load("C4", "INCR", "2026-12-02 10:00:00", 20132, "Same three rows."),
    ]),
    "dyear": Chain("DV-65 on a 3.0 target: the 3.0 INIT writes p1month '20101' (year 201) and 189912; 4.0 runs the next INCR over them (O12).",
                   "INIT 3.0 with exotic dates -> INCR both", [
        Load("C5", "INIT", "2026-11-20 10:00:00", 20140, "INIT on 3.0 only: d_ versions from 0201 and 1899.", "30"),
        Load("C6", "INCR", "2026-12-02 10:00:00", 20141, "First 4.0 load over the exotic partitions."),
    ]),
    "rerundirty": Chain("Rerun with the same loadingId over a target 3.0 made dirty itself (INIT, then an INCR with a LOWER id, both "
                        "3.0 only), then both versions rerun that INCR twice and run a normal INCR (DV-08 on dirt, DV-28; S01, S03).",
                        "INIT 3.0 -> INCR 3.0 lower id -> INCR rerun same id x2 (both) -> INCR", [
        Load("R10", "INIT", "2027-01-13 10:00:00", 20205, "INIT on 3.0 only.", "30"),
        Load("R11", "INCR", "2027-01-14 10:00:00", 20200, "INCR on 3.0 only with a LOWER loadingId: twins.", "30"),
        Load("R12", "INCR", "2027-01-14 14:00:00", 20200, "Rerun of R11 (same loadingId); 4.0 starts from the 3.0 root after R11."),
        Load("R13", "INCR", "2027-01-14 18:00:00", 20200, "Second rerun, same sources as R12."),
        Load("R14", "INCR", "2027-01-15 10:00:00", 20206, "Normal next-day INCR."),
    ]),
    "mday": Chain("DEV day 2026-10-16 replayed (forks src_dup_div_divh, src_dup_divtype, same_day_hist_layout): INIT and two "
                  "INCRs on the INIT calendar day, then an INCR on the 1st of the next month; two m_ rows per fulldivcode "
                  "(3.0 writes the twins itself); d_ history since 2023 spread over p1month 202310..202701.",
                  "INIT; INCR on the INIT day; 2nd INCR on the INIT day; INCR on the 1st of month", _days("M1", 20300, [
        ("INIT", "2027-01-13 08:00", "INIT (DEV 27957342) over duplicated m_ rows and d_ history from 2023."),
        ("INCR", "2027-01-13 12:00", "INCR on the INIT day (DEV 27958163): D-1/D-2 lie before the INIT date."),
        ("INCR", "2027-01-13 16:00", "2nd INCR on the INIT day (DEV 27959455)."),
        ("INCR", "2027-02-01 10:00", "1st of month: closures land in 202701, rewrite zone 202702+202701, 2023 partitions untouched."),
    ])),
    "mcut": Chain("FAITHFUL CUTOVER (fork dtgt_faithful_cutover, DEV-like): 3.0 alone runs INIT + two INCRs on the INIT day "
                  "over duplicated source rows, so the target dirt (div_ twins, two open div_h versions with one PK, "
                  "R-only legs) is 3.0's own; 4.0 takes over from that 3.0 root at MC3.",
                  "INIT 3.0; 2 INCRs same day 3.0; cutover INCR next day (both); INCR on the 1st of month (both)", _days("M2", 20310, [
        ("INIT", "2027-01-13 08:00", "3.0 INIT over duplicated source rows.", "30"),
        ("INCR", "2027-01-13 12:00", "3.0 INCR on the INIT day.", "30"),
        ("INCR", "2027-01-13 16:00", "3.0 2nd INCR on the INIT day; 4.0 starts from the 3.0 state after it.", "30"),
        ("INCR", "2027-01-14 10:00", "Cutover INCR on both: duplicate copies #b of U11/U13 gone, U14 #a changes."),
        ("INCR", "2027-02-01 10:00", "1st of month, both versions."),
    ])),
    "mval": Chain("SOURCE-side dirty values of the DEV audit (local_dirty chain dsrc): empty and dangling FKs, garbage dates, "
                  "spaces, long strings, the literal 'null', ties in the d_ history.",
                  "INIT; INCR next day; 2nd INCR the same day; INCR on the 1st of month", _days("M3", 20320, [
        ("INIT", "2027-01-13 10:00", "INIT with dirty source values."),
        ("INCR", "2027-01-14 10:00", "Next day: dirty keys change one field."),
        ("INCR", "2027-01-14 16:00", "Second INCR the same day: no change."),
        ("INCR", "2027-02-01 10:00", "1st of month: legal address, parent and name changes."),
    ])),
    "mretry": Chain("Relaunch of a completed INCR with the same loadingId (fork essa_retry_rerun F3), then the next INCR; "
                    "a duplicated source key makes 3.0 write twins before the relaunch (DV-08 over dirty target).",
                    "INIT; INCR next day; relaunch of the same loading (same loadingId); INCR next day", [
        Load("M40", "INIT", "2027-01-13 10:00:00", 20330, "Clean INIT (plus one duplicated source key)."),
        Load("M41", "INCR", "2027-01-14 10:00:00", 20331, "Name / short code / type changes."),
        Load("M42", "INCR", "2027-01-14 16:00:00", 20331, "RELAUNCH of loading 20331 (same loadingId, same sources)."),
        Load("M43", "INCR", "2027-01-15 10:00:00", 20333, "Next INCR, sources unchanged."),
    ]),
    "dgrow": Chain("Dirt accumulated over 4 INCRs at volume (perf, DV-49/DV-54/DV-46/DV-81): churning keys with history, "
                   "exact duplicate snapshot rows, mass vanish/return, a renamed NULL-key group (X01-X04).",
                   "INIT with 1900 snapshot rows + 1200 d_ rows; 4 INCRs on consecutive days", _days("G1", 20400, [
        ("INIT", "2027-01-13 10:00", "INIT: 1900 snapshot rows (300 x3 exact duplicates, 100 NULL-key rows), 1200 d_ rows."),
        ("INCR", "2027-01-14 10:00", "150 renames, 300 keys vanish, the NULL group renamed."),
        ("INCR", "2027-01-15 10:00", "150 renames, 300 keys return at once, the NULL group renamed."),
        ("INCR", "2027-01-16 10:00", "150 renames, 300 keys vanish again, the NULL group renamed."),
        ("INCR", "2027-01-17 10:00", "150 renames, 300 keys return again, the NULL group renamed."),
    ])),
    "dtwincut": Chain("3.0 writes a dirty target (div_type / time_zone_rf / NULL-key div_ twins), 4.0 takes over: "
                      "empty-sfsdivision INCR over twins (DV-57), then every division returns at once (DV-46) (X11-X14).",
                      "INIT 3.0 with twins -> INCR empty sfsdivision -> INCR everything returns", [
        Load("M0", "INIT", "2027-01-13 10:00:00", 20450, "INIT on 3.0 only: 3.0 itself writes twins; the 4.0 half starts from this state.", "30"),
        Load("M1", "INCR", "2027-01-14 10:00:00", 20451, "INCR: sfsdivision empty, rfzonetime unchanged (twins stay in the input)."),
        Load("M2", "INCR", "2027-01-15 10:00:00", 20452, "INCR: all 802 snapshot rows return at once."),
    ]),
    "midnight": Chain("DV-05: INCRs starting just before midnight of the 1st, so the run crosses into the new day; every step "
                      "takes 'today' at its own moment (start_dt/closures, rewrite zone, VANISH, stg.chg_div_h) (N05).",
                      "INIT -> INCR on the 1st -> INCR at 23:58 on the 31st -> INCR at 23:59:50 on the 28th", [
        Load("N10", "INIT", "2026-12-20 10:00:00", 20500, "INIT."),
        Load("N11", "INCR", "2027-01-01 10:00:00", 20501, "1st of month: closures at D-2 land in 202612."),
        Load("N12", "INCR", "2027-01-31 23:58:00", 20502, "Crosses midnight: steps after 00:00 take 2027-02-01 (zone 202701+, start/closures a day later)."),
        Load("N13", "INCR", "2027-02-28 23:59:50", 20503, "Midnight 10 s after the start, before the history steps of both versions: they take 2027-03-01."),
    ]),
}

MIN, MAX = "1900-01-01", "9999-12-31"


def d(label, delta=0, chain=None):
    """Business date of a load plus delta days, ISO string. d('L3', -1) = D-1 of L3."""
    for ch in ([chain] if chain else CHAINS):
        for ld in CHAINS[ch]:
            if ld.label == label:
                return (ld.date + _dt.timedelta(days=delta)).isoformat()
    raise KeyError(label)


def p1month(date_iso):
    return "999912" if date_iso == MAX else date_iso[:4] + date_iso[5:7]


# --------------------------------------------------------------------------------------------
# Source tables. Layouts = DESCRIBE of the real DEV tables (FESS_DEV, 2026-10-26; same column
# order and types). Only the columns in READS are ever filled by a case; the rest stay NULL.
# Reference tables (gar_fias/rdm) are minimal: only the three columns both versions read.
# --------------------------------------------------------------------------------------------

_M_SFS = """addinformation:string address_addrfact_addinformation:string address_addrfact_addrarea_areaname:string address_addrfact_addrarea_objtype:string address_addrfact_addrcity_cityname:string address_addrfact_addrcity_objtype:string address_addrfact_addrhouse_housenumbers:string address_addrfact_addrhouse_objtype:string address_addrfact_addrplace_placename:string address_addrfact_addrplace_objtype:string address_addrfact_addrstreet_streetname:string address_addrfact_addrstreet_objtype:string address_addrfact_postcode:string address_addrfact_propertymanagmentsystemid:string address_addrfact_enddateaddress:string address_addrfact_fiascode:string address_addrfact_geographiccoordinates_geographiclatitude:string address_addrfact_geographiccoordinates_geographiclongitude:string address_addrfact_ismainaddress:string address_addrfact_metrostation:string address_addrfact_okatocode:string address_addrfact_oktmocode:string address_addrfact_timezone:string address_addrfact_startdateaddress:string address_addrfact_parkplacecount:string address_country:string address_isidenticaladdresses:string address_legaladdrdoc:string address_locationaddress:string address_placestatus:string address_placetype:string address_ppkoroute:string address_subjectcode:string address_postcode:string bushvsp_codeauthorizedvsp:string bushvsp_codebport:string businessformats_businessformatadd1:string businessformats_businessformatadd2:string businessformats_businessformatmain:string businessstatus:string casnsicode:string changeplandate:string closedate:timestamp contactmanagers_email:string contactmanagers_manager:string contactmanagers_phonemanager:string divisiontype00:string divisionkind:string facilities_ismeetsrequirements:string facilities_facilitystatus:string facilities_ismoneyinsurance:string foreignexchangelaw:string fulldivcode:string fullname:string headcount_groupposcount:string headcount_groupposvacancy:string headcount_positioncode:string headcount_positionname:string headdivision:string holidaymode_holidayschedule:string holidaymode_modetype:string id:string individualname:string modificationattributes_createdby:string modificationattributes_creationdate:string modificationattributes_modificationdate:string modificationattributes_modifiedby:string osbcode:string opendate:timestamp overallcount:string requisitesdivision_bic:string requisitesdivision_correspondentacc:string requisitesdivision_idparticipant:string requisitesdivision_inn:string requisitesdivision_kpp:string requisitesdivision_okpo:string requisitesdivision_registrationdate:string requisitesdivision_registrationnum:string serving_isbusinessservice:string serving_ischildrencorner:string serving_iscustomerreception:string serving_isdisabledequip:string serving_isdisplayexternresources:string serving_isdivisiongated:string serving_isgivebiodata:string serving_ispersonservice:string serving_istargetvsp:string serving_issurdo:string serving_isvisuallyimpaired:string serving_iszone24withselfservicedev:string serving_statevehiclenumber:string serving_reasonnotdisplayextres:string shortdivcode:string statreportcode:string subordination_oldcasnsicode:string subordination_oldstatrepcode:string subordination_subordinationdate:string tbcode:string vspcode:string headdivisioncode:string ctl_loading:string"""

_D_SFS = """addinformation:string address_addrfact_addinformation:string address_addrfact_addrarea_areaname:string address_addrfact_addrarea_objtype:string address_addrfact_addrcity_cityname:string address_addrfact_addrcity_objtype:string address_addrfact_addrhouse_housenumbers:string address_addrfact_addrhouse_objtype:string address_addrfact_addrplace_placename:string address_addrfact_addrplace_objtype:string address_addrfact_addrstreet_streetname:string address_addrfact_addrstreet_objtype:string address_addrfact_postcode:string address_addrfact_propertymanagmentsystemid:string address_addrfact_enddateaddress:timestamp address_addrfact_fiascode:string address_addrfact_geographiccoordinates_geographiclatitude:string address_addrfact_geographiccoordinates_geographiclongitude:string address_addrfact_ismainaddress:string address_addrfact_metrostation:string address_addrfact_okatocode:string address_addrfact_oktmocode:string address_addrfact_timezone:string address_addrfact_startdateaddress:timestamp address_addrfact_parkplacecount:string address_country:string address_isidenticaladdresses:string address_legaladdrdoc:string address_locationaddress:string address_placestatus:string address_placetype:string address_ppkoroute:string address_subjectcode:string address_postcode:string bushvsp_codeauthorizedvsp:string bushvsp_codebport:string businessformats_businessformatadd1:string businessformats_businessformatadd2:string businessformats_businessformatmain:string businessstatus:string casnsicode:string changeplandate:timestamp closedate:timestamp contactmanagers_email:string contactmanagers_manager:string contactmanagers_phonemanager:string divisiontype:string divisionkind:string facilities_ismeetsrequirements:string facilities_facilitystatus:string facilities_ismoneyinsurance:string foreignexchangelaw:string fulldivcode:string fullname:string headcount_groupposcount:string headcount_groupposvacancy:string headcount_positioncode:string headcount_positionname:string headdivision:string holidaymode_holidayschedule:string holidaymode_modetype:string id:string individualname:string modificationattributes_createdby:string modificationattributes_creationdate:timestamp modificationattributes_modificationdate:timestamp modificationattributes_modifiedby:string osbcode:string opendate:timestamp overallcount:string requisitesdivision_bic:string requisitesdivision_correspondentacc:string requisitesdivision_idparticipant:string requisitesdivision_inn:string requisitesdivision_kpp:string requisitesdivision_okpo:string requisitesdivision_registrationdate:timestamp requisitesdivision_registrationnum:string serving_isbusinessservice:string serving_ischildrencorner:string serving_iscustomerreception:string serving_isdisabledequip:string serving_isdisplayexternresources:string serving_isdivisiongated:string serving_isgivebiodata:string serving_ispersonservice:string serving_istargetvsp:string serving_issurdo:string serving_isvisuallyimpaired:string serving_iszone24withselfservicedev:string serving_statevehiclenumber:string serving_reasonnotdisplayextres:string shortdivcode:string statreportcode:string subordination_oldcasnsicode:string subordination_oldstatrepcode:string subordination_subordinationdate:timestamp tbcode:string vspcode:string headdivisioncode:string ctl_do:string ctl_action:string snapshot_:string ctl_validfrom:timestamp ctl_loading:int"""

_SUCC = "id:string idx:int successors_id_successor:string successors_successordivision:string successors_successorsdate:timestamp successors_successorstype:string"
_TZ = "code:int name00:string abbreviation:string timedesignation:string utcformat:string ctl_loading:int"
_REF = "sid:string deleted_flag:boolean dirty_flag:boolean"


def _cols(spec):
    return [tuple(c.split(":")) for c in spec.split()]


# short table alias -> (full name, columns)
SOURCES = {
    "m_sfs": ("usl_vspc_idl_in_snp.ebx_rep_m_sfs_0002_sfsdivision", _cols(_M_SFS)),
    "d_sfs": ("usl_vspc_idl_in_diff.ebx_rep_d_sfs_0002_sfsdivision", _cols(_D_SFS)),
    "succ": ("usl_vspc_idl_in_snp.ebx_rep_m_sfs_0002_sfsdivision_successors", _cols(_SUCC)),
    "tz": ("usl_vspc_idl_in_snp.ebx_rep_m_rfzonetime_0002_rfzonetime", _cols(_TZ)),
    "gar": ("usl_gar_fias_idl_snp.addr", _cols(_REF)),
    "okato": ("usl_rdm_idl_spark_snp.okato2", _cols(_REF)),
    "oktmo": ("usl_rdm_idl_spark_snp.oktmo2", _cols(_REF)),
    "pay": ("usl_rdm_idl_spark_snp.pay_sys_prtcpnt", _cols(_REF)),
}

_CHG3 = "3.0 tables/chgdivh/ChgDivHSql.java:14-87 (INCR), :211-290 (INIT)"
_CHG4 = "4.0 transformations/ChgDivHTransformation.java:38-111 (INCR), :237-316 (INIT)"
_DIVATTR = [
    "fulldivcode", "fullname", "shortdivcode", "closedate", "opendate", "headdivisioncode",
    "requisitesdivision_bic", "requisitesdivision_registrationnum", "address_locationaddress",
    "address_addrfact_fiascode", "address_addrfact_timezone", "serving_isdisabledequip",
    "address_addrfact_postcode", "address_postcode", "address_addrfact_addrarea_areaname",
    "address_addrfact_addrplace_placename", "address_addrfact_addrcity_cityname",
    "address_addrfact_addrhouse_housenumbers", "address_addrfact_addrstreet_streetname", "address_country",
    "address_addrfact_okatocode", "address_addrfact_oktmocode", "address_subjectcode", "address_legaladdrdoc"]

# Which source columns 3.0 / 4.0 actually read (and where). gen.py asserts that every column here
# exists in the DDL above and that cases fill no other column (except ctl/technical ones marked *).
READS = {
    "m_sfs": dict(
        [(c, _CHG3 + "; " + _CHG4) for c in _DIVATTR] + [
            ("divisiontype00", "3.0 DivTypeTransformer.java:39, TerrDivTypeTransformer.java:33; 4.0 DivTypeTransformation.java:52, TerrDivTypeTransformation.java:36"),
            ("id", "3.0 DivDivRelationTransformer.java:35-42; 4.0 DivDivRelationTransformation.java:47-55"),
            ("ctl_loading", "* technical: not read by 3.0; read by 4.0 (no requiredColumns -> select *, rt-fw-reader BaseReader.java:88-111), not used in its transformations"),
        ]),
    "d_sfs": dict(
        [(c, "INIT only: " + _CHG3.replace(":14-87 (INCR), ", ":") + "; 4.0 ChgDivHTransformation.java:237-276") for c in _DIVATTR if c != "address_addrfact_postcode"] + [
            ("address_addrfact_postcode", "INIT inner select only (not projected to chg_div_h): 3.0 ChgDivHSql.java:225; 4.0 :251"),
            ("snapshot_", "filter snapshot_ LIKE '%DIF%': 3.0 ChgDivHSql.java:249; 4.0 :275"),
            ("ctl_do", "filter ctl_do = 'R': 3.0 ChgDivHSql.java:250; 4.0 :276"),
            ("ctl_validfrom", "becomes start_dt of the version: 3.0 ChgDivHSql.java:245; 4.0 :271"),
            ("ctl_action", "* technical: not read"),
            ("ctl_loading", "* technical: not read (row order is by ctl_validfrom)"),
        ]),
    "succ": {
        "id": "3.0 DivDivRelationTransformer.java:35,40; 4.0 DivDivRelationTransformation.java:51",
        "successors_id_successor": "3.0 DivDivRelationTransformer.java:42; 4.0 :52",
        "successors_successorstype": "3.0 DivDivRelationTransformer.java:43; 4.0 :53,56",
        "idx": "* technical: not read (used only to make duplicate successor rows distinct in the source)",
    },
    "tz": {
        "abbreviation": "3.0 TimeZoneTransformer.java:32-41; 4.0 TimeZoneTransformation.java:34-43",
        "code": "3.0 TimeZoneTransformer.java:34 (int, no cast); 4.0 :36 (cast to string)",
        "timedesignation": "3.0 :35; 4.0 :37",
        "name00": "3.0 :36; 4.0 :38",
        "utcformat": "3.0 :39; 4.0 :41",
        "ctl_loading": "* technical: not read",
    },
}
for _r in ("gar", "okato", "oktmo", "pay"):
    READS[_r] = {c: "lookup in chg_div_h preselect: 3.0 ChgDivHSql.java:47-87, :299-338; 4.0 ChgDivHTransformation.java:71-111, :325-364"
                 for c in ("sid", "deleted_flag", "dirty_flag")}


def check_reads():
    for t, cols in READS.items():
        names = [c for c, _ in SOURCES[t][1]]
        for c in cols:
            assert c in names, "READS: %s.%s is not in the source DDL" % (t, c)


# --------------------------------------------------------------------------------------------
# Case DSL
# --------------------------------------------------------------------------------------------

GONE = "__GONE__"   # value of an `at` entry: the row is absent from this load on


class SrcRow:
    """One logical source row whose content may change from load to load.

    base: attributes at the first load where the row exists (start).
    at:   {load_label: dict of changed attributes | GONE}. Changes accumulate; a dict after GONE
          brings the row back (with all earlier changes plus the new ones).
    """

    def __init__(self, case, table, label, base, at=None, start=None):
        self.case, self.table, self.label, self.base = case, table, label, dict(base)
        self.at = dict(at or {})
        self.start = start
        for k in self.base:
            if k not in READS[table]:
                raise ValueError("%s: column %s.%s is read by neither 3.0 nor 4.0" % (case.id, table, k))
        for v in self.at.values():
            if v != GONE:
                for k in v:
                    if k not in READS[table]:
                        raise ValueError("%s: column %s.%s is read by neither 3.0 nor 4.0" % (case.id, table, k))

    def states(self, chain):
        """[(load, dict or None)] for every load of the chain."""
        out, cur, present = [], dict(self.base), self.start is None
        for ld in CHAINS[chain]:
            if self.start == ld.label:
                present = True
            ch = self.at.get(ld.label)
            if ch == GONE:
                present = False
            elif ch is not None:
                cur.update(ch)
                present = True
            out.append((ld, dict(cur) if present else None))
        return out


class Expect:
    def __init__(self, case, load, table, key, rows, note=None):
        self.case, self.load, self.table, self.key, self.rows, self.note = case, load, table, key, rows, note


class Case:
    def __init__(self, id, title, chain="main", rules=(), changes=(), scenario="", tables=(), summary="",
                 deterministic=True, known40="", accepted="", accept=()):
        self.id, self.title, self.chain = id, title, chain
        self.rules, self.changes, self.scenario, self.tables = list(rules), list(changes), scenario, list(tables)
        self.summary, self.deterministic = summary, deterministic
        self.known40 = known40   # observed / known behaviour of the 4.0 baseline, for the reader (not checked)
        self.accepted = accepted  # non-empty: a documented, accepted 3.0/4.0 deviation (the comparator says ACCEPTED)
        self.accept = list(accept)  # "<table>:<key>" (snp/diff/stg table without layer): the only rows the deviation may touch
        self.rows, self.expects, self.notes, self.special_keys = [], [], {}, {}

    # ---- source rows ----
    def div(self, key, at=None, start=None, label=None, **attrs):
        """A division row in the snapshot ebx_rep_m_sfs_0002_sfsdivision. key = fulldivcode = id.

        Defaults: no addresses, no time zone, no BIC, no parent, no registration number (so reg_ord is NULL
        everywhere except in C25, which owns that behaviour), type '<case>_T', disabled-equipment flag '0'."""
        base = dict(fulldivcode=key, id=key, fullname="%s name" % key, shortdivcode="01",
                    opendate="2020-01-01 00:00:00",
                    serving_isdisabledequip="0", divisiontype00="%s_T" % self.id, ctl_loading="1")
        base.update(attrs)
        r = SrcRow(self, "m_sfs", label or key, base, at, start)
        self.rows.append(r)
        return r

    def hist(self, div_row, validfrom, ctl_loading, start=None, ctl_do="R", snapshot_="DIF", at=None, **attrs):
        """A history row in the diff table ebx_rep_d_sfs_0002_sfsdivision, derived from the division's base."""
        base = {k: v for k, v in div_row.base.items() if k in READS["d_sfs"]}
        base.update(dict(ctl_do=ctl_do, ctl_action="U", snapshot_=snapshot_, ctl_validfrom=validfrom, ctl_loading=ctl_loading))
        base.update(attrs)
        r = SrcRow(self, "d_sfs", "%s@%s" % (div_row.label, validfrom), base, at, start)
        self.rows.append(r)
        return r

    def succ(self, src_id, succ_id, stype, idx=0, at=None, start=None):
        r = SrcRow(self, "succ", "%s->%s#%d" % (src_id, succ_id, idx),
                   dict(id=src_id, idx=idx, successors_id_successor=succ_id, successors_successorstype=stype), at, start)
        self.rows.append(r)
        return r

    def tz(self, label, at=None, start=None, **attrs):
        base = dict(ctl_loading=1)
        base.update(attrs)
        r = SrcRow(self, "tz", label, base, at, start)
        self.rows.append(r)
        return r

    def ref(self, table, sid, deleted=False, dirty=False, at=None, start=None):
        r = SrcRow(self, table, sid, dict(sid=sid, deleted_flag=deleted, dirty_flag=dirty), at, start)
        self.rows.append(r)
        return r

    # ---- expectations (3.0 truth) ----
    def expect(self, load, table, key, rows, note=None):
        self.expects.append(Expect(self, load, table, key, rows, note))

    def note(self, load, text):
        self.notes.setdefault(load, []).append(text)

    def owns(self, sid, why, table=None):
        """A natural key that does not contain the case id (e.g. 'KALT', NULL) belongs to this case (only in table, if given)."""
        self.special_keys[(sid, table)] = why


# expected-row helpers ------------------------------------------------------------------------
NOTNULL = "<notnull>"
NULL = None


def ver(start, end, vf, ld, **attrs):
    """Snapshot row of a historical table (div_h, addr_h, div_type, div_div_relation)."""
    r = dict(start_dt=start, end_dt=end, ctl_validfrom=vf, ctl_loading=ld)
    r.update(attrs)
    return r


def leg(do, action, start, end, vf, **attrs):
    """Diff row of a historical table (ctl_loading = the load being checked)."""
    r = dict(ctl_do=do, ctl_action=action, start_dt=start, end_dt=end, ctl_validfrom=vf)
    r.update(attrs)
    return r


def hub(vf, ld, **attrs):
    r = dict(ctl_validfrom=vf, ctl_loading=ld)
    r.update(attrs)
    return r


def hleg(do, action, vf, **attrs):
    r = dict(ctl_do=do, ctl_action=action, ctl_validfrom=vf)
    r.update(attrs)
    return r


def RUN(label):
    return "RUN@" + label


START = "START"


def addr_key(durty=None, src=None, okato=None, oktmo=None):
    """3.0 AddrTransformer.getKey('|') (AddrTransformer.java:248-263): NULL parts become 'NULL'."""
    if durty is None and src is None and okato is None and oktmo is None:
        return None
    return "|".join("NULL" if p is None else p for p in (durty, src, okato, oktmo))


# --------------------------------------------------------------------------------------------
# SQL rendering
# --------------------------------------------------------------------------------------------


def _lit(v, typ):
    if v is None:
        return "NULL"
    if typ == "string":
        s = str(v)
        assert "\\" not in s and "\n" not in s and not s.rstrip().endswith(";"), "unsupported literal %r" % s
        return "'" + s.replace("'", "\\'") + "'"
    if typ == "timestamp":
        s = str(v)
        if len(s) == 10:
            s += " 00:00:00"
        return "timestamp'%s'" % s
    if typ == "boolean":
        return "true" if v else "false"
    if typ == "int":
        return str(int(v))
    raise ValueError(typ)


def render_load_sql(chain, load_label, rows):
    """Full source state for one load: drop+create every source table, insert all rows present at this load."""
    out = ["-- generated by synth/gen.py -- chain=%s load=%s -- DO NOT EDIT (edit synth/cases.py)" % (chain, load_label)]
    by_table = {}
    for r in rows:
        for ld, st in r.states(chain):
            if ld.label == load_label and st is not None:
                by_table.setdefault(r.table, []).append((r, st))
    for t, (full, cols) in SOURCES.items():
        ov = CHAINS[chain].types
        cols = [(c, ov.get("%s.%s" % (t, c), ty)) for c, ty in cols]
        types = dict(cols)
        out.append("drop table if exists %s;" % full)
        out.append("create table %s (\n  %s\n) stored as parquet;" % (full, ",\n  ".join("`%s` %s" % c for c in cols)))
        items = by_table.get(t, [])
        if not items:
            out.append("-- %s: no rows at %s" % (t, load_label))
            continue
        used = [c for c, _ in cols if any(c in st for _, st in items)]
        out.append("insert into %s (%s) values" % (full, ", ".join(used)))
        items = sorted(items, key=lambda x: (x[0].case.id, x[0].label))
        for i, (r, st) in enumerate(items):
            # one row per line, trailing comment names the case; the stand's splitter ends a statement at a
            # line ending with ';', so the terminating ';' goes on its own line
            out.append("  (%s)%s  -- %s %s" % (", ".join(_lit(st.get(c), types[c]) for c in used),
                                               "," if i < len(items) - 1 else "", r.case.id, r.label))
        out.append(";")
    return "\n".join(out) + "\n"
