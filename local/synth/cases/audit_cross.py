"""Audit 1.3, theme cross (X1/X2): candidates DV-11, DV-14, DV-15, DV-35, DV-36.

Only DV-11 is expressible with data on the current stand (V01). DV-14/15/35/36 need a different DDL/model, Spark
build, classpath or the production config; see the phase 1.3 report. The DV-11 addr variants are C15/C16 (hash.py).
"""
from model import (Case, GONE, MIN, MAX, NOTNULL, START, RUN, d, ver, leg, hub, hleg, addr_key, p1month)  # noqa: F401
from cases import case


def v01():
    c = case(id="V01", title="DV-11: time_zone_rf hash without separator: code 70 + shift '1' == code 7 + shift '01' -> EQUALS in 3.0",
             rules=["H05-hash"], changes=["NH-hash-essa-xxhash-concat", "HPWR-hash-xxhash64-noseparator"], scenario="S4",
             tables=["time_zone_rf"],
             summary="DV-11. 3.0 hub hash = xxhash64(concat of the non-ctl columns, NULL -> '', no separator) "
                     "(E/slave/loading/SnpDataLoader.java:116-133) in the column order of the snp table: DataLoaderHubDiff "
                     "matchSchemas the input to the snp prototype before hashing (DataLoaderHubDiff.java:58, "
                     "SparkWrapper.java:104-114), DDL order time_zone_code, time_zone_msk_shift_val, time_zone_name are adjacent "
                     "(ddl...model.9.47.0_fw_3.9.6.sql:204-206, same file in 4.0). 3.0 hashes code as int (TimeZoneTransformer.java:34), the string is the same. "
                     "Moving a digit from code to timedesignation at L3 keeps the concatenation '701' -> EQUALS, rn2 (OLD) is kept: "
                     "no legs, snp keeps code '70', shift '1', L0 ctl (DataLoaderHubDiff.java:75-80, 127-135). A name change at L5 is "
                     "a real change: (R,U) carries the CURRENT source values (code '7', shift '01'), (U,U) the stored L0 row. "
                     "4.0: vendor md5(concat_ws('|')) sees '70|1' vs '7|01' (AbstractChangesCalculator.java:138-139); compat "
                     "HUB_HASH_ESSA30 (HubCompatCalculator.java:121-128) must drop those legs with hash30 over CompatTables' "
                     "time_zone_rf list (CompatTables.java:64-73, 103-105). If that list's order differed from the snp DDL, 4.0 "
                     "would write legs at L3.")
    c.tz("V01_TZ", code=70, abbreviation="V01_TZ", name00="V01 zone", timedesignation="1", utcformat="+4",
         at={"L3": dict(code=7, timedesignation="01"), "L5": dict(name00="V01 zone 2")})
    old = hub(RUN("L0"), "L0", time_zone_code="70", time_zone_msk_shift_val="1", time_zone_name="V01 zone")
    c.expect("L0", "snp.time_zone_rf", "V01_TZ", [old])
    c.expect("L3", "snp.time_zone_rf", "V01_TZ", [old], note="3.0 EQUALS: the stored row and its L0 ctl stay")
    c.expect("L3", "diff.time_zone_rf", "V01_TZ", [])
    c.expect("L5", "snp.time_zone_rf", "V01_TZ",
             [hub(RUN("L5"), "L5", time_zone_code="7", time_zone_msk_shift_val="01", time_zone_name="V01 zone 2")])
    c.expect("L5", "diff.time_zone_rf", "V01_TZ", [
        hleg("R", "U", RUN("L5"), time_zone_code="7", time_zone_msk_shift_val="01", time_zone_name="V01 zone 2"),
        hleg("U", "U", RUN("L0"), time_zone_code="70", time_zone_msk_shift_val="1", time_zone_name="V01 zone")])


for _f in [v01]:
    _f()
