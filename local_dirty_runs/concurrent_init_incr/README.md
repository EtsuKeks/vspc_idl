# vspc_idl 3.0 vs 4.0 parity stand - DIRTY data variant

This copy carries a different catalog: dirty patterns from the DEV audit (`/Users/timur/Desktop/ess/dev_data_anomalies_20261109.txt`),
grouped by its classes B/C/D/E/F/G in `synth/cases/{b_dups,c_nulls,d_fk,e_hist,f_values,g_stg}.py`, two chains:
`dsrc` (source-side dirt) and `dtgt` (target-side dirt seeded by SQL into snp/diff/stg before T1/T3, `Case.sql(load, ...)`,
statements prefixed `/*opt*/` may fail in one version's root). Cases carry no 3.0 expectations: the check is 3.0 vs 4.0.
```
./stand.sh run dsrc --v40-classes ../local/target/v40_final_classes --ports 19980,19981
./stand.sh run dtgt --v40-classes ../local/target/v40_final_classes --ports 19982,19983
./stand.sh explain B31      # source rows and seeded SQL of a case
```
Everything below describes the stand itself (unchanged).


Real ESSA 3.0 code (`data/vspc_idl_old`, read-only) and real RT-FW 4.0 code (`../target/classes`) run on the same
synthetic sources; the 16 target tables are compared row by row after every load. No Docker: Spark local,
MiniDFSCluster, Derby metastore, a patched `java.time.Clock` for the business date. Mocks: CTL (3.0 `CtlHelper`,
4.0 `StubCoordinator`), SK client (deterministic key = hash of the natural key, identical in both versions),
Kafka logger. 3.0 is the reference.

## Layout

| path | what |
|---|---|
| `stand.sh` | the only entry point: `build`, `gen`, `explain`, `run`, `compare`, `combine`, `hdfsd`, `apply`, `clean` (header = usage) |
| `java/common/` | `Stand` (HDFS daemon, SQL, one-JVM load driver, export), `Compare` (comparator), SK client mock - on both classpaths |
| `java/v30/`, `java/v40/` | version-specific mocks: `Run30` + CTL helper (3.0); CTL coordinator + core factory (4.0); `ctl40.properties` = 4.0 CTL params |
| `java/pom30.xml`, `java/pom40.xml` | dependency lists only (the two versions need different RT-FW/ESSA jars) |
| `synth/cases/<group>.py` | the case catalog, one file per theme (history, hubs, addr, hash, sources, links, keys, modes) |
| `synth/model.py` | chains (load sequences), source DDL (DEV layouts), case DSL, seed rendering |
| `synth/gen.py`, `synth/check.py`, `synth/rules_ref.py` | seeds/expectations/docs generator, expectation checker, rule-id legend |
| [`synth/CASES.md`](synth/CASES.md) | **catalog**: per group, case -> what it tests -> expected 3.0 outcome (generated) |
| [`synth/CHAINS.md`](synth/CHAINS.md) | **scenarios**: chains, their loads and the run modes they cover (generated) |
| `target/` | builds (`cp30`, `cp40`, `vspc30`, `common`, `classes30`, `classes40`, `clock`), frozen 4.0 builds (`v40_*_classes`), seeds (`synth`), runs (`runs/`) |

Generated, not stored: target DDL (from the 3.0 DDL in `data/vspc_idl_old`), source DDL and seeds (from the catalog),
the patched clock (from the JDK `src.zip`).

## Build and run

```
./stand.sh build                      # 3.0 + stand classes + clock -> target/  (4.0 is built by its own mvn compile)
./stand.sh run main                   # chain main on 3.0 and 4.0 in parallel (HDFS ports 19980/19981), then compare
./stand.sh run keys --only 30         # 3.0 only, e.g. to validate the catalog
./stand.sh run main --upto L3 --v40-classes target/v40_final_classes --keep
./stand.sh run all --v40-classes target/v40_final_classes   # every chain in turn (overview: target/runs/all_<time>.txt)
./stand.sh explain C34                # source rows per load and the exact expected 3.0 rows of one case
./stand.sh gen                        # after editing synth/cases/*.py: validate + regenerate CASES.md / CHAINS.md
```

One load = one JVM per version: it applies the load's source state (drop/create/insert of the 8 source tables;
the first load of a chain also creates databases and targets), runs the product with the business clock set to the
load's fake time, and exports the 16 targets as parquet. Only the HDFS daemon of the root lives across loads.
3.0 and 4.0 halves of a chain run in parallel on their own roots `<run>/r30`, `<run>/r40`. A load that takes longer
than `--step-timeout` (default 2400 s) is killed and the chain of that version stops (FAILED). Roots are deleted
after the run unless `--keep`. Run one chain at a time.

Cutover chains (`cutover`): loads marked "3.0 only" run on 3.0; after the last of them the 3.0 root is copied and the
4.0 half continues from that copy (after the 3.0 half, on the same HDFS port, because the metastore stores table
locations with the port).

Frozen 3.0 reference: run 3.0 once, then only 4.0 per new build and combine the halves:
```
./stand.sh run main --only 30 --out target/runs/ref30
./stand.sh run main --only 40 --seeds target/runs/ref30 --out target/runs/new40
./stand.sh combine target/runs/cmp_new40 target/runs/ref30 target/runs/new40
```

## Results (`target/runs/<chain>_<time>/`)

| file | content |
|---|---|
| `summary.txt` | status, timings, tables that differ per load, stg.chg_div_h schema, per case where 3.0 and 4.0 differ (`ACCEPTED` = documented deviation), expectation check |
| `cmp/<load>.txt`, `cmp/mismatches.tsv` | every differing row, grouped by case: `CHANGED` (same row identity, columns + both values), `ONLY 3.0`, `ONLY 4.0` |
| `expect_check.txt` | catalog expectations vs actual rows, for 3.0 (sanity of the catalog) and 4.0 |
| `dump/<30\|40>/<load>/<layer>.<table>.tsv` | normalized exports: `_case`, `_key`, sk columns shown as natural keys |
| `exp30/`, `exp40/`, `logs/<v>/<load>.log`, `timings.tsv` | raw exports, one log per load, seconds per load |

Comparison: all columns, multiset (`exceptAll` both ways); `ctl_validfrom` by class (`START` = start_dt 00:00,
`RUN@Lk` = within 3 h after the fake time of load Lk, `NULL`), because the versions stamp different instants of one
run; `ctl_loading` exactly (shown as the label of the first load with that loadingId).

## Add a case

1. In the fitting `synth/cases/<group>.py` add a function (copy a similar one) and append it to the list at the end of
   the file. Use only keys that contain the case id (`C41_1`, `'C41 fact'`, `'C41_T'`); other keys: `c.owns(key, why)`.
   - source rows: `c.div(key, at={"L3": dict(fullname="..."), "L5": GONE}, start="L1", **attrs)`,
     `c.hist(div_row, "2026-05-01 10:00:00", ctl_loading, **attrs)` (d_ history, INIT only), `c.tz(...)`,
     `c.succ(src_id, succ_id, type)`, `c.ref("gar"|"okato"|"oktmo"|"pay", sid, deleted=, dirty=)`;
     only columns that 3.0/4.0 read are accepted (`READS` in `synth/model.py`).
   - expectations (3.0 truth): `c.expect("L3", "snp.div_h", key, [ver(start, end, RUN("L3"), "L3", div_name=...)])`,
     diff legs `leg(...)` / `hleg(...)`, hubs `hub(...)`; `[]` = no rows. `accepted="..."` marks a documented deviation.
2. `./stand.sh gen`, then `./stand.sh run <chain> --only 30 --upto <load>`: 3.0 must PASS in `expect_check.txt`;
   if not, 3.0 is right and the catalog is wrong (`./stand.sh compare <run> --regen` re-checks without rerunning).
