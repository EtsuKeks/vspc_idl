# vspc_idl 3.0 vs 4.0 parity stand

Real ESSA 3.0 code (`ess/vspc_idl_old`, read-only) and real RT-FW 4.0 code (this repository) run on the same synthetic
sources; after every load the 16 target tables and `stg.chg_div_h` are compared row by row. 3.0 is the reference.
No Docker: Spark local, MiniDFSCluster, Derby metastore, a patched `java.time.Clock` for the business date.
Mocks: CTL (3.0 `CtlHelper`, 4.0 `StubCoordinator`), SK client (key = hash of the natural key, same in both), Kafka logger.

## Run

```
./stand.sh build                     # 3.0 + stand classes + clock -> target/ (4.0: its own mvn compile)
./stand.sh gen                       # after editing synth/cases/*.py: validate, regenerate synth/CASES.md, CHAINS.md
./stand.sh explain C34               # source rows per load + expected 3.0 rows of a case (or of a chain)
./stand.sh run main                  # one chain: 3.0 and 4.0 halves in parallel, then compare -> summary.txt
./stand.sh run keys --only 30        # 3.0 only (validates the catalog: expect_check.txt must PASS)
./stand.sh compare <runDir> --regen  # re-generate seeds/expectations from the current catalog and re-compare, no rerun
```

Heavy runs go to the VM, on a frozen build: `./stand.sh freeze` prints `<hash>` (`ess/builds/<hash>`: 4.0 + 3.0 + stand),
then `dispatch.py submit --dir <this dir> --same-path -x ./target --build <hash> --name <label>
-c "./stand.sh run <chain> --out target/runs/<name>" -o target/runs` (`ess/colab/dispatcher`).
One chain per JVM pair; `run all` runs the 38 chains one after another. `--upto`, `--keep`, `--seeds`, `combine`: see the
header of `stand.sh`.

## What is compared

- Per load, per table: all columns as a multiset (`exceptAll` both ways); every differing row is attributed to a case by
  its natural key (keys carry the case id; `keymap.tsv` for the rest).
- `ctl_validfrom` by class: `START` (start_dt 00:00), `RUN@Lk` (within **3 h** after the fake time of load Lk), `NULL`.
  `ctl_loading` exactly (shown as the label of the load).
- `expect_check.txt`: catalog expectations (3.0 truth) against both versions; 3.0 must PASS everywhere.

Files of a run (`target/runs/<chain>_<time>/`): `summary.txt` (result), `cmp/cases.txt` (verdict per case),
`cmp/mismatches.tsv` and `cmp/<load>.txt` (every differing row: `CHANGED` with both values, `ONLY 3.0`, `ONLY 4.0`),
`dump/<30|40>/<load>/` (normalized exports), `logs/`, `timings.tsv`.

## Result codes and verdicts

| rc | meaning |
|---|---|
| 0 | everything compared; no difference, or only `ACCEPTED` ones |
| 3 | at least one case `DIFFERENT` (or rows not attributable to a case) |
| 4 | no difference in what was compared, but not everything was compared (a load or table missing on one side) |
| other | the comparator crashed (`compare.log`) |

Per case (`cmp/cases.txt`): `same`; `same*` (nothing differs, but not everything was compared, e.g. cutover chains);
`ACCEPTED`; `DIFFERENT`.

**accepted / accept.** A case with `accepted="<reason>"` documents a deviation that is decided (`DECISIONS.md`);
`accept=["<table>:<key>", ...]` lists the only rows it may touch (table without layer, snp/diff/stg alike, `\N` = NULL key).
All differences inside the list → `ACCEPTED`; any row outside → `DIFFERENT` with `[NOT ACCEPTED <key>]`.
`DV77` (`synth/cases/__init__.py`) is the window tie of DV-77: 3.0 picks the twin by physical order, so its outcome
changes between identical runs; those keys are accepted, nothing else is.

## Limits (what the stand does not show)

- `ctl_validfrom` is a class, not a value: two stamps within 3 h of one load look equal (DV-56 → IFT).
- SK client is a mock: deterministic, no registry, no side effects. Real SK context, protocol, `Long.MIN_VALUE`, empty
  input calls (DV-02, DV-59, DV-60, DV-63) → IFT.
- No CTL parameters per load, no failure injection or retries inside a load, no SQL/HDFS step between loads,
  no concurrent INIT+INCR, no physical layer (ACL, file layout, stg/stgtmp tails), one clock, no Kerberos/launcher
  (Timur's decision 10.11: the stand engine is not extended). All of it is in `ess/audit_v2/IFT_CHECKLIST.md`.
- Target-side dirt is only what 3.0 itself writes (loads marked "3.0 only", then 4.0 continues from the 3.0 root);
  nothing is seeded by SQL into snp/diff/stg.
- Timings are indicative only (both halves share one machine; cutover chains run the halves one after another).
- A load that crosses midnight mixes business dates (N05, DV-05).

## Layout

`stand.sh` (only entry point) · `java/common` (`Stand` load driver/export, `Compare`, SK mock) · `java/v30`, `java/v40`
(version mocks, `ctl40.properties`) · `java/pom30.xml`, `pom40.xml` (dependency lists) · `synth/cases/*.py` (catalog,
one function per case; `GROUPS` in `__init__.py` gives the order) · `synth/model.py` (chains, source DDL, case DSL) ·
`synth/gen.py`, `check.py`, `rules_ref.py` · generated: [`synth/CASES.md`](synth/CASES.md),
[`synth/CHAINS.md`](synth/CHAINS.md) · `target/` (builds, seeds, runs; git-ignored).

To add a case: copy a similar function in the fitting `synth/cases/<group>.py`, use keys that contain the case id,
write the expectations in 3.0 terms, `./stand.sh gen`, then `./stand.sh run <chain> --only 30`: 3.0 must PASS.
