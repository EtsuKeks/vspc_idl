# Compatibility layer: old engine (ESSA) → RT-FW publisher

A mart moved from ESSA to RT-FW publishes through the RT-FW publisher (INCR merge path). The publisher computes the
diff and the new snapshot by its own rules, which differ from the old loaders in many small ways. The compatibility
layer (compat) bends the publisher's input and output so the target rows (snp, diff) come out as the old engine
wrote them. Consumers must notice nothing.

Reference implementation: vspc_idl, package `ru.sberbank.bigdata.dffess.vspc.compat`. The registry of all fixes, with
the old-engine rule for each one, is `compat/package-info.java`. The fix id (e.g. `PERIOD_ZONE_MONTH`) sits in a
comment next to its code: search by it. All compat log lines start with `[vspc-compat]` (`CompatSupport.fail/info`).

## How it is wired

`MainClass` → `StartupSelfCheck.verifyOrDie` → transformations → `BaseTransformation.publishChecked(result, mask)` →
RT-FW publisher (merge mode) → shadow `ProcessingStage` → `CompatHooks` → `HubCompatCalculator` /
`PeriodRewriteCompatCalculator` → `MergeCompatCalculator`. The publisher's own calculators still do the main work;
compat only decorates them.

## Mechanisms

Each item: problem → solution → where → what to configure in another mart.

**1. Shadow ProcessingStage and its three hooks.**
The publisher gives no extension point for the diff and merge calculators. The jar carries a copy of the vendor
class `ru/sberbank/bigdata/sl/rt/publisher/strategy/process/ProcessingStage.java`: vendor text unchanged, plus three
blocks marked `VSPC-COMPAT begin/end` — `enrichTechFieldsForInit` → `CompatHooks.enrichInit`, `calculateDiff` →
`CompatHooks.wrapChanges`, `calculateMergedSnp` → `CompatHooks.wrapMerge`.
Another mart: copy the class from the vendor text of your RT-FW version and move over only the three blocks.

**2. Shade exclusion.** Without it the vendor `ProcessingStage.class` may win on the classpath and compat silently
switches off. `pom.xml`, maven-shade-plugin filter on `rt-fw-publisher` excludes that class.
`StartupSelfCheck.checkShadowStage` refuses to start if `ProcessingStage` is loaded from a jar other than `CompatHooks`'s.
Another mart: copy the filter as is.

**3. Decorators of the changes calculator and of merge.**
`CompatHooks.wrapChanges` accepts only the exact vendor classes: `HistoricalPeriodWithRewriteChangesCalculator` for
period tables → `PeriodRewriteCompatCalculator`; `NonHistoricalChangesCalculator` for hubs → `HubCompatCalculator`.
Flags `fullSlice/reInit/collapsePeriods/physicalDeletion` must be false. `wrapMerge` accepts only
`GeneralMergeCalculator` after a wrapped diff of the same stage → `MergeCompatCalculator`. Anything else is a refusal.
Each decorator: fixes the input → calls the vendor calculator → fixes its output (legs).
Another mart: nothing, as long as its tables are HUB or PERIOD on these calculators.

**4. Table list.** Compat must know every published table. `CompatTables` (static block, `add(...)`): code, publish
mask, kind (`HUB` / `PERIOD`), PK, and all columns in the old DDL order (the order defines the hash, see 6).
An unknown mask or code is a refusal (`byMask`, `byCode`). Per-table switches `histSnp()` and `initVfFromStart()`
are by table code. Constants `P1MONTH` and `OPEN_PARTITION` (open versions partition) are fixed names.
Another mart: fill the list from its old DDL; set the switches by which old loader each table used; check partition names.

**5. Old-engine window for PK duplicates and NULL keys.**
The old engine numbered rows per key (snp OLD + input NEW, `desc(ctl_loading), asc(status)`) and took the first;
the vendor joins by PK, not null-safe, and multiplies duplicates. Keys with >1 row in input or snp (period: >1 open)
and NULL keys go around the vendor: `CompatSupport.keys30` picks them, `CompatSupport.window30` computes the window,
the calculators turn it into legs (`windowLegs` in both), `MergeCompatCalculator` puts their snapshot back
(`winKeys/winSnp`, `mergeNullKeys`). Ties in the window go by physical plan order, as in the old engine (DV-77).
Another mart: nothing beyond the PK in `CompatTables`.

**6. Old-engine comparison hash.** The vendor decides "changed or not" by its own rule; the old engine used
xxhash64(concat of DDL columns, NULL → ''), period tables without start_dt/end_dt. `CompatTables.Table.hash30`;
used in `HubCompatCalculator.hashLegs` (equal by old hash — no legs; changed but vendor-silent — legs added) and in
`PeriodRewriteCompatCalculator.calculate` (step 3). Another mart: correct column list and order in `CompatTables`.

**7. Rewrite zone by months.** The vendor reads/rewrites from a depth in days (`max_increment_depth_days`); the old
engine rewrote partitions `p1month >= month(today - 1 month)`. `PeriodRewriteCompatCalculator.getMinBusinessDateUpdateFilters`.
Closed partitions inside the zone are carried over untouched (`MergeCompatCalculator.calculate`, `PERIOD_CLOSED_KEEP`).
Another mart: metadata must have a BUSINESS_HISTORY partition by end_dt; the depth parameter in vars has no effect on data.

**8. Closing, new keys, VANISH, silent deletions** (all in `PeriodRewriteCompatCalculator`, steps numbered in `calculate`/`legs`).
- Open versions close at load date − 2 (D-2). The vendor closes at new start_dt − 1, so in INCR the transformation
  must set start_dt = `VspcLoadDate.sqlDate()` − 1 (example: `DivHTransformation`). Compat's own closings use D-2 directly.
- Repeat of the same full PK on a consecutive day: (R,I)+(U,D), the old version is deleted silently (`PERIOD_CONSEC_DAY`).
- New key without an open version: start_dt = 1900-01-01 (`newStart`); for `histSnp()` tables a returning key gets
  max(closed end_dt) + 1 (`returner`, `closedEnd`).
- VANISH (`histSnp()` tables only): open versions of keys absent from the input close at `current_date()` − 1.
- Silent deletions: vendor (R,D) legs the old engine did not publish go to `stash()`, not to diff;
  `MergeCompatCalculator` still removes them from the snapshot.
- "Today" is one date per transformation step: `VspcLoadDate` (reset in `publishChecked`).
Another mart: start_dt rule in each period transformation; `histSnp()` for tables loaded by the old DataLoaderHistSnp.

**9. ctl_validfrom.** INIT: the vendor stamps now(); for tables where the old loader used start_dt,
`CompatHooks.enrichInit` overwrites it (`initVfFromStart()`). INCR: one stamp for all legs of a load; hub (U,U) keeps
the previous version's value (`HubCompatCalculator.undoValidFrom`). Another mart: set `initVfFromStart()` per table.

**10. Empty input marker.** The vendor exits early on empty input; the old engine still processed window groups
(and closed all open versions for `histSnp()` tables). `CompatHooks.emptyIncrMarker` adds one row with key
`EMPTY_INPUT_MARKER`; both calculators drop it first thing. Another mart: nothing.

**11. Repeat of a load with the same ctl_loading.** The old engine appended to diff partition L, the vendor replaces
it. `PublishPreconditions.keepLegs` moves the partition aside around the publish and restores it. Another mart: nothing.

**12. Type mismatch input → target.** The vendor would write NULL; the old insertInto cast or failed.
`PublishPreconditions.alignTypes`: ANSI cast or refusal before writing. Another mart: nothing.

**13. Startup guards and guards against vendor changes.**
- `StartupSelfCheck`: RT-FW publisher/core versions on the classpath must equal the pinned constants; CTL parameters
  the old engine ignored are refused (`checkParams`: `init_diff_flag` on INIT, `starting_business_date`,
  `max_increment_depth_days` < 1); shadow class location (see 2).
- `CompatHooks.readField`: a missing vendor field means the vendor class changed — refusal.
- Comments `V/...:<line>` point at the vendor lines compat relies on.

**14. Spark and Kerberos settings the old engine set itself.**
- Code: `BaseTransformation` sets the SK client system
  properties (`turbo_boost` default true, `service_instance_code`) and local keys under `debug_sk_off`.
- Launcher `src/main/resources/hdfs/rtfw/StartLauncher.sh`: vendor listing (`.orig` next to it) plus marks `vspc Δn`:
  own Kerberos cache per run, a copy under the fixed name `rtfw_kerberos_ticket_cache_file`, `kvno_command.sh`,
  jar download, `$SPARK_HOME/jars/*` on the classpath, mart main class. Δ1 (argument list) is mart-specific.
- `vars_all.yml`, `spark_parameters` of every stand: `spark.yarn.appMasterEnv.KRB5CCNAME` and
  `spark.executorEnv.KRB5CCNAME` = that fixed name.

**15. AQE for SPARK_REBALANCE.** Publications and drafts use `PublisherMode.sparkRebalance()` (file layout only,
no effect on rows). It needs `spark.sql.adaptive.enabled=true` and
`spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled=true` in `spark_parameters`, otherwise the publisher
falls back to SALTING. Another mart: add both to every stand.

## Upgrading RT-FW

Startup will fail first in `StartupSelfCheck` (version mismatch). Before changing the pinned versions go through the
checklist in the `StartupSelfCheck` javadoc: re-copy `ProcessingStage` from the new vendor text with the three
`VSPC-COMPAT` blocks; same calculator classes and fields; same step order in `PublisherImpl.runIncr/runInit`;
re-check the vendor lines cited in compat comments; parity stand green.

## Parity stand

`local/README.md`: the old and new code run on the same synthetic sources, all targets compared row by row.
`./stand.sh build`, then `./stand.sh run <chain>` (or `run all`); result in `target/runs/<run>/summary.txt`.

## Do not

- Edit the vendor part of the shadow `ProcessingStage` or add logic there: only the three hooks.
- Remove the shade exclusion or the `StartupSelfCheck` call in `MainClass`.
- Publish past `publishChecked` (bypasses type alignment, empty marker, keepLegs, table check).
- Reorder columns in `CompatTables`: it changes the hash and produces false updates.
- Change pinned versions in `StartupSelfCheck` without the checklist above.
