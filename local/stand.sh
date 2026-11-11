#!/bin/bash
# Local 3.0 vs 4.0 parity stand for vspc_idl. One entry point; see README.md.
#
#   ./stand.sh build                       compile 3.0 (from $V30, read-only), the stand Java code, the patched clock
#   ./stand.sh freeze                      immutable build for a phase (4.0 + 3.0 + stand) -> $ESS/builds/<hash>; then
#        STAND_BUILD=<hash>/stand STAND_V40_CLASSES=<hash>/v40 ./stand.sh run ...  (or dispatcher submit --build <hash>)
#   ./stand.sh gen                         validate the case catalog, regenerate synth/CASES.md and CHAINS.md
#   ./stand.sh explain <case|chain>        source rows per load + expected 3.0 rows of one case (or all cases of a chain)
#   ./stand.sh run <chain>|all [options]   run a chain (or every chain in turn) on 3.0 and 4.0 in parallel, then compare
#        --only 30|40          one version only            --upto <load>        stop after this load
#        --v40-classes <dir>   4.0 classes (default $STAND_V40_CLASSES or $V40/target/classes)
#        --out <dir>           run directory (default target/runs/<chain>_<yyyymmdd_HHMMSS>)
#        --ports <p30>,<p40>   HDFS ports (default 19980,19981)   --keep   keep the roots (HDFS + metastore)
#        --step-timeout <sec>  kill a load that takes longer (default 2400)
#        --seeds <runDir>      reuse the generated seeds/expectations of another run (frozen catalog)
#   ./stand.sh compare <runDir> [--regen] [--mask table.col,...]   (re)compare a run directory -> summary.txt
#   ./stand.sh combine <newDir> <run30Dir> <run40Dir>              3.0 half of one run vs 4.0 half of another
#   ./stand.sh hdfsd <root> <port>                                  HDFS daemon of a root (foreground)
#   ./stand.sh apply <30|40> <root> <port> <chain> <load>          load the source state of one load into a root
#   ./stand.sh clean <root>                                         stop the root's java processes, delete the root
set -uo pipefail
ESS=/Users/timur/Desktop/ess
L="$(cd "$(dirname "$0")" && pwd)"
V40="$(cd "$L/.." && pwd)"                      # vspc_idl 4.0 (RT-FW): the repository around this directory
V30=$ESS/vspc_idl_old                      # vspc_idl 3.0 (ESSA): read-only reference
OUT=$L/target                                   # runs (git-ignored)
B="${STAND_BUILD:-$OUT}"                          # builds: cp30 cp40 vspc30 common classes30 classes40 clock (freeze: $ESS/builds/<hash>/stand)
export JAVA_HOME=$(ls -d $HOME/.local/jdk/jdk8*/Contents/Home | head -1)
JAVA=$JAVA_HOME/bin/java
MVN="mvn -B -q -s $ESS/.m2/settings.xml"
V40CLS="${STAND_V40_CLASSES:-$V40/target/classes}"

jc() { $JAVA_HOME/bin/javac -nowarn -encoding UTF-8 -source 8 -target 8 "$@" 2>&1 | grep -v '^Note' || true; }
gen() { python3 -I "$L/synth/gen.py" "$@"; }

# classpath of one version's JVM (3.0 also needs the root's core-site.xml/hive-site.xml first)
cp_of() {  # cp_of <30|40> <root>
  if [ "$1" = 30 ]; then echo "$2/conf:$B/classes30:$B/common:$B/vspc30:$(cat "$B/cp30")"
  else echo "$B/classes40:$B/common:$V40CLS:$(cat "$B/cp40")"; fi
}
jvm() {  # jvm <30|40> <root> <port> <Stand args...>; runs in <root> so derby.log etc. stay there
  local V=$1 R=$2 P=$3; shift 3
  mkdir -p "$R/conf"
  printf '<configuration><property><name>fs.defaultFS</name><value>hdfs://localhost:%s</value></property></configuration>\n' "$P" > "$R/conf/core-site.xml"
  cat > "$R/conf/hive-site.xml" <<X
<configuration>
<property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:derby:;databaseName=$R/metastore_db;create=true</value></property>
<property><name>hive.metastore.warehouse.dir</name><value>/warehouse</value></property>
<property><name>hive.exec.dynamic.partition.mode</name><value>nonstrict</value></property>
<property><name>hive.metastore.schema.verification</name><value>false</value></property>
<property><name>datanucleus.schema.autoCreateAll</name><value>true</value></property>
</configuration>
X
  local SP=""
  # 3.0: every ESSA app closes its SparkSession and the next one builds a new context from these system properties
  # (as with one spark-submit per app); Stand uses the same values for apply/export
  [ "$V" = 30 ] && SP="-Dspark.master=local[2] -Dspark.app.name=stand30 -Dspark.ui.enabled=false -Dspark.sql.warehouse.dir=/warehouse
 -Dspark.hadoop.fs.defaultFS=hdfs://localhost:$P -Dspark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$R/metastore_db;create=true
 -Dspark.sql.session.timeZone=Europe/Moscow -Dspark.sql.shuffle.partitions=4 -Dspark.default.parallelism=4
 -Dspark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED -Dspark.sql.legacy.parquet.int96RebaseModeInWrite=CORRECTED
 -Dspark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED -Dspark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED
 -Dspark.hadoop.metastore.catalog.default=spark -Dspark.sql.parquet.writeLegacyFormat=true"
  (cd "$R" && exec $JAVA -Xmx3g $SP -Duser.timezone=Europe/Moscow -Dstand.hdfs=$P -Dstand.params=$L/java/ctl40.properties \
     -Xbootclasspath/p:$B/clock ${STAND_JOPTS:-} -cp "$(cp_of $V "$R")" local.stand.Stand "$R" "$@")
}
start_hdfsd() {  # start_hdfsd <root> <port> <log>; prints the pid
  mkdir -p "$1"
  (cd "$1" && exec $JAVA -Xmx1g -Dstand.hdfs=$2 -cp "$B/common:$(cat "$B/cp40")" local.stand.Stand "$1" hdfsd) > "$3" 2>&1 &
  local pid=$! i
  for i in $(seq 1 120); do grep -q "up at" "$3" 2>/dev/null && break; kill -0 $pid 2>/dev/null || break; sleep 1; done
  grep -q "up at" "$3" || { echo "hdfsd of $1 did not start (see $3)" >&2; kill $pid 2>/dev/null; return 1; }
  echo $pid
}
stop_pid() { kill "$1" 2>/dev/null; wait "$1" 2>/dev/null; true; }

# ------------------------------------------------------------------------------------------------------------ build
cmd_build() {
  set -e
  mkdir -p "$B"
  echo "[build] classpaths (java/pom30.xml, java/pom40.xml)"
  $MVN -f "$L/java/pom30.xml" dependency:build-classpath -Dmdep.outputFile="$B/cp30"
  $MVN -f "$L/java/pom40.xml" dependency:build-classpath -Dmdep.outputFile="$B/cp40"
  echo "[build] vspc_idl 3.0 from $V30 -> target/vspc30"
  rm -rf "$B/vspc30" && mkdir -p "$B/vspc30"
  jc -cp "$(cat "$B/cp30")" -d "$B/vspc30" $(find "$V30/src/main/java" -name '*.java')
  cp -R "$V30/src/main/resources/." "$B/vspc30/"
  echo "[build] stand: java/common (driver, comparator, SK mock), java/v30 (Run30, CTL mock), java/v40 (CTL/core mocks)"
  rm -rf "$B/common" "$B/classes30" "$B/classes40" && mkdir -p "$B/common" "$B/classes30" "$B/classes40"
  jc -cp "$(cat "$B/cp40")" -d "$B/common" $(find "$L/java/common" -name '*.java')
  jc -cp "$B/common:$B/vspc30:$(cat "$B/cp30")" -d "$B/classes30" $(find "$L/java/v30" -name '*.java')
  jc -cp "$B/common:$V40CLS:$(cat "$B/cp40")" -d "$B/classes40" $(find "$L/java/v40" -name '*.java')
  echo "[build] business clock: java.time.Clock from the JDK sources + settable offset"
  rm -rf "$B/clock" && mkdir -p "$B/clock/src"
  (cd "$B/clock/src" && unzip -q -o "$JAVA_HOME/src.zip" java/time/Clock.java)
  python3 -I - "$B/clock/src/java/time/Clock.java" <<'PY'
import sys
p = sys.argv[1]; s = open(p, encoding="utf-8").read()
old = "        @Override\n        public long millis() {\n            return System.currentTimeMillis();"
assert s.count(old) == 1, "unexpected JDK Clock.java"
s = s.replace(old, "        // LOCAL STAND PATCH: business clock offset (ms), set by local.stand.Stand for the product run only;\n"
                   "        // java.time.* and Spark current_date/current_timestamp follow it, System.currentTimeMillis stays real.\n"
                   "        static volatile long STAND_OFFSET = 0L;\n"
                   "        @Override\n        public long millis() {\n            return System.currentTimeMillis() + STAND_OFFSET;")
open(p, "w", encoding="utf-8").write(s)
PY
  jc -d "$B/clock" "$B/clock/src/java/time/Clock.java"; rm -rf "$B/clock/src"
  for f in vspc30/ru/sberbank/bigdata/dffess/vspc/impl/VspcDataProductGenerator common/local/stand/Stand common/local/compare/Compare \
           classes30/local/stand/Run30 classes40/ru/sberbank/bigdata/sl/rt/core/StubCoordinator 'clock/java/time/Clock$SystemClock'; do
    [ -f "$B/$f.class" ] || { echo "[build] FAILED: $f.class missing"; exit 1; }
  done
  [ -d "$V40CLS/ru" ] || echo "[build] WARNING: no 4.0 classes in $V40CLS (build the 4.0 repository: mvn compile)"
  for d in vspc30 common classes30 classes40 clock; do echo "  $d: $(find "$B/$d" -name '*.class' | wc -l | tr -d ' ') classes"; done
  echo "[build] ok"
}

# ------------------------------------------------------------------------------------------------------------ freeze
# one immutable build for a whole phase: 4.0 (mvn compile of a copy of pom.xml+src) + 3.0 + stand -> $ESS/builds/<hash>
cmd_freeze() {
  set -e
  local T=$ESS/builds/.tmp_$$ H
  rm -rf "$T"; mkdir -p "$T/src40"
  trap "rm -rf '$T'" EXIT
  cp -R "$V40/pom.xml" "$V40/src" "$T/src40/"
  echo "[freeze] 4.0: mvn compile (copy of $V40)"
  $MVN -o -f "$T/src40/pom.xml" compile
  mv "$T/src40/target/classes" "$T/v40"
  (B=$T/stand; V40CLS=$T/v40; cmd_build)
  H=$(cd "$T" && find v40 stand -type f | LC_ALL=C sort | while read -r f; do printf '%s %s\n' "$f" "$(shasum -a 256 < "$f" | cut -c1-64)"; done \
      | shasum -a 256 | cut -c1-12)
  local SRC40 SRC30 STANDJ
  SRC40=$(cd "$T/src40" && find pom.xml src -type f | LC_ALL=C sort | xargs shasum -a 256 | shasum -a 256 | cut -c1-12)
  SRC30=$(cd "$V30" && find pom.xml src -type f | LC_ALL=C sort | xargs shasum -a 256 | shasum -a 256 | cut -c1-12)
  STANDJ=$(cd "$L/java" && find . -type f | LC_ALL=C sort | xargs shasum -a 256 | shasum -a 256 | cut -c1-12)
  printf '{"hash": "%s", "created": "%s", "v40_git_head": "%s", "v40_src_sha": "%s", "v30_src_sha": "%s", "stand_java_sha": "%s"}\n' \
    "$H" "$(date '+%F %T')" "$(git -C "$V40" rev-parse --short HEAD)" "$SRC40" "$SRC30" "$STANDJ" > "$T/BUILD.json"
  rm -rf "$T/src40"
  if [ -d "$ESS/builds/$H" ]; then echo "[freeze] identical build exists: $ESS/builds/$H"
  else mv "$T" "$ESS/builds/$H"; fi
  cat "$ESS/builds/$H/BUILD.json"
  echo "[freeze] use: STAND_BUILD=$ESS/builds/$H/stand STAND_V40_CLASSES=$ESS/builds/$H/v40 ./stand.sh run ...   (dispatcher: submit --build $H)"
}

# ------------------------------------------------------------------------------------------------------------ run
cmd_run() {
  local CHAIN=$1; shift
  RUN=""; local UPTO="" ONLY="" KEEP=0 PORTS="19980,19981" STEP_TIMEOUT=2400 SEEDS=""
  while [ $# -gt 0 ]; do
    case $1 in
      --upto) UPTO=$2; shift 2 ;; --only) ONLY=$2; shift 2 ;; --v40-classes) V40CLS=$2; shift 2 ;;
      --out) RUN=$2; shift 2 ;; --ports) PORTS=$2; shift 2 ;; --keep) KEEP=1; shift ;;
      --step-timeout) STEP_TIMEOUT=$2; shift 2 ;; --seeds) SEEDS=$2; shift 2 ;; *) echo "unknown option $1"; exit 2 ;;
    esac
  done
  V40CLS="$(cd "$V40CLS" && pwd)" || exit 2   # the JVMs start in their root: the path must be absolute
  RUN=${RUN:-$OUT/runs/${CHAIN}_$(date +%Y%m%d_%H%M%S)}
  mkdir -p "$RUN"; RUN="$(cd "$RUN" && pwd)"
  if [ -n "$SEEDS" ]; then
    cp -R "$SEEDS/synth" "$RUN/synth"
    [ -f "$RUN/synth/ddl.sql" ] || { gen --out "$RUN/gen.tmp" > /dev/null && cp "$RUN/gen.tmp/ddl.sql" "$RUN/synth/" && rm -rf "$RUN/gen.tmp"; }
  else gen --out "$RUN/synth" || exit 1; fi
  local TL=$RUN/synth/$CHAIN/timeline.tsv
  [ -f "$TL" ] || { echo "unknown chain $CHAIN (see synth/CHAINS.md)"; exit 2; }
  LOADS=$(awk -v upto="$UPTO" 'NR>1 { print $1; if ($1 == upto) exit }' "$TL")
  P30=${PORTS%,*}; P40=${PORTS#*,}
  # cutover chains: loads marked "30" in the timeline run on 3.0 only; the 4.0 half starts from a copy of the 3.0
  # root taken after the last such load, on the 3.0 port (table locations in the metastore include the port),
  # after the 3.0 half has finished
  TAKEOVER=$(awk -F'\t' 'NR>1 && $5=="30" {l=$1} END {print l}' "$TL")
  { echo "chain=$CHAIN loads=$(echo $LOADS) ports=$P30,$P40 only=${ONLY:-both}${TAKEOVER:+ 4.0 starts from the 3.0 state after $TAKEOVER}"
    echo "v40_classes=$V40CLS"; echo "build=$B"; echo "started=$(date '+%F %T')"; } > "$RUN/run.info"
  cat "$RUN/run.info"
  printf "version\tload\tstep\tseconds\trc\n" > "$RUN/timings.tsv"
  trap 'pkill -f "local.stand.Stand $RUN/r" 2>/dev/null; true' EXIT
  trap 'exit 130' INT TERM

  run_chain() {  # run_chain <V> <port>
    local V=$1 PORT=$2 ROOT=$RUN/r$1 LOGS=$RUN/logs/$1 HP ld mode ft id by t0 rc pid seeds
    mkdir -p "$ROOT" "$LOGS" "$RUN/exp$V"
    HP=$(start_hdfsd "$ROOT" "$PORT" "$LOGS/hdfsd.log") || { echo "FAILED hdfsd" > "$RUN/status$V"; return 1; }
    for ld in $LOADS; do
      IFS=$'\t' read -r mode ft id by < <(awk -F'\t' -v l="$ld" '$1==l {print $2"\t"$3"\t"$4"\t"$5}' "$TL")
      if [ "$V" = 40 ] && [ "$by" = 30 ]; then continue; fi
      seeds="$RUN/synth/$CHAIN/$ld.sql"
      [ -f "$ROOT/.ddl_done" ] || seeds="$RUN/synth/ddl.sql $seeds"
      t0=$(date +%s)
      jvm "$V" "$ROOT" "$PORT" load "$V" "$mode" "$id" "$ft" "$RUN/exp$V/$ld" $seeds > "$LOGS/$ld.log" 2>&1 & pid=$!
      ( t=0; while kill -0 $pid 2>/dev/null; do sleep 5; t=$((t + 5))
          if [ $t -ge "$STEP_TIMEOUT" ]; then echo "TIMEOUT after ${t}s" > "$LOGS/$ld.timeout"; pkill -P $pid; kill $pid; break; fi
        done ) &
      wait $pid; rc=$?
      printf "%s\t%s\tload\t%s\t%s\n" "$V" "$ld" $(( $(date +%s) - t0 )) "$rc" >> "$RUN/timings.tsv"
      echo "[$V] $ld $mode rc=$rc ($(( $(date +%s) - t0 ))s; $(grep -a '^\[stand\] \(run\|export\|done\)' "$LOGS/$ld.log" | awk '{printf "%s%s ", $2, $4}'))"
      if [ $rc != 0 ]; then
        echo "FAILED $ld" > "$RUN/status$V"; grep -a -E "Exception|ERROR" "$LOGS/$ld.log" | head -5 | cut -c1-300; break
      fi
      touch "$ROOT/.ddl_done"; echo "$ld" >> "$RUN/done$V"; echo "OK" > "$RUN/status$V"
      if [ "$V" = 30 ] && [ "$ld" = "$TAKEOVER" ] && [ "$ONLY" != 30 ]; then   # snapshot for the 4.0 half
        stop_pid "$HP"; rm -rf "$RUN/r40"; cp -R "$ROOT" "$RUN/r40"; rm -rf "$RUN/r40/conf"
        HP=$(start_hdfsd "$ROOT" "$PORT" "$LOGS/hdfsd.log") || { echo "FAILED hdfsd" > "$RUN/status$V"; return 1; }
      fi
    done
    stop_pid "$HP"
  }

  local T0=$(date +%s) PIDS=()
  if [ -n "$TAKEOVER" ]; then
    [ "$ONLY" = 40 ] && { echo "chain $CHAIN: the 4.0 half needs the 3.0 half of the same run"; exit 2; }
    run_chain 30 "$P30"
    if [ "$ONLY" != 30 ]; then
      if [ -d "$RUN/r40" ]; then run_chain 40 "$P30"; else echo "FAILED before takeover" > "$RUN/status40"; fi
    fi
  else
    for V in 30 40; do
      [ -n "$ONLY" ] && [ "$ONLY" != "$V" ] && continue
      if [ $V = 30 ]; then run_chain 30 "$P30" & else run_chain 40 "$P40" & fi
      PIDS+=($!)
    done
    wait "${PIDS[@]}"
  fi
  echo "[run] chain finished in $(( $(date +%s) - T0 ))s"
  cmd_compare "$RUN"; local RC=$?
  [ $KEEP = 1 ] || rm -rf "$RUN/r30" "$RUN/r40"
  echo "finished=$(date '+%F %T') total=$(( $(date +%s) - T0 ))s" >> "$RUN/run.info"
  echo "[run] results: $RUN/summary.txt"
  return $RC
}

# ------------------------------------------------------------------------------------------------------------ compare
cmd_compare() {
  local RUN MASK="" REGEN=0 CHAIN LOADS RC T0 RES
  RUN="$(cd "$1" && pwd)"; shift
  while [ $# -gt 0 ]; do case $1 in --regen) REGEN=1; shift ;; --mask) MASK="--mask $2"; shift 2 ;; *) echo "unknown $1"; exit 2 ;; esac; done
  if [ $REGEN = 1 ]; then gen --out "$RUN/synth" || return 1; fi
  CHAIN=$(sed -n 's/^chain=\([a-z0-9_]*\).*/\1/p' "$RUN/run.info")
  LOADS=$(awk 'NR>1 {print $1}' "$RUN/synth/$CHAIN/timeline.tsv" | while read -r l; do [ -d "$RUN/exp30/$l" ] || [ -d "$RUN/exp40/$l" ] && echo "$l"; done | paste -sd, -)
  [ -n "$LOADS" ] || { echo "[compare] no exports in $RUN"; return 1; }
  rm -rf "$RUN/cmp" "$RUN/dump"; T0=$(date +%s)
  (cd "$RUN" && $JAVA -Xmx3g -cp "$B/common:$(cat "$B/cp40")" local.compare.Compare "$RUN" "$CHAIN" "$LOADS" $MASK) > "$RUN/compare.log" 2>&1
  RC=$?
  grep '^\[compare\]' "$RUN/compare.log"
  case $RC in 0) RES="all compared, no differences (or ACCEPTED)" ;; 3) RES="differences (see per case)" ;;
    4) RES="no differences, but not everything was compared (NOT COMPARED / same*)" ;;
    *) echo "[compare] comparator crashed (rc=$RC), see $RUN/compare.log"; tail -20 "$RUN/compare.log"; return $RC ;; esac
  python3 -I "$L/synth/check.py" "$RUN" > /dev/null
  {
    cat "$RUN/run.info"; echo
    echo "== result: rc=$RC - $RES"; echo
    echo "== status"
    for V in 30 40; do
      [ -f "$RUN/status$V" ] || continue
      echo "  $V: $(cat "$RUN/status$V"); loads done: $(tr '\n' ' ' < "$RUN/done$V" 2>/dev/null)"
      set -- $(cat "$RUN/status$V")
      if [ "$1" = FAILED ] && [ -f "$RUN/logs/$V/$2.log" ]; then
        if [ -f "$RUN/logs/$V/$2.timeout" ]; then echo "     reason: $(cat "$RUN/logs/$V/$2.timeout") (killed by --step-timeout: the load hung)"
        else
          echo "     reason: $(grep -a -m1 'CTL report' "$RUN/logs/$V/$2.log" | cut -c1-300)"
          echo "             $(grep -a -m1 -E 'TransformationException|Exception:' "$RUN/logs/$V/$2.log" | cut -c1-200)"
        fi
      fi
    done
    echo; echo "== timings (seconds per load: apply + run + export in one JVM)"
    awk -F'\t' 'NR>1 { printf "  %s %-4s %4ds rc=%s\n", $1, $2, $4, $5; s[$1]+=$4 } END { for (v in s) printf "  total %s %5ds\n", v, s[v] }' "$RUN/timings.tsv"
    echo "  compare+check: $(( $(date +%s) - T0 ))s"
    echo; echo "== 3.0 vs 4.0 per load (tables that differ; ctl_validfrom compared by class)"
    awk -F'\t' 'NR>1 { tot[$1]++; if ($7=="DIFF") { bad[$1]++; d[$1]=d[$1]" "$2"("$5"/"$6")" } else if ($7=="NOT_COMPARED") { nc[$1]++; n[$1]=n[$1]" "$2 } }
                END { for (l in tot) printf "  %s  %s%s\n", l, (nc[l]==tot[l]) ? "not compared (one side missing)" : (bad[l] ? bad[l]" differ:"d[l] : (tot[l]-nc[l])"/"(tot[l]-nc[l])" compared match"),
                      (nc[l] && nc[l]<tot[l]) ? "; NOT COMPARED:"n[l] : "" }' "$RUN/cmp/tables.tsv" | sort
    echo "  (only30/only40 = rows present on one side after exceptAll; per row: cmp/<load>.txt, cmp/mismatches.tsv)"
    echo; echo "== stg.chg_div_h schema after each load"
    for d in "$RUN"/exp30/*/ "$RUN"/exp40/*/; do [ -f "$d/stg.chg_div_h.schema" ] || continue
      ld=$(basename "$d"); o="$RUN/exp30/$ld/stg.chg_div_h.schema"; [ "${d#$RUN/exp30}" != "$d" ] && continue
      if [ -f "$o" ]; then cmp -s "$o" "$d/stg.chg_div_h.schema" && echo "  $ld same" || { echo "  $ld DIFFERENT:"; diff "$o" "$d/stg.chg_div_h.schema" | sed 's/^/    /'; }; fi
    done
    for d in "$RUN"/exp30/*/; do [ -f "$d/stg.chg_div_h.schema" ] && echo "  3.0 $(basename "$d"): $(grep -o 'start_dt:[a-z]*' "$d/stg.chg_div_h.schema")"; done
    echo; echo "== 3.0 vs 4.0 per case"; cat "$RUN/cmp/cases.txt"
    echo; echo "== catalog expectations (details: expect_check.txt)"; sed -n '3,/^PASS n/p' "$RUN/expect_check.txt"
  } > "$RUN/summary.txt"
  cat "$RUN/summary.txt"
  return $RC
}

cmd_combine() {
  local NEW=$1 R30 R40 C30 C40 f
  R30="$(cd "$2" && pwd)"; R40="$(cd "$3" && pwd)"
  C30=$(sed -n 's/^chain=\([a-z0-9_]*\).*/\1/p' "$R30/run.info"); C40=$(sed -n 's/^chain=\([a-z0-9_]*\).*/\1/p' "$R40/run.info")
  [ "$C30" = "$C40" ] || { echo "[combine] different chains: $C30 vs $C40"; exit 2; }
  diff -rq "$R30/synth/$C30" "$R40/synth/$C40" --exclude=keymap.tsv --exclude=cases.tsv > /dev/null \
    || { echo "[combine] the two runs used different seeds (diff -r $R30/synth/$C30 $R40/synth/$C40)"; exit 2; }
  mkdir -p "$NEW/logs"; NEW="$(cd "$NEW" && pwd)"
  ln -sfn "$R30/exp30" "$NEW/exp30"; ln -sfn "$R40/exp40" "$NEW/exp40"; ln -sfn "$R30/logs/30" "$NEW/logs/30"; ln -sfn "$R40/logs/40" "$NEW/logs/40"
  rm -rf "$NEW/synth"; cp -R "$R30/synth" "$NEW/synth"
  for f in status30 done30; do [ -f "$R30/$f" ] && cp "$R30/$f" "$NEW/$f"; done
  for f in status40 done40; do [ -f "$R40/$f" ] && cp "$R40/$f" "$NEW/$f"; done
  { head -1 "$R30/timings.tsv"; awk 'NR>1 && $1==30' "$R30/timings.tsv"; awk 'NR>1 && $1==40' "$R40/timings.tsv"; } > "$NEW/timings.tsv"
  { echo "chain=$C30 (combined: 3.0 from $R30, 4.0 from $R40)"; grep '^v40_classes=' "$R40/run.info"; } > "$NEW/run.info"
  cmd_compare "$NEW"
}

# ------------------------------------------------------------------------------------------------------------ main
main() {
CMD=${1:-help}; shift || true
case $CMD in
  build) cmd_build ;;
  freeze) cmd_freeze ;;
  gen) gen --md ;;
  explain) gen --explain "$1" ;;
  run) [ $# -ge 1 ] || { sed -n '2,20p' "$0"; exit 2; }
       if [ "$1" = all ]; then   # every chain, one after the other; overview in target/runs/all_<time>.txt
         shift; local A=$OUT/runs/all_$(date +%Y%m%d_%H%M%S).txt c
         mkdir -p "$OUT/runs"
         for c in $(gen --chains); do
           echo "$(date +%T) $c" | tee -a "$A"
           ( cmd_run "$c" "$@" > "$OUT/runs/$c.last.log" 2>&1 ); echo "  rc=$? $(grep -m1 '^\[run\] results' "$OUT/runs/$c.last.log")" | tee -a "$A"
         done
       else cmd_run "$@"; fi ;;
  compare) cmd_compare "$@" ;;
  combine) cmd_combine "$@" ;;
  hdfsd) mkdir -p "$1"; R="$(cd "$1" && pwd)"; cd "$R" && exec $JAVA -Xmx1g -Dstand.hdfs=$2 -cp "$B/common:$(cat "$B/cp40")" local.stand.Stand "$R" hdfsd ;;
  apply) mkdir -p "$2"; R="$(cd "$2" && pwd)"; gen --out "$OUT/synth" > /dev/null
         jvm "$1" "$R" "$3" sql "$OUT/synth/ddl_schemas.sql" "$OUT/synth/$4/$5.sql" ;;
  clean) [ -e "$1" ] || exit 0; R="$(cd "$1" && pwd)"
         case "$R/" in "$OUT/"?*) ;; *) echo "refusing to delete $R (not under $OUT/)"; exit 2 ;; esac
         pkill -f "local.stand.Stand $R " 2>/dev/null; sleep 1; rm -rf "$R"; echo "[clean] removed $R" ;;
  *) sed -n '2,20p' "$0" ;;
esac
}
# one line, read completely before it runs: editing this file during a run cannot inject commands
main "$@"; exit $?
