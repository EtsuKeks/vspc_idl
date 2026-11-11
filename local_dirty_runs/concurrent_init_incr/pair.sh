#!/bin/bash
# concurrent_init_incr driver (3.0 RunPair / 4.0 sequential).  Usage:
#   ./pair.sh p0  <V> <root> <port>                         INIT A0 (dsrc) on a fresh root, export exp/<root>/P0
#   ./pair.sh sched <name> <p0root> <port> <schedule>       copy p0root -> R/<name>, pair load (3.0 RunPair) at A1 source, then follow-up INCR (A2 source)
#   ./pair.sh seq40 <name> <p0root> <port>                  4.0: INIT 14000 then INCR 14001 (A1 source), then INCR 14002 (A2)
set -uo pipefail
L="$(cd "$(dirname "$0")" && pwd)"; OUT=$L/target; R=$L/R; S=$L/R/synth
export JAVA_HOME=$(ls -d $HOME/.local/jdk/jdk8*/Contents/Home | head -1); JAVA=$JAVA_HOME/bin/java
V40CLS=/Users/timur/Desktop/ess/vspc_idl/local_dirty/target/v40_current_classes
cp_of() { if [ "$1" = 30 ]; then echo "$2/conf:$OUT/classes30:$OUT/common:$OUT/vspc30:$(cat "$OUT/cp30")"; else echo "$OUT/classes40:$OUT/common:$V40CLS:$(cat "$OUT/cp40")"; fi; }
jvm() {  # jvm <30|40> <root> <port> <mainclass> <args...>
  local V=$1 RT=$2 P=$3 M=$4; shift 4
  mkdir -p "$RT/conf"
  printf '<configuration><property><name>fs.defaultFS</name><value>hdfs://localhost:%s</value></property></configuration>\n' "$P" > "$RT/conf/core-site.xml"
  cat > "$RT/conf/hive-site.xml" <<X
<configuration>
<property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:derby:;databaseName=$RT/metastore_db;create=true</value></property>
<property><name>hive.metastore.warehouse.dir</name><value>/warehouse</value></property>
<property><name>hive.exec.dynamic.partition.mode</name><value>nonstrict</value></property>
<property><name>hive.metastore.schema.verification</name><value>false</value></property>
<property><name>datanucleus.schema.autoCreateAll</name><value>true</value></property>
</configuration>
X
  local SP=""
  [ "$V" = 30 ] && SP="-Dspark.master=local[2] -Dspark.app.name=stand30 -Dspark.ui.enabled=false -Dspark.sql.warehouse.dir=/warehouse
 -Dspark.hadoop.fs.defaultFS=hdfs://localhost:$P -Dspark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$RT/metastore_db;create=true
 -Dspark.sql.session.timeZone=Europe/Moscow -Dspark.sql.shuffle.partitions=4 -Dspark.default.parallelism=4
 -Dspark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED -Dspark.sql.legacy.parquet.int96RebaseModeInWrite=CORRECTED
 -Dspark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED -Dspark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED
 -Dspark.hadoop.metastore.catalog.default=spark -Dspark.sql.parquet.writeLegacyFormat=true"
  (cd "$RT" && exec $JAVA -Xmx3g $SP -Duser.timezone=Europe/Moscow -Dstand.hdfs=$P -Dstand.params=$L/java/ctl40.properties \
     -Xbootclasspath/p:$OUT/clock -cp "$(cp_of $V "$RT")" $M "$RT" "$@")
}
hdfsd() {  # hdfsd <root> <port> <log> -> pid
  (cd "$1" && exec $JAVA -Xmx1g -Dstand.hdfs=$2 -cp "$OUT/common:$(cat "$OUT/cp40")" local.stand.Stand "$1" hdfsd) > "$3" 2>&1 &
  local pid=$! i; for i in $(seq 1 120); do grep -q "up at" "$3" 2>/dev/null && break; sleep 1; done
  grep -q "up at" "$3" || { echo "hdfsd failed $3" >&2; kill $pid; return 1; }; echo $pid
}
[ -d "$S" ] || { mkdir -p "$R"; python3 -I "$L/synth/gen.py" --out "$S" > /dev/null; }
case $1 in
  p0) V=$2; RT=$R/$3; P=$4; mkdir -p "$RT" "$R/exp/$3" "$R/logs"
      HP=$(hdfsd "$RT" "$P" "$R/logs/$3.hdfsd.log") || exit 1
      jvm $V "$RT" "$P" local.stand.Stand load $V INIT 11000 "2027-01-13 10:00:00" "$R/exp/$3/P0" "$S/ddl.sql" "$S/dsrc/A0.sql" > "$R/logs/$3.P0.log" 2>&1; echo "p0 $3 rc=$?"
      kill $HP; wait $HP 2>/dev/null ;;
  sched) N=$2; P0=$R/$3; P=$4; SCH=$5; RT=$R/$N; rm -rf "$RT"; cp -R "$P0" "$RT"; rm -rf "$RT/conf"; mkdir -p "$R/exp/$N"
      HP=$(hdfsd "$RT" "$P" "$R/logs/$N.hdfsd.log") || exit 1
      jvm 30 "$RT" "$P" local.stand.RunPair "$SCH" 14000 14001 "2027-01-14 10:00:00" "$R/exp/$N/PN" "$S/dsrc/A1.sql" > "$R/logs/$N.PN.log" 2>&1; echo "$N pair rc=$?"
      jvm 30 "$RT" "$P" local.stand.Stand load 30 INCR 14002 "2027-01-14 16:00:00" "$R/exp/$N/F" "$S/dsrc/A2.sql" > "$R/logs/$N.F.log" 2>&1; echo "$N follow-up rc=$?"
      kill $HP; wait $HP 2>/dev/null ;;
  seq40) N=$2; P0=$R/$3; P=$4; RT=$R/$N; rm -rf "$RT"; cp -R "$P0" "$RT"; rm -rf "$RT/conf"; mkdir -p "$R/exp/$N"
      HP=$(hdfsd "$RT" "$P" "$R/logs/$N.hdfsd.log") || exit 1
      jvm 40 "$RT" "$P" local.stand.Stand load 40 INIT 14000 "2027-01-14 10:00:00" "$R/exp/$N/PI" "$S/dsrc/A1.sql" > "$R/logs/$N.PI.log" 2>&1; echo "$N INIT rc=$?"
      jvm 40 "$RT" "$P" local.stand.Stand load 40 INCR 14001 "2027-01-14 10:00:00" "$R/exp/$N/PN" > "$R/logs/$N.PN.log" 2>&1; echo "$N INCR rc=$?"
      jvm 40 "$RT" "$P" local.stand.Stand load 40 INCR 14002 "2027-01-14 16:00:00" "$R/exp/$N/F" "$S/dsrc/A2.sql" > "$R/logs/$N.F.log" 2>&1; echo "$N follow-up rc=$?"
      kill $HP; wait $HP 2>/dev/null ;;
esac
