#!/bin/bash
# essa_retry_rerun driver: acquire a slot; batch1 = F3 (relaunch) 3.0 || F1 (div_h retry) 3.0;
# batch2 = F3 4.0 || F2a (mover retry) 3.0; release.
D=/Users/timur/Desktop/ess/vspc_idl/local_dirty_runs/essa_retry_rerun
SL=/Users/timur/Desktop/ess/vspc_idl/local_dirty_runs/.slots
V40=/Users/timur/Desktop/ess/vspc_idl/local_dirty/target/v40_current_classes
SLOT=""
while [ -z "$SLOT" ]; do
  for s in s1 s2 s3; do if mkdir $SL/$s 2>/dev/null; then SLOT=$s; break; fi; done
  [ -z "$SLOT" ] && sleep 30
done
case $SLOT in s1) PA=19980; PB=19981;; s2) PA=19982; PB=19983;; s3) PA=19984; PB=19985;; esac
echo "$(date +%T) slot $SLOT ports $PA,$PB"
cleanup() { pkill -f "local.stand.Stand $D/target/runs" 2>/dev/null; sleep 2; [ -n "$SLOT" ] && rmdir $SL/$SLOT; echo "$(date +%T) released $SLOT"; }
trap cleanup EXIT
trap 'exit 130' INT TERM
R=$D/target/runs
cd $D
echo "$(date +%T) batch1: F3 3.0 || F1 3.0"
./stand.sh run dretry --only 30 --v40-classes $V40 --ports $PA,$PB --out $R/F3_30 > $R.F3_30.log 2>&1 &
p1=$!
STAND_JOPTS="-Dstand.fail=div_h:after:1:13101" ./stand.sh run dretryc --only 30 --ports $PB,$PA --v40-classes $V40 --out $R/F1 > $R.F1.log 2>&1 &
p2=$!
wait $p1; echo "$(date +%T) F3_30 rc=$?"; wait $p2; echo "$(date +%T) F1 rc=$?"
echo "$(date +%T) batch2: F3 4.0 || F2a 3.0"
./stand.sh run dretry --only 40 --seeds $R/F3_30 --v40-classes $V40 --ports $PA,$PB --out $R/F3_40 > $R.F3_40.log 2>&1 &
p1=$!
STAND_JOPTS="-Dstand.fail=kit_move_vspc_idl:after:1:13101 -Dstand.continueOnFail=true" ./stand.sh run dretryc --only 30 --ports $PB,$PA --v40-classes $V40 --out $R/F2a > $R.F2a.log 2>&1 &
p2=$!
wait $p1; echo "$(date +%T) F3_40 rc=$?"; wait $p2; echo "$(date +%T) F2a rc=$?"
./stand.sh combine $R/F3 $R/F3_30 $R/F3_40 > $R.F3.combine.log 2>&1; echo "$(date +%T) combine rc=$?"
echo "$(date +%T) all done"
