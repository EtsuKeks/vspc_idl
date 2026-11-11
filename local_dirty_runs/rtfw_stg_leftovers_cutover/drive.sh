#!/bin/bash
# drive.sh <name> <with_seeds 1|0> <only: both|30>: acquire a slot, run chain dg1, release
D=/Users/timur/Desktop/ess/vspc_idl/local_dirty_runs/rtfw_stg_leftovers_cutover
SL=/Users/timur/Desktop/ess/vspc_idl/local_dirty_runs/.slots
NAME=$1; SEED=$2; ONLY=$3
while :; do
  for s in s1 s2 s3; do mkdir $SL/$s 2>/dev/null && { SLOT=$s; break 2; }; done
  sleep 30
done
case $SLOT in s1) PORTS=19980,19981;; s2) PORTS=19982,19983;; s3) PORTS=19984,19985;; esac
OUTD=$D/target/runs/$NAME
trap 'pkill -f "local.stand.Stand $OUTD/r" 2>/dev/null; rmdir $SL/$SLOT; echo "released $SLOT at $(date +%T)"' EXIT
echo "slot $SLOT ports $PORTS at $(date '+%F %T')"
export PROBE=$D/presql/probe.sql
[ "$SEED" = 1 ] && export PRESQL_DIR=$D/presql || unset PRESQL_DIR
OPT=""; [ "$ONLY" = 30 ] && OPT="--only 30"
$D/stand.sh run dg1 --v40-classes /Users/timur/Desktop/ess/vspc_idl/local_dirty/target/v40_current_classes --ports $PORTS --out $OUTD $OPT
RC=$?
pkill -f "local.stand.Stand $OUTD/r" 2>/dev/null; pkill -f "local.stand.Ls $(echo ${PORTS%,*})" 2>/dev/null
sleep 2
echo "run rc=$RC"
