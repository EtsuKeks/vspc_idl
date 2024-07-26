#!/bin/bash
loading_id=${1}
user_principal=${2}
ctl=${3}
user_keytab_path=${4}
dm_builder_jar_local=${5}
yarn_queue=${6}
user_keytab_path_local=tmp.keytab

hdfs dfs -get ${user_keytab_path} ${user_keytab_path_local}
kinit -kt ${user_keytab_path_local} ${user_principal}
kinit -R

user_ticket_cache_path=`klist |grep FILE: | awk -F: '{print $3}'`
user_ticket_cache_file_name=`ls ${user_ticket_cache_path} | awk -F/ '{print $3}'`
cp ${user_ticket_cache_path} ./

echo "spark-submit --files ${user_ticket_cache_file_name} --master yarn --deploy-mode cluster --queue ${yarn_queue} --conf spark.yarn.appMasterEnv.RTFW_KERBEROS_TICKET_CACHE_FILE=${user_ticket_cache_file_name} --conf spark.yarn.maxAppAttempts=1 --driver-memory 12G --class ru.sberbank.bigdata.dffess.rt.Main ${dm_builder_jar_local} ${loading_id} ${ctl}"
spark-submit --files ${user_ticket_cache_file_name} --master yarn --deploy-mode cluster --queue ${yarn_queue} --conf spark.yarn.appMasterEnv.RTFW_KERBEROS_TICKET_CACHE_FILE=${user_ticket_cache_file_name} --conf spark.yarn.maxAppAttempts=1 --driver-memory 12G --class Main ${dm_builder_jar_local} ${loading_id} ${ctl}
spark_submit_status=$?
echo Spark-приложение завершено со статусом $spark_submit_status
exit $spark_submit_status