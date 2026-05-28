#!/bin/bash
loading_id=${1}
user_principal=${2}
ctl=${3}
user_keytab_path=${4}
app_jar=${5}${6}/${7}
devops_distributive_jar_local=${7}
user_keytab_path_local=tmp.keytab

export SPARK_HOME=/usr/sdp/current/spark3-client

hdfs dfs -get ${user_keytab_path} ${user_keytab_path_local}
kinit -kt ${user_keytab_path_local} ${user_principal}
kinit -R

klist

#получаем ссылку на кеш тикета, который в дальнейшем будет передаваться в драйвер.
user_ticket_cache_path=`klist |grep FILE: | awk -F: '{print $3}'`
user_ticket_cache_file_name=`ls ${user_ticket_cache_path} | awk -F/ '{print $3}'`
echo $? user_ticket_cache_path=$user_ticket_cache_path

export KRB5CCNAME=${user_ticket_cache_file_name}
echo $? KRB5CCNAME=$KRB5CCNAME
export RTFW_KERBEROS_TICKET_CACHE_FILE=${user_ticket_cache_file_name}

cp ${user_ticket_cache_path} .

#удаляем кейтаб файл
rm ${user_keytab_path_local}

# for SK multi
hdfs dfs -get /oozie-app/t/idl/vspc_idl/init/kvno_command.sh
kvno_command=$(bash kvno_command.sh $KRB5CCNAME)
echo ---------------- kvno_command begin ----------------
$kvno_command
echo $? kvno_result

echo "start_ls"
pwd
ls -lah
echo "end_ls"

#вызываем запуск расчёта дата-продукта с двумя аргументами - loading_id, CTL URL
if false; then
  java -Xmx2G -XX:MetaspaceSize=100M \
  -classpath /usr/sdp/current/spark3-client/jars/*:${devops_distributive_jar_local} \
  ru.sberbank.bigdata.dffess.vspc.MainClassSpawnedApp ${loading_id} ${ctl}
else
  spark-submit --files ${user_ticket_cache_file_name} --master yarn --deploy-mode cluster --queue vspc \
  --name "loading_id=${loading_id}:wf_name=usl_vspc_idl:pu=all" --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.driver.extraJavaOptions='-Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false' \
  --conf spark.executor.extraJavaOptions='-Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false' \
  --conf spark.executorEnv.KRB5CCNAME=${user_ticket_cache_file_name} \
  --conf spark.yarn.appMasterEnv.RTFW_KERBEROS_TICKET_CACHE_FILE=${user_ticket_cache_file_name} \
  --conf spark.yarn.appMasterEnv.KRB5CCNAME=${user_ticket_cache_file_name} --driver-memory 12G \
  --conf 'spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/etc/zookeeper/conf/zookeeper_client_jaas.conf -Dzookeeper.sasl.client=true -Dzookeeper.sasl.clientconfig=Client' \
  --conf 'spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/etc/zookeeper/conf/zookeeper_client_jaas.conf -Dzookeeper.sasl.client=true -Dzookeeper.sasl.clientconfig=Client' \
  --class ru.sberbank.bigdata.dffess.vspc.MainClass ${devops_distributive_jar_local} ${loading_id} ${ctl}
fi