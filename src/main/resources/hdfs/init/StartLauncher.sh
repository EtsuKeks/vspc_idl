#!/bin/bash

user_principal=${2}
dm_builder_jar=${5}${6}/${7}
dm_builder_jar_local=${7}
user_keytab_path=${4}
user_keytab_path_local=tmp.keytab
loading_id=${1}
ctl=${3}
launcher_list=${8}
parallel_threads=${9}
logical_env=${10}
debug_sk_off=${11}
entity_id=${12}

umask 022
export HADOOP_CONF_DIR=/etc/hadoop/conf

hdfs dfs -get ${user_keytab_path} ${user_keytab_path_local} 
kinit -kt ${user_keytab_path_local} ${user_principal}
kinit -R

echo "hdfs dfs -get ${dm_builder_jar} ${dm_builder_jar_local}"
hdfs dfs -get ${dm_builder_jar} ${dm_builder_jar_local}

timestamp=$(date +%Y-%m-%dT%H:%M:%S)

echo "curl -d '{"loading_id": '$loading_id', "entity_id": '$entity_id', "stat_id": 12, "avalue": [ '$timestamp' ]}' -H "Content-Type: application/json" -X POST $ctl/v1/api/statval/m"

echo "Start write statistic to ctl"

curl -d '{"loading_id": '$loading_id', "entity_id": '$entity_id', "stat_id": 12, "avalue": [ "'$timestamp'" ]}' -H "Content-Type: application/json" -X POST $ctl/v1/api/statval/m

echo "End write statistic to ctl"

echo "java -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false -classpath $CLASSPATH:/usr/sdp/current/hive-metastore/lib/*:${dm_builder_jar_local} ru.sberbank.bigdata.dffess.vspc.MainClass --loading_id=${loading_id} --user_keytab_path_local=${user_keytab_path_local} --base_jar_path=. --ctl_url=${ctl} --launcher_list=${launcher_list}  --repair_mode=${repair_mode} --type=arc  --sk_mode=${sk_mode}"
java -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false -classpath $CLASSPATH:/usr/sdp/current/hive-metastore/lib/*:${dm_builder_jar_local} ru.sberbank.bigdata.dffess.vspc.MainClass --loading_id=${loading_id} --user_keytab_path_local=${user_keytab_path_local} --base_jar_path=. --ctl_url=${ctl} --launcher_list=${launcher_list}  --type=arc --logical_env=${logical_env} --debug_sk_off=${debug_sk_off} --parallel_threads=${parallel_threads}