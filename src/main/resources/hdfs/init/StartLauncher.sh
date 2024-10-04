#!/bin/bash

user_principal=${2}
app_jar=${5}${6}/${7}
app_jar_local=${7}
user_keytab_path=${4}
user_keytab_path_local=tmp.keytab
loading_id=${1}
#ctl=${3}
ctl=${8}

umask 022
export HADOOP_CONF_DIR=/etc/hadoop/conf

hdfs dfs -get ${user_keytab_path} ${user_keytab_path_local}
kinit -kt ${user_keytab_path_local} ${user_principal}
kinit -R

echo "hdfs dfs -get ${app_jar} ${app_jar_local}"
hdfs dfs -get ${app_jar} ${app_jar_local}

# вычисляем user_ticket_cache_file_name
user_ticket_cache_path=`klist |grep FILE: | awk -F: '{print $3}'`
user_ticket_cache_file_name=`ls ${user_ticket_cache_path} | awk -F/ '{print $3}'`
# копируем user_ticket_cache_file в папку запуска приложения
cp ${user_ticket_cache_path} .
# заполняем значение переменной RTFW_KERBEROS_TICKET_CACHE_FILE
export RTFW_KERBEROS_TICKET_CACHE_FILE=${user_ticket_cache_file_name}
echo user_ticket_cache_path : ${user_ticket_cache_path}
echo user_ticket_cache_file_name : ${user_ticket_cache_file_name}

echo "java -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false -classpath /usr/sdp/current/hadoop-hdfs-client/*:${app_jar_local}  ru.sberbank.bigdata.dffess.vspc.MainClass --loading_id=${loading_id} --user_keytab_path_local=${user_keytab_path_local} --base_jar_path=. --ctl_url=${ctl}"
java -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false -classpath /usr/sdp/current/hadoop-hdfs-client/*:${app_jar_local} ru.sberbank.bigdata.dffess.vspc.MainClass --loading_id=${loading_id} --user_keytab_path_local=${user_keytab_path_local} --base_jar_path=. --ctl_url=${ctl}