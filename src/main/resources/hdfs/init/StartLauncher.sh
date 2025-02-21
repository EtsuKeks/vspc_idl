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

# for SK multi
user=${9}
krb5cc_file_name=krb5cc_${user}_$(uuidgen)
krb5cc_file_path=/tmp/${krb5cc_file_name}
export KRB5CCNAME=/tmp/${krb5cc_file_name}
#export RTFW_KERBEROS_TICKET_CACHE_FILE=${krb5cc_file_name}
export RTFW_KERBEROS_TICKET_CACHE_FILE=rtfw_kerberos_ticket_cache_file

hdfs dfs -get ${user_keytab_path} ${user_keytab_path_local}
kinit -kt ${user_keytab_path_local} ${user_principal}
kinit -R

# for SK multi
kvno_command=$(bash kvno_command.sh $KRB5CCNAME)
echo ---------------- kvno_command begin ----------------
echo ${kvno_command}
echo ----------------  kvno_command end  ----------------
$kvno_command
hadoopFileSystems=$(cat /etc/hadoop/conf/hdfs-site.xml | tr '\n' ' ' | sed -e 's/^.*dfs.nameservices<\/name>\s*<value>\([^<]*\)<.*/\1/' | tr ',' '\n' | sed -e 's/^\(.*\)$/hdfs:\/\/\1:8020/' | tr '\n' ',')
cp $krb5cc_file_path ./rtfw_kerberos_ticket_cache_file

klist

echo "hdfs dfs -get ${app_jar} ${app_jar_local}"
hdfs dfs -get ${app_jar} ${app_jar_local}

echo krb5cc_file_name : ${krb5cc_file_name}
echo hadoopFileSystems: ${hadoopFileSystems}
ls -lah

# вычисляем user_ticket_cache_file_name
user_ticket_cache_path=`klist |grep FILE: | awk -F: '{print $3}'`
user_ticket_cache_file_name=`ls ${user_ticket_cache_path} | awk -F/ '{print $3}'`
# копируем user_ticket_cache_file в папку запуска приложения
cp ${user_ticket_cache_path} .
# заполняем значение переменной RTFW_KERBEROS_TICKET_CACHE_FILE
echo user_ticket_cache_path : ${user_ticket_cache_path}
echo user_ticket_cache_file_name : ${user_ticket_cache_file_name}

echo "java -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false -classpath /usr/sdp/current/hadoop-hdfs-client/*:${app_jar_local} ru.sberbank.bigdata.dffess.vspc.MainClass --loading_id=${loading_id} --ctl_url=${ctl} --hadoop_file_systems=${hadoopFileSystems} --krb5cc_file_name=${krb5cc_file_name} --krb5ccname=${KRB5CCNAME}"
      java -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false -classpath /usr/sdp/current/hadoop-hdfs-client/*:${app_jar_local} ru.sberbank.bigdata.dffess.vspc.MainClass --loading_id=${loading_id} --ctl_url=${ctl} --hadoop_file_systems=${hadoopFileSystems} --krb5cc_file_name=${krb5cc_file_name} --krb5ccname=${KRB5CCNAME}
