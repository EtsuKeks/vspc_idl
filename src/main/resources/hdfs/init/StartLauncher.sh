#!/bin/bash

loading_id=${1}
user_principal=${2}
ctl=${3}
app_jar=${5}${6}/${7}
app_jar_local=${7}
user_keytab_path=${4}
user_keytab_path_local=tmp.keytab
user=${8}

umask 022
export HADOOP_CONF_DIR=/etc/hadoop/conf

krb5cc_file_name=krb5cc_${user}_$(uuidgen)
krb5cc_file_path=/tmp/${krb5cc_file_name}
export KRB5CCNAME=/tmp/${krb5cc_file_name}

hdfs dfs -get ${user_keytab_path} ${user_keytab_path_local}
kinit -kt ${user_keytab_path_local} ${user_principal} || kinit -kt ${user_keytab_path_local} ${user_principal} || kinit -kt ${user_keytab_path_local} ${user_principal}

# for SK multi
kvno_command=$(bash kvno_command.sh $KRB5CCNAME)
echo ---------------- kvno_command begin ----------------
echo ${kvno_command}
echo ----------------  kvno_command end  ----------------
$kvno_command
hadoopFileSystems=$(cat /etc/hadoop/conf/hdfs-site.xml | tr '\n' ' ' | sed -e 's/^.*dfs.nameservices<\/name>\s*<value>\([^<]*\)<.*/\1/' | tr ',' '\n' | sed -e 's/^\(.*\)$/hdfs:\/\/\1:8020/' | tr '\n' ',')

cp $krb5cc_file_path ./
cp $krb5cc_file_path ./rtfw_kerberos_ticket_cache_file

klist

echo "hdfs dfs -get ${app_jar} ${app_jar_local}"
hdfs dfs -get ${app_jar} ${app_jar_local}

echo krb5cc_file_name : ${krb5cc_file_name}
echo krb5cc_file_path : ${krb5cc_file_path}
echo hadoopFileSystems: ${hadoopFileSystems}

export RTFW_KERBEROS_TICKET_CACHE_FILE=rtfw_kerberos_ticket_cache_file

echo "start_ls"
ls -lah
echo "end_ls"

echo "java -Djava.security.krb5.conf=/etc/krb5.conf -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.jgss.debug=false -classpath /usr/sdp/current/hadoop-hdfs-client/*:${app_jar_local} ru.sberbank.bigdata.dffess.vspc.MainClass --loading_id=${loading_id} --ctl_url=${ctl} --hadoop_file_systems=${hadoopFileSystems} --krb5cc_file_name=${krb5cc_file_name} --krb5ccname=${krb5cc_file_name}"
java -Djava.security.krb5.conf=/etc/krb5.conf -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.jgss.debug=false -classpath /usr/sdp/current/hadoop-hdfs-client/*:${app_jar_local} ru.sberbank.bigdata.dffess.vspc.MainClass --loading_id=${loading_id} --ctl_url=${ctl} --hadoop_file_systems=${hadoopFileSystems} --krb5cc_file_name=${krb5cc_file_name} --krb5ccname=${krb5cc_file_name}