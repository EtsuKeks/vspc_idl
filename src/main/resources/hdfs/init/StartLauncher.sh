#!/bin/bash
echo begin StartLauncher.sh
umask 022

user_principal=${2}
app_jar=${5}${6}/${7}
app_jar_local=${7}
user_keytab_path=${4}
user_keytab_path_local=tmp.keytab
loading_id=${1}
ctl_secure=${3}

export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_OPTS="$HADOOP_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"
export YARN_OPTS="$YARN_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"
export HBASE_OPTS="$HBASE_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"
export JAVA_OPTS="$JAVA_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"

# for SK multi
user=${8}
krb5cc_file_name=krb5cc_${user}_$(uuidgen)
export KRB5CCNAME=/tmp/${krb5cc_file_name}
export RTFW_KERBEROS_TICKET_CACHE_FILE=rtfw_kerberos_ticket_cache_file

hdfs dfs -get ${user_keytab_path} ${user_keytab_path_local}
kinit_command="kinit -kt ${user_keytab_path_local} ${user_principal}"
echo kinit_command : ${kinit_command}
eval "$kinit_command" || eval "$kinit_command" || eval "$kinit_command"

# for SK multi
echo "bash kvno_command.sh "${KRB5CCNAME}" "$kinit_command" "$user" "" "$sl_secman_host" "$ssk_tgt_tgs_renew_h""
bash kvno_command.sh "${KRB5CCNAME}" "$kinit_command" "$user" "" "$sl_secman_host" "$ssk_tgt_tgs_renew_h"

hadoopFileSystems=$(cat /etc/hadoop/conf/hdfs-site.xml | tr '\n' ' ' | sed -e 's/^.*dfs.nameservices<\/name>\s*<value>\([^<]*\)<.*/\1/' | tr ',' '\n' | sed -e 's/^\(.*\)$/hdfs:\/\/\1:8020/' | tr '\n' ',')
cp ${KRB5CCNAME} .
cp ${KRB5CCNAME} ./rtfw_kerberos_ticket_cache_file

klist -c ${KRB5CCNAME}

echo "hdfs dfs -get ${app_jar} ${app_jar_local}"
hdfs dfs -get ${app_jar} ${app_jar_local}

echo KRB5CCNAME : ${KRB5CCNAME}
echo hadoopFileSystems: ${hadoopFileSystems}
ls -lah

echo "java -Djava.security.krb5.conf=/etc/krb5.conf -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false -classpath /usr/sdp/current/hadoop-hdfs-client/*:${app_jar_local} ru.sberbank.bigdata.dffess.vspc.MainClass --loading_id=${loading_id} --ctl_url=${ctl_secure} --hadoop_file_systems=${hadoopFileSystems} --krb5cc_file_name=${krb5cc_file_name} --krb5ccname=${KRB5CCNAME}"
java -Djava.security.krb5.conf=/etc/krb5.conf -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false -classpath /usr/sdp/current/hadoop-hdfs-client/*:${app_jar_local} ru.sberbank.bigdata.dffess.vspc.MainClass --loading_id=${loading_id} --ctl_url=${ctl_secure} --hadoop_file_systems=${hadoopFileSystems} --krb5cc_file_name=${krb5cc_file_name} --krb5ccname=${KRB5CCNAME}