#!/bin/bash
loading_id=${1}
user_principal=${2}
ctl=${3}
user_keytab_path=${4}
app_jar=${5}${6}/${7}
devops_distributive_jar_local=${7}
user=${8}
user_keytab_path_local=tmp.keytab

export SPARK_HOME=/usr/sdp/current/spark3-client
export KRB5CCNAME=/tmp/krb5cc_${user}_$(uuidgen)

hdfs dfs -get ${user_keytab_path} ${user_keytab_path_local}
kinit -kt ${user_keytab_path_local} ${user_principal} || kinit -kt ${user_keytab_path_local} ${user_principal} || kinit -kt ${user_keytab_path_local} ${user_principal}
kinit -R

user_ticket_cache_file_name=rtfw_kerberos_ticket_cache_file

export RTFW_KERBEROS_TICKET_CACHE_FILE=${user_ticket_cache_file_name}

bash kvno_command.sh "${KRB5CCNAME}" "kinit -kt ${user_keytab_path_local} ${user_principal}" "${user}" "" "${sl_secman_host}" "${ssk_tgt_tgs_renew_h}"
rm ${user_keytab_path_local}

cp ${KRB5CCNAME} ./${user_ticket_cache_file_name}

hdfs dfs -get ${app_jar} ${devops_distributive_jar_local}

export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.jgss.native=true"
export HADOOP_OPTS="$HADOOP_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"
export YARN_OPTS="$YARN_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"
export HBASE_OPTS="$HBASE_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"

# запускаем приложение data-продукта
# Сначала jar-ы Spark, потом $CLASSPATH oozie-действия: с $CLASSPATH впереди на деве подхватывался более старый
# log4j-core, и RT-FW-логгер падал с NoSuchMethodError Configurator.setLevel(Logger, Level).
java \
-Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false \
-classpath $SPARK_HOME/jars/*:/usr/sdp/current/hive-metastore/lib/*:$CLASSPATH:${devops_distributive_jar_local} \
ru.sberbank.bigdata.sl.rt.core.RtfwLauncher \
${loading_id} \
${ctl} \
ru.sberbank.bigdata.dffess.vspc.MainClass

java_main_status=$?
echo Приложение data-продукта завершено со статусом $java_main_status
exit $java_main_status
