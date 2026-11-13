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

# Собираем classpath: jar-ы Spark, jar-ы hive-metastore и classpath oozie — оба без log4j-jar-ов, затем наш jar.
# log4j берётся только из Spark: так надёжнее — логгер RT-FW не подхватывает чужой log4j из hive-metastore,
# на котором падал с NoSuchMethodError.
hive_metastore_jars=$(ls /usr/sdp/current/hive-metastore/lib/*.jar | grep -v '/log4j-' | paste -sd: -)
oozie_classpath=$(echo "$CLASSPATH" | tr ':' '\n' | grep -v '/log4j-[^/]*\.jar$' | paste -sd: -)
launch_classpath="$SPARK_HOME/jars/*:${hive_metastore_jars}:${oozie_classpath}:${devops_distributive_jar_local}"
echo "log4j в classpath запуска:"; echo "$launch_classpath" | tr ':' '\n' | grep -i log4j; ls $SPARK_HOME/jars | grep -i log4j

# запускаем приложение data-продукта
java \
-Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false \
-classpath "${launch_classpath}" \
ru.sberbank.bigdata.sl.rt.core.RtfwLauncher \
${loading_id} \
${ctl} \
ru.sberbank.bigdata.dffess.vspc.MainClass

java_main_status=$?
echo Приложение data-продукта завершено со статусом $java_main_status
exit $java_main_status
