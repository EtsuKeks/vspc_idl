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

export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.jgss.native=true"
export HADOOP_OPTS="$HADOOP_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"
export YARN_OPTS="$YARN_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"
export HBASE_OPTS="$HBASE_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"

#вызываем запуск расчёта дата-продукта с двумя аргументами - loading_id, CTL URL.
#Целевой способ запуска (инструкция RT-FW 6.3): JAR запускается как Java-приложение через
#единую точку входа RtfwLauncher, который сам создаёт Spark-сессию из параметров CTL-потока
#и авто-рассчитывает spark.yarn.access.hadoopFileSystems (см. раздел 6.3 инструкции).
#Третий аргумент RtfwLauncher - main-класс data-продукта (Spark-точка входа, запускается как
#единственный дочерний SpawnedApp). Параметры spark.*, требуемые для java-запуска
#(KRB5CCNAME, zookeeper jaas, hadoopFileSystems), задаются в spark_parameters CTL-потока.
java -Xmx2G -XX:MetaspaceSize=100M \
  -Dsun.security.jgss.native=true \
  -Djavax.security.auth.useSubjectCredsOnly=false \
  -DRTFW_KERBEROS_TICKET_CACHE_FILE=${user_ticket_cache_file_name} \
  -classpath /usr/sdp/current/spark3-client/jars/*:/usr/sdp/current/hive-metastore/lib/*:${devops_distributive_jar_local} \
  ru.sberbank.bigdata.sl.rt.core.RtfwLauncher \
  ${loading_id} ${ctl} ru.sberbank.bigdata.dffess.vspc.MainClass

java_main_status=$?
echo "data-продукт vspc_idl (RtfwLauncher) завершён со статусом $java_main_status"
exit $java_main_status