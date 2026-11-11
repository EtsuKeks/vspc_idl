#!/bin/bash
# Эталон: листинг инструкции RT-FW 3.16, п. 6.3.2 «при запуске через java или RtfwLauncher» (копия рядом: StartLauncher.sh.listing-6.3.2.orig).
# Все отличия от эталона помечены «vspc Δn»; остальное совпадает с листингом построчно.
loading_id=${1}
user_principal=${2}
ctl=${3}
user_keytab_path=${4}
# vspc Δ1: usl_vspc_idl.xml (как в 3.0) передаёт 8 аргументов: 5 = nameNode, 6 = каталог lib, 7 = имя jar, 8 = учётка
app_jar=${5}${6}/${7}
devops_distributive_jar_local=${7}
user=${8}
user_keytab_path_local=tmp.keytab

export SPARK_HOME=/usr/sdp/current/spark3-client
# vspc Δ8 (DV-38): свой кэш Kerberos на каждый запуск, как в 3.0 (vspc_idl_old StartLauncher.sh:22-23)
export KRB5CCNAME=/tmp/krb5cc_${user}_$(uuidgen)

hdfs dfs -get ${user_keytab_path} ${user_keytab_path_local}
kinit -kt ${user_keytab_path_local} ${user_principal} || kinit -kt ${user_keytab_path_local} ${user_principal} || kinit -kt ${user_keytab_path_local} ${user_principal}
kinit -R

# получаем ссылку на кеш тикета, который в дальнейшем будет передаваться в драйвер.
user_ticket_cache_path=`klist |grep FILE: | awk -F: '{print $3}'`
# vspc Δ9 (DV-37): постоянное имя копии кэша, на него указывает KRB5CCNAME AM и executors в vars_all.yml, как в 3.0 (vspc_idl_old StartLauncher.sh:24,37)
user_ticket_cache_file_name=rtfw_kerberos_ticket_cache_file
echo $? user_ticket_cache_path=$user_ticket_cache_path

export RTFW_KERBEROS_TICKET_CACHE_FILE=${user_ticket_cache_file_name}

export KRB5CCNAME=${user_ticket_cache_path}
echo $? KRB5CCNAME=$KRB5CCNAME  # копируем user_ticket_cache_file в папку запуска приложения

# пример получения тикетов для hbase, если в ДП используется ССК
# vspc Δ2: kvno_command.sh — общий скрипт ESSA (как в 3.0): локализуется oozie через <file> в usl_vspc_idl.xml,
#          принимает 6 аргументов и сам выполняет kvno (команду не печатает), при необходимости делает kinit по keytab
bash kvno_command.sh "${KRB5CCNAME}" "kinit -kt ${user_keytab_path_local} ${user_principal}" "${user}" "" "${sl_secman_host}" "${ssk_tgt_tgs_renew_h}"

# удаляем кейтаб файл
# vspc Δ3: перенесено ниже вызова kvno_command.sh — его запасной kinit читает keytab
rm ${user_keytab_path_local}

# vspc Δ9: копия под постоянным именем
cp ${user_ticket_cache_path} ./${user_ticket_cache_file_name}

# vspc Δ4: jar лежит в каталоге lib дистрибутива и не локализуется oozie — скачиваем, как в 3.0
hdfs dfs -get ${app_jar} ${devops_distributive_jar_local}

export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.jgss.native=true"
export HADOOP_OPTS="$HADOOP_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"
export YARN_OPTS="$YARN_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"
export HBASE_OPTS="$HBASE_OPTS -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false"

# или заполняем значение переменной RTFW_KERBEROS_TICKET_CACHE_FILE
export RTFW_KERBEROS_TICKET_CACHE_FILE=${user_ticket_cache_file_name}

# запускаем приложение data-продукта
# или заполняем значение java System property RTFW_KERBEROS_TICKET_CACHE_FILE
# vspc Δ5: строка-комментарий выше в листинге стояла внутри java-команды с переносом «\» и обрывала её
# vspc Δ6: + $SPARK_HOME/jars/* (как в rt-fw-templater 3.16.1): RtfwLauncher запускает приложение через
#          org.apache.spark.launcher.SparkLauncher, а Spark в jar витрины не входит (scope provided)
# vspc Δ7: main-класс data-продукта vspc_idl вместо ru.sberbank.bigdata.sl.rt.Main (как предписывает п. 6.3.1)
java \
-Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false \
-classpath $CLASSPATH:$SPARK_HOME/jars/*:/usr/sdp/current/hive-metastore/lib/*:${devops_distributive_jar_local} \
-DRTFW_KERBEROS_TICKET_CACHE_FILE=${user_ticket_cache_file_name} \
ru.sberbank.bigdata.sl.rt.core.RtfwLauncher \
${loading_id} \
${ctl} \
ru.sberbank.bigdata.dffess.vspc.MainClass

java_main_status=$?
echo Приложение data-продукта завершено со статусом $java_main_status
exit $java_main_status
