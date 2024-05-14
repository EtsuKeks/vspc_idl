user_keytab_path_local=kill_tmp.keytab
hdfs_jar=${nameNode}${jars_path::-3}${jar_name}
echo "${hdfs_jar}"

hdfs dfs -get ${user_keytab_path} ${user_keytab_path_local} 
kinit -kt ${user_keytab_path_local} ${user_principal}
kinit -R
klist

hdfs dfs -get ${hdfs_jar} ${jar_name} 
echo "the jar was copied"

echo "${ctl_url}"

ls -la

echo "spark-submit --driver-java-options="-Dsun.security.jgss.native=true -Djava.security.krb5.debug=true -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false" --name loading_id=${loading_id}:wf_name=${wf_name}_killer --master yarn --deploy-mode cluster --queue ${queue} --conf spark.shuffle.service.enabled=true --conf spark.executor.memory=5G --conf spark.dynamicAllocation.enabled=true --conf spark.yarn.maxAppAttempts=1 --conf spark.dynamicAllocation.minExecutors=3 --conf spark.dynamicAllocation.maxExecutors=10 --conf spark.driver.memory=5G --files /etc/hive/conf/hive-site.xml --keytab ${user_keytab_path_local} --principal ${user_principal} --class ru.sberbank.bigdata.gbmasper.YarnKillerApplication ${jar_name} --loading_id ${loading_id} --update_rate ${update_rate} --ctl_entity_id ${ctl_entity_id} --ctl_stat_id ${ctl_stat_id} --ctl_url ${ctl_url} --keytab ${user_keytab_path_local} --wf_id ${wf_id} --oozie_job_id ${oozie_job_id} --application_type ${application_type} --oozie_job_action ${oozie_job_action}"

spark-submit --driver-java-options="-Dsun.security.jgss.native=true -Djava.security.krb5.debug=true -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false" --name loading_id=${loading_id}:wf_name=${wf_name}_killer --master yarn --deploy-mode cluster --queue ${queue} --conf spark.shuffle.service.enabled=true --conf spark.executor.memory=5G --conf spark.dynamicAllocation.enabled=true --conf spark.yarn.maxAppAttempts=1 --conf spark.dynamicAllocation.minExecutors=3 --conf spark.dynamicAllocation.maxExecutors=10 --conf spark.driver.memory=5G --files /etc/hive/conf/hive-site.xml --keytab ${user_keytab_path_local} --principal ${user_principal} --class ru.sberbank.bigdata.gbmasper.YarnKillerApplication ${jar_name} --loading_id ${loading_id} --update_rate ${update_rate} --ctl_entity_id ${ctl_entity_id} --ctl_stat_id ${ctl_stat_id} --ctl_url ${ctl_url} --keytab ${user_keytab_path_local} --wf_id ${wf_id} --oozie_job_id ${oozie_job_id} --application_type ${application_type} --oozie_job_action ${oozie_job_action}