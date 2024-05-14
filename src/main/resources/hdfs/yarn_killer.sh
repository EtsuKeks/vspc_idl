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

echo "spark-submit --name loading_id=${loading_id}:wf_name=${wf_name}_killer --master yarn --deploy-mode cluster --queue ${queue} --files /etc/hive/conf/hive-site.xml --keytab ${user_keytab_path_local} --principal ${user_principal} --class ru.sberbank.bigdata.gbmasper.Application ${jar_name} --loading_id ${loading_id} --update_rate ${update_rate} --ctl_entity_id ${ctl_entity_id} --ctl_stat_id ${ctl_stat_id} --ctl_url ${ctl_url} --oozie_job_id ${oozie_job_id} --oozie_job_action ${oozie_job_action}"