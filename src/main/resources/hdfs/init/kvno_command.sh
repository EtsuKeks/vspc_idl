export KRB5CCNAME=$1
result='kvno -c '$KRB5CCNAME' '
IFS=',' read -a nameservices <<< $(echo $XML | xmllint --xpath 'string(//configuration/property[name="dfs.nameservices"]/value)' /etc/hadoop/conf/hdfs-site.xml)
for nameservice in ${nameservices[@]}; do
IFS=',' read -a namenodes <<< $(echo $XML | xmllint --xpath $(echo "string(//configuration/property[name=\"dfs.ha.namenodes."$(echo $nameservice)"\"]/value)") /etc/hadoop/conf/hdfs-site.xml)
namenode=${namenodes[0]}
node=$(echo $XML | xmllint --xpath $(echo "string(//configuration/property[name=\"dfs.namenode.http-address."$(echo $nameservice)"."$(echo $namenode)"\"]/value)") /etc/hadoop/conf/hdfs-site.xml)
node=${node%:*}
result+=' '$(ssh -K ${node} 'command="'""'"; while IFS= read -r line; do if [[ -n ${line// /} ]]; then command+="'" hbase/"'"$line; fi done < /etc/hbase/conf/regionservers; echo $command')
done
echo $result