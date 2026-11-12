krb5cc_file_name=$1
kinit_command=$2
user=$3
instance_code=$4
secman_host=$5
tgt_tgs_renew=$6

echo -e "Start kvno_command with parameters:\nkrb5cc_file_name = $krb5cc_file_name\nkinit_command=$kinit_command\nuser=$user\ninstance_code=$instance_code\nsecman_host=$secman_host\ntgt_tgs_renew=$tgt_tgs_renew"

#Формируем список хостов hbase до получения ticket cache с сервера для того, чтобы получить список регион-серверов через ssh независимо от корректности ticket cache
echo "Start finding hbase hosts for kvno command"
res=''
IFS=',' read -a nameservices <<< $(echo $XML | xmllint --xpath 'string(//configuration/property[name="dfs.nameservices"]/value)' /etc/hadoop/conf/hdfs-site.xml)
for nameservice in ${nameservices[@]}; do
IFS=',' read -a namenodes <<< $(echo $XML | xmllint --xpath $(echo "string(//configuration/property[name=\"dfs.ha.namenodes."$(echo $nameservice)"\"]/value)") /etc/hadoop/conf/hdfs-site.xml)
namenode=${namenodes[0]}
node=$(echo $XML | xmllint --xpath $(echo "string(//configuration/property[name=\"dfs.namenode.http-address."$(echo $nameservice)"."$(echo $namenode)"\"]/value)") /etc/hadoop/conf/hdfs-site.xml)
node=${node%:*}
if [ -n "${node}" ]; then
        res+=' '$(ssh -K ${node} '
            command=""
            while IFS= read -r line; do
                if [[ -n ${line// /} ]]; then
                    command+=" hbase/"$line
                fi
            done < /etc/hbase/conf/regionservers
            XML_CONTENT=$(cat /etc/hbase/conf/hbase-site.xml 2>/dev/null)
            if [ -n "$XML_CONTENT" ]; then
                ZK_QUORUM=$(echo "$XML_CONTENT" | xmllint --xpath "string(//configuration/property[name=\"hbase.zookeeper.quorum\"]/value)" - 2>/dev/null)
                if [ -n "$ZK_QUORUM" ]; then
                    IFS="," read -ra zookeeper_hosts <<< "$ZK_QUORUM"
                    for zookeeper_host in "${zookeeper_hosts[@]}"; do
                        command+=" zookeeper/"$zookeeper_host
                    done
                fi
            fi
            echo "$command"
        ')
fi
done

user_pattern="^u_\(\w\+\)_s_usl_\(\(\w\+\)_\)\?t_\(\w\+\)"
log_env=$(sed 's/'${user_pattern}'/\3/' <<< "${user}")
data_product_name=$(sed 's/'${user_pattern}'/\4/' <<< "${user}")

approle_path="/v1/auth/approle/login"
data_product_secman="${data_product_name//_/-}"

#Получение role_id, secure_id из keytab
keytab_path=/keytab/${user}.keytab
$(hdfs dfs -test -e "${keytab_path}");
if [ $? != "0" ]; then echo "Keytab ${keytab_path} not found"; fi
vault_namespace=$(hdfs dfs -cat ${keytab_path} | jq -r ".namespace")
role_id=$(hdfs dfs -cat ${keytab_path} | jq -r ".role_id")
secret_id=$(hdfs dfs -cat ${keytab_path} | jq -r ".secret_id" | base64 -d | openssl rsautl -decrypt -inkey $(hdfs dfs -cat /keytab/${user}.keytab | jq -r ".secret_id_decrypt_key_path"))

#Получение токена из secman
if [[ ! "$secman_host" =~ ^http ]]; then secman_host="https://"$secman_host; fi
data_json=$(jq -n --arg role_id "$role_id" --arg secret_id "$secret_id" '{role_id: $role_id, secret_id: $secret_id}')
curl_token="curl -s --request POST --url $secman_host$approle_path --header 'Content-Type:application/json' --header 'x-vault-namespace:$vault_namespace' --data '$data_json'"
echo "Start search secman token"
token=$(eval "$curl_token" | jq -r ".auth.client_token // empty" )
if [[ -z "${token}" ]]; then
	echo "Error in get token with url: "$secman_host$approle_path
fi
echo "Complete search secman token"

#Получение сертификата из secman
email=$(hdfs dfs -cat /user/${user}/app.properties | grep secman_email | cut -d '=' -f2)

common_name=$(hdfs dfs -cat /user/${user}/app.properties | grep secman_sberca_common_name | awk -F '=' '{print $2}')
if [ -z "$common_name" ]; then \
	app_env=$(hdfs dfs -cat /user/${user}/app.properties | grep app_env | cut -d '=' -f2); \
	secman_cefss=$(hdfs dfs -cat /user/${user}/app.properties | grep secman_cefss | awk -F '=' '{print tolower($2)}'); \
	common_name="$data_product_secman-$app_env-$secman_cefss"; \
fi

secman_sberca_role_name=$(hdfs dfs -cat /user/${user}/app.properties | grep secman_sberca_role_name | cut -d '=' -f2)
if [ -z "$secman_sberca_role_name" ]; then \
	secman_sberca_role_name="${common_name}-client"; \
fi

secman_cert_path=$(hdfs dfs -cat /user/${user}/app.properties | grep secman_cert_path | cut -d '=' -f2)
url="$secman_host/$secman_cert_path/$secman_sberca_role_name"

data_json=$(jq -n --arg common_name "$common_name" --arg email "$email" --arg format "pem" '{common_name: $common_name, email: $email, format: "pem"}')
curl_cert="curl -s --request POST --url $url --header 'X-Vault-Token:'"\"${token}\"" --header 'x-vault-namespace: $vault_namespace' --data '$data_json'"

echo "Start finding certificate"
cert_json=$(eval "$curl_cert")
certificate=$(echo "$cert_json" | jq -r '.data.certificate // empty')
private_key=$(echo "$cert_json" | jq -r '.data.private_key // empty')
cacert=$(echo "$cert_json" | jq -r '.data.issuing_ca // empty')
if [[ -z "${certificate}" || -z "${private_key}" || -z "${cacert}" ]]; then
	echo "Error in get certificate with url: "$url
fi
echo "Complete finding certificate"

#Поиск хостов service-discovery
echo "Start finding service discovery hosts"
hbase_namespace=usl_$data_product_name
if [[ -n "$log_env" ]]; then hbase_namespace=${log_env}__${hbase_namespace}; fi
sk_container_table_name=sk_container
if [[ -n "$instance_code" ]]; then sk_container_table_name=${sk_container_table_name}___${instance_code}; fi
sk_container_table_name=${hbase_namespace}:${sk_container_table_name}
echo "sk_container_table_name: "$sk_container_table_name
sk_container_description=$(echo "describe '$sk_container_table_name'" | hbase shell)
echo "sk_container_description: "$sk_container_description
column_families=$(echo "$sk_container_description" | grep -oP "(?<=NAME => ').+?(?=')")
column_family=$(echo $column_families | cut -d' ' -f1)
echo "column_family: "$column_family
sd_hosts_key="SD_HOSTS"
qualifier="c"
sd_hosts_list=$(echo "get '$sk_container_table_name', '$sd_hosts_key'" | hbase shell | grep "$column_family:$qualifier" | awk '{print $NF}')
sd_hosts_list=${sd_hosts_list#*"="}
echo "Complete finding service discovery hosts: "$sd_hosts_list

#Обращение к service-discovery для получения хоста сервиса
data_product_name_full=usl.$data_product_name
if [[ -n "$log_env" ]]; then data_product_name_full=${log_env}__${data_product_name_full}; fi
if [[ -n "$instance_code" ]]; then data_product_name_full=${data_product_name_full}___${instance_code}; fi
echo "Start finding OpenShift server host in Service Discovery"
IFS=',' read -a sd_hosts <<< $sd_hosts_list
for sd_host in ${sd_hosts[@]};
do
	echo $sd_host
	url_sd=${sd_host}/product_entities_info/${data_product_name_full}
	curl_sd="curl -s --request GET --cert <(echo '$certificate') --key <(echo '$private_key') --cacert <(echo '$cacert') --url $url_sd"
	server_info=$(eval "$curl_sd")
	container_codes_and_hosts=$(echo "$server_info" | jq -c '.entitiesInfo | to_entries[] | {code:.value.containerCode, host:.value.containerHost}')
  sorted_container_codes_and_hosts=$(echo "$container_codes_and_hosts" | jq -s 'sort_by(.code)')
  server_host=$(echo "$sorted_container_codes_and_hosts" | jq -r '.[0].host')
  container_code=$(echo "$sorted_container_codes_and_hosts" | jq -r '.[0].code')
	if [ -n "$server_host" ] && [ "$server_host" != "null" ]; then
    echo "Успех! Найден host: $server_host (code: $container_code)"
    break
  fi
done
if [[ -z "${server_host}" ]]; then
	echo "Server host not found in any Service Discovery: "$sd_hosts_list
fi
echo "Complete finding OpenShift server host in Service Discovery: "$server_host

if [[ -n "$container_code" ]]; then container_header=" --header 'ssk-server-container-code:"${container_code}"'"; fi

url_tc="https://${server_host}/ticket_cache/${data_product_name_full}"
echo "Start finding ticket cache using url: "$url_tc
curl_tc="curl -s --request GET --cert <(echo '$certificate') --key <(echo '$private_key') --cacert <(echo '$cacert') --url $url_tc $container_header"
ticket_cache=$(eval "$curl_tc")
echo "Complete finding ticket cache using url: "$url_tc
#ticket_cache_text=$(echo "$ticket_cache" | tr -d '"\' )
#echo "$ticket_cache_text" | base64 -d > $krb5cc_file_name
echo "$ticket_cache" | sed 's/\\u003d/=/g' | tr -d '"' | base64 -d > $krb5cc_file_name
if [ $? -ne 0 ]; then 
	echo "Error in get ticket cache: "$url_tc
else
	echo "Ticket cache file created: "$krb5cc_file_name
fi

echo "start logging ticket cache from server"
klist -c $krb5cc_file_name
echo "complete logging ticket cache from server"

#Проверяем, что полученный тикетный кэш содержит tgt, tgs, до истечений которых больше $tgt_tgs_renew. Если нет - обновляем
current_date_sec=$(date +%s)
if [ -z "$tgt_tgs_renew" ]; then echo "tgt_tgs_renew parameter will use default value 12"; tgt_tgs_renew=12; fi
klist_output=$(klist -c $krb5cc_file_name)
line=$(echo "$klist_output" | grep -oP "[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}  krbtgt/")
expires_date=$(echo "$line" | sed 's/krbtgt\/$//')
if [[ -z "$expires_date" ]]; then \
	echo "expires date not found"; \
	expires_date_sec=$current_date_sec
else
	expires_date_sec=$(date --date="$expires_date" +%s)
fi
hours_diff=$(( (expires_date_sec - current_date_sec) / 3600 ))
echo "Time until KRBTGT expires in hour: "$hours_diff
if (( $hours_diff < $tgt_tgs_renew )); then \
	echo -e "Time until KRBTGT expires is less then "$tgt_tgs_renew", kinit command will be execute: \n"$kinit_command; \
	eval "$kinit_command"; \
	if [ $? -ne 0 ]; then echo "kinit_command failed"; exit 1; fi \
fi;


#Ищем в полученном с сервера ticket cache список хостов hbase с актуальным tgs (до времени окончания больше tgt_tgs_renew часов)
klist_output=$(klist -c $krb5cc_file_name)
regex='[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}  (hbase|zookeeper)/.*$'

hosts=()
while IFS= read -r line; do \
if [[ $line =~ $regex ]]; then \
record=$(echo "$line" | grep -oP "$regex"); \
expires_date=${record%"  "*}; \
principal=${record#*"  "}; \
host=${principal%"@"*}; \
expires_date_sec=$(date --date="$expires_date" +%s); \
hours_diff=$(( (expires_date_sec - current_date_sec) / 3600 )); \
if (( $hours_diff > $tgt_tgs_renew )); then hosts+=("$host"); fi \
fi; done <<< "$klist_output"


#Оставляем только хосты, tgs которых не найдены на сервере или время до завершения меньше допустимого
nodes=()
IFS=' ' read -a hbase_nodes <<< $(echo $res)
#Проверка на то, что нашли хотя бы один регион сервер. Защита от того, что нет прав на получение списка регионов по ssh
if [ ${#hbase_nodes[@]} -eq 0 ]; then echo "ERROR: no any hbase region server host found"; exit 1; fi
for hbase_node in "${hbase_nodes[@]}"; do
  found=false
  for host in "${hosts[@]}"; do
    if [[ "$hbase_node" == "$host" ]]; then
      found=true
      break
    fi
  done
  
  if ! $found; then
    nodes+=( "$hbase_node" )
  fi
done

#Формируем команду kvno, если есть хотя бы один хост, для которого нужно выполнить
if [ ${#nodes[@]} -gt 0 ]; then \
	result='kvno -c '$KRB5CCNAME' '${nodes[@]}; \
	echo -e "Some hbase regionservers not found in ticket cache from server or it expires date will be soon, so kvno command will be executed:\n"$result; \
	$result
else \
	echo "Ticket cache from server contains fresh TGS for all hbase regionservers, kvno command will not be executed"; \
fi
klist
echo "Complete kvno command"