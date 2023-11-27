------------------------------------------------------------------------------------------------------------------------
-- Manual tables snapshot
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
-- Manual tables difference data
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
-- Published data snapshot
------------------------------------------------------------------------------------------------------------------------

-- Адрес: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.addr;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.addr (
 addr_dp_code STRING,
    addr_sk BIGINT,
    city_name STRING,
    cntry_dp_code STRING,
    cntry_sk BIGINT,
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    district_name STRING,
    fias_code STRING,
    house_num STRING,
    idx_num STRING,
    locty_name STRING,
    okato2_dp_code STRING,
    okato2_sk BIGINT,
    reg_addr_name STRING,
    rus_fed_subj_dp_code STRING,
    rus_fed_subj_sk BIGINT,
    sid STRING,
    src_txt STRING,
    street_name STRING
)

stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/addr';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.addr;

-- Адрес: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.addr_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.addr_h (
    addr_dp_code STRING,
    addr_sk BIGINT,
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    end_dt DATE,
    standard_addr_txt STRING,
    start_dt DATE
)
partitioned by (p1month STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/addr_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.addr_h;

-- Подразделение территориальной структуры: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.div_;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.div_ (
    code STRING,
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    div_close_dt DATE,
    div_dp_code STRING,
    div_open_dt DATE,
    div_sk BIGINT,
    legal_addr_dp_code STRING,
    legal_addr_sk BIGINT,
    parent_div_dp_code STRING,
    parent_div_sk BIGINT,
    short_div_code STRING,
   sid STRING,
    subbranch_code STRING
)

stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/div_';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.div_;

-- Подразделение территориальной структуры: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.div_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.div_h (
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    div_dp_code STRING,
    div_name STRING,
    div_sk BIGINT,
    div_ups_pay_sys_prtcpnt_dp_code STRING,
    div_ups_pay_sys_prtcpnt_sk BIGINT,
    end_dt DATE,
    fact_addr_dp_code STRING,
    fact_addr_sk BIGINT,
    start_dt DATE
)
partitioned by (p1month STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/div_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.div_h;


-- Подразделение территориальной структуры: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.div_type (
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    dirty_flag BOOLEAN,
    div_dp_code STRING,
    div_sk BIGINT,
    div_type_terr_div_type_dp_code STRING,
    div_type_terr_div_type_sk BIGINT,
    end_dt DATE,
    start_dt DATE
)
partitioned by (p1month STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.div_type;

-- Тип подразделений территориальной структуры: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.terr_div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.terr_div_type (
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    div_type_code STRING,
    sid STRING,
    terr_div_type_dp_code STRING,
    terr_div_type_sk BIGINT
)

stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/terr_div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.terr_div_type;



------------------------------------------------------------------------------------------------------------------------
-- Published difference data
------------------------------------------------------------------------------------------------------------------------

-- Адрес: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.addr;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.addr (
   addr_dp_code STRING,
    addr_sk BIGINT,
    city_name STRING,
    cntry_dp_code STRING,
    cntry_sk BIGINT,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    district_name STRING,
    fias_code STRING,
    house_num STRING,
    idx_num STRING,
    locty_name STRING,
    okato2_dp_code STRING,
    okato2_sk BIGINT,
    reg_addr_name STRING,
    rus_fed_subj_dp_code STRING,
    rus_fed_subj_sk BIGINT,
    sid STRING,
    src_txt STRING,
    street_name STRING
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/addr';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.addr;

-- Адрес: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.addr_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.addr_h (
    addr_dp_code STRING,
    addr_sk BIGINT,
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    end_dt DATE,
    p1month STRING,
    standard_addr_txt STRING,
    start_dt DATE
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/addr_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.addr_h;

-- Подразделение территориальной структуры: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.div_;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.div_ (
    code STRING,
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    div_close_dt DATE,
    div_dp_code STRING,
    div_open_dt DATE,
    div_sk BIGINT,
    legal_addr_dp_code STRING,
    legal_addr_sk BIGINT,
    parent_div_dp_code STRING,
    parent_div_sk BIGINT,
    short_div_code STRING,
    sid STRING,
    subbranch_code STRING
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/div_';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.div_;

-- Подразделение территориальной структуры: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.div_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.div_h (
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    div_dp_code STRING,
    div_name STRING,
    div_sk BIGINT,
    div_ups_pay_sys_prtcpnt_dp_code STRING,
    div_ups_pay_sys_prtcpnt_sk BIGINT,
    end_dt DATE,
    fact_addr_dp_code STRING,
    fact_addr_sk BIGINT,
    p1month STRING,
    start_dt DATE
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/div_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.div_h;

-- Подразделение территориальной структуры: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.div_type (
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    dirty_flag BOOLEAN,
    div_dp_code STRING,
    div_sk BIGINT,
    div_type_terr_div_type_dp_code STRING,
    div_type_terr_div_type_sk BIGINT,
    end_dt DATE,
    p1month STRING,
    start_dt DATE
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.div_type;

-- Тип подразделений территориальной структуры: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.terr_div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.terr_div_type (
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    div_type_code STRING,
    sid STRING,
    terr_div_type_dp_code STRING,
    terr_div_type_sk BIGINT
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/terr_div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.terr_div_type;




------------------------------------------------------------------------------------------------------------------------
-- CTL_LOG tables
------------------------------------------------------------------------------------------------------------------------

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.ctl_log;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.ctl_log(
        provider_product_code STRING,
        provider_dataset_code STRING,
        consumer_product_code STRING,
        consumer_dataset_code STRING,
        provider_loading_id int,
        consumer_loading_id int)
		partitioned by (registry_month STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/ctl_log'
	tblproperties ("skip.header.line.count"="1");

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.ctl_log;


------------------------------------------------------------------------------------------------------------------------
-- STAGE TABLES
------------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------------
-- Stage tables snapshot
------------------------------------------------------------------------------------------------------------------------

-- Адрес: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.addr;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.addr (
    addr_dp_code STRING,
    addr_sk BIGINT,
    city_name STRING,
    cntry_dp_code STRING,
    cntry_sk BIGINT,
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    district_name STRING,
    fias_code STRING,
    house_num STRING,
    idx_num STRING,
    locty_name STRING,
    okato2_dp_code STRING,
    okato2_sk BIGINT,
    reg_addr_name STRING,
    rus_fed_subj_dp_code STRING,
    rus_fed_subj_sk BIGINT,
    sid STRING,
    src_txt STRING,
    street_name STRING
)

stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/addr';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.addr;

-- Адрес: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.addr_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.addr_h (
    addr_dp_code STRING,
    addr_sk BIGINT,
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    end_dt DATE,
    standard_addr_txt STRING,
    start_dt DATE
)
partitioned by (p1month STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/addr_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.addr_h;

-- Подразделение территориальной структуры: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.div_;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.div_ (
    code STRING,
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    div_close_dt DATE,
    div_dp_code STRING,
    div_open_dt DATE,
    div_sk BIGINT,
    legal_addr_dp_code STRING,
    legal_addr_sk BIGINT,
    parent_div_dp_code STRING,
    parent_div_sk BIGINT,
    short_div_code STRING,
    sid STRING,
    subbranch_code STRING
)

stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/div_';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.div_;

-- Подразделение территориальной структуры: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.div_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.div_h (
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    div_dp_code STRING,
    div_name STRING,
    div_sk BIGINT,
    div_ups_pay_sys_prtcpnt_dp_code STRING,
    div_ups_pay_sys_prtcpnt_sk BIGINT,
    end_dt DATE,
    fact_addr_dp_code STRING,
    fact_addr_sk BIGINT,
    start_dt DATE
)
partitioned by (p1month STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/div_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.div_h;


-- Подразделение территориальной структуры: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.div_type (
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    dirty_flag BOOLEAN,
    div_dp_code STRING,
    div_sk BIGINT,
    div_type_terr_div_type_dp_code STRING,
    div_type_terr_div_type_sk BIGINT,
    end_dt DATE,
    p1month STRING,
    start_dt DATE
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.div_type;

-- Тип подразделений территориальной структуры: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.terr_div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.terr_div_type (
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    div_type_code STRING,
    sid STRING,
    terr_div_type_dp_code STRING,
    terr_div_type_sk BIGINT
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/terr_div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.terr_div_type;




------------------------------------------------------------------------------------------------------------------------
--- DIFF STG TABLES
------------------------------------------------------------------------------------------------------------------------


-- Адрес: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.diff_addr;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_addr (
    addr_dp_code STRING,
    addr_sk BIGINT,
    city_name STRING,
    cntry_dp_code STRING,
    cntry_sk BIGINT,
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    district_name STRING,
    fias_code STRING,
    house_num STRING,
    idx_num STRING,
    locty_name STRING,
    okato2_dp_code STRING,
    okato2_sk BIGINT,
    reg_addr_name STRING,
    rus_fed_subj_dp_code STRING,
    rus_fed_subj_sk BIGINT,
    sid STRING,
    src_txt STRING,
    street_name STRING
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/diff_addr';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_addr;

-- Адрес: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.diff_addr_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_addr_h (
    addr_dp_code STRING,
    addr_sk BIGINT,
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    end_dt DATE,
    p1month STRING,
    standard_addr_txt STRING,
    start_dt DATE
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/diff_addr_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_addr_h;

-- Подразделение территориальной структуры: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.diff_div_;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_div_ (
    code STRING,
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    div_close_dt DATE,
    div_dp_code STRING,
    div_open_dt DATE,
    div_sk BIGINT,
    legal_addr_dp_code STRING,
    legal_addr_sk BIGINT,
    parent_div_dp_code STRING,
    parent_div_sk BIGINT
    short_div_code STRING,
    sid STRING,
    subbranch_code STRING
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/diff_div_';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_div_;

-- Подразделение территориальной структуры: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.diff_div_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_div_h (
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    div_dp_code STRING,
    div_name STRING,
    div_sk BIGINT,
    div_ups_pay_sys_prtcpnt_dp_code STRING,
    div_ups_pay_sys_prtcpnt_sk BIGINT,
    end_dt DATE,
    fact_addr_dp_code STRING,
    fact_addr_sk BIGINT,
    p1month STRING,
    start_dt DATE
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/diff_div_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_div_h;

-- Подразделение территориальной структуры: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.diff_div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_div_type (
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    dirty_flag BOOLEAN,
    div_dp_code STRING,
    div_sk BIGINT,
    div_type_terr_div_type_dp_code STRING,
    div_type_terr_div_type_sk BIGINT,
    end_dt DATE,
    p1month STRING,
    start_dt DATE
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/diff_div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_div_type;

-- Тип подразделений территориальной структуры: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.diff_terr_div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_terr_div_type (
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    div_type_code STRING,
    sid STRING,
    terr_div_type_dp_code STRING,
    terr_div_type_sk BIGINT
)
partitioned by (ctl_loading BIGINT, ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/diff_terr_div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_terr_div_type;


