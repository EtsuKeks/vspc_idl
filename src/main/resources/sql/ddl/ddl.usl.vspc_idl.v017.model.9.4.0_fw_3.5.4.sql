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

-- Подразделение территориальной структуры Сбербанка: hub table

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
    div_time_zone_time_zone_rf_dp_code STRING,
    div_time_zone_time_zone_rf_sk BIGINT,
    full_reg_num STRING,
    legal_addr_dp_code STRING,
    legal_addr_sk BIGINT,
    parent_div_dp_code STRING,
    parent_div_sk BIGINT,
    reg_ord BIGINT,
    short_div_code STRING,
    sid STRING,
    subbranch_code STRING
)

stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/div_';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.div_;

-- Подразделение территориальной структуры Сбербанка: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.div_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.div_h (
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    div_dp_code STRING,
    div_name STRING,
    div_sk BIGINT,
    end_dt DATE,
    fact_addr_dp_code STRING,
    fact_addr_sk BIGINT,
    pay_sys_prtcpnt_dp_code STRING,
    pay_sys_prtcpnt_sk BIGINT,
    start_dt DATE
)
partitioned by (p1month STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/div_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.div_h;

-- Подразделение территориальной структуры Сбербанка: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.div_type (
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
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

-- Вид подразделений территориальной структуры Сбербанка: hub table

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

-- Часовая зона РФ: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_snp.time_zone_rf;

create external table usl_{{ logical_env_prefix }}vspc_idl_snp.time_zone_rf (
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    sid STRING,
    time_zone_abbr_code STRING,
    time_zone_code STRING,
    time_zone_msk_shift_val STRING,
    time_zone_name STRING,
    time_zone_rf_dp_code STRING,
    time_zone_rf_sk BIGINT,
    time_zone_wrld_shift_val STRING
)

stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/snp/time_zone_rf';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_snp.time_zone_rf;


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
partitioned by (ctl_loading BIGINT,ctl_do STRING)
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
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/addr_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.addr_h;

-- Подразделение территориальной структуры Сбербанка: hub table

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
    div_time_zone_time_zone_rf_dp_code STRING,
    div_time_zone_time_zone_rf_sk BIGINT,
    full_reg_num STRING,
    legal_addr_dp_code STRING,
    legal_addr_sk BIGINT,
    parent_div_dp_code STRING,
    parent_div_sk BIGINT,
    reg_ord BIGINT,
    short_div_code STRING,
    sid STRING,
    subbranch_code STRING
)
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/div_';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.div_;

-- Подразделение территориальной структуры Сбербанка: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.div_h;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.div_h (
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    div_dp_code STRING,
    div_name STRING,
    div_sk BIGINT,
    end_dt DATE,
    fact_addr_dp_code STRING,
    fact_addr_sk BIGINT,
    p1month STRING,
    pay_sys_prtcpnt_dp_code STRING,
    pay_sys_prtcpnt_sk BIGINT,
    start_dt DATE
)
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/div_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.div_h;

-- Подразделение территориальной структуры Сбербанка: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.div_type (
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    div_dp_code STRING,
    div_sk BIGINT,
    div_type_terr_div_type_dp_code STRING,
    div_type_terr_div_type_sk BIGINT,
    end_dt DATE,
    p1month STRING,
    start_dt DATE
)
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.div_type;

-- Вид подразделений территориальной структуры Сбербанка: hub table

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
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/terr_div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.terr_div_type;

-- Часовая зона РФ: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_diff.time_zone_rf;

create external table usl_{{ logical_env_prefix }}vspc_idl_diff.time_zone_rf (
    ctl_action STRING,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    sid STRING,
    time_zone_abbr_code STRING,
    time_zone_code STRING,
    time_zone_msk_shift_val STRING,
    time_zone_name STRING,
    time_zone_rf_dp_code STRING,
    time_zone_rf_sk BIGINT,
    time_zone_wrld_shift_val STRING
)
partitioned by (ctl_loading BIGINT,ctl_do STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/diff/time_zone_rf';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_diff.time_zone_rf;


------------------------------------------------------------------------------------------------------------------------
-- CTL_LOG and SUCCESS_LOG tables
------------------------------------------------------------------------------------------------------------------------

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.ctl_log;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.ctl_log(
        provider_product_code STRING,
        provider_dataset_code STRING,
        consumer_product_code STRING,
        consumer_dataset_code STRING,
        provider_loading_id   INTEGER,
        consumer_loading_id   INTEGER,
        upd_provider_loading_id INTEGER,
        upd_consumer_loading_id INTEGER,
        consumer_loading_dttm TIMESTAMP,
        version STRING)
        partitioned by (registry_month STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/ctl_log'
    tblproperties ("skip.header.line.count"="1");

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.ctl_log;


drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.success_log;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.success_log(
        program_unit STRING,
        upd_loading_id INTEGER)

    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/success_log'
    tblproperties ("skip.header.line.count"="1");

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.success_log;
