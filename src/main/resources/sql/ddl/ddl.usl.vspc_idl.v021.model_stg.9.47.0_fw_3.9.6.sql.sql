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
    end_dt DATE,
    fact_addr_dp_code STRING,
    fact_addr_sk BIGINT,
    pay_sys_prtcpnt_dp_code STRING,
    pay_sys_prtcpnt_sk BIGINT,
    start_dt DATE
)
partitioned by (p1month STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/div_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.div_h;


-- Подразделение территориальной структуры: satellite table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.div_type (
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
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/div_type';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.div_type;

-- Тип подразделений территориальной структуры: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.terr_div_type;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.terr_div_type (
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
    end_dt DATE,
    fact_addr_dp_code STRING,
    fact_addr_sk BIGINT,
    pay_sys_prtcpnt_dp_code STRING,
    pay_sys_prtcpnt_sk BIGINT,
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

-- Часовая зона РФ: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.time_zone_rf;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.time_zone_rf (
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
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/time_zone_rf';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.time_zone_rf;

-- Часовая зона РФ: hub table

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.diff_time_zone_rf;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_time_zone_rf (
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
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/diff_time_zone_rf';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.diff_time_zone_rf;

------------------------------------------------------------------------------------------------------------------------
-- entity_statistic table
------------------------------------------------------------------------------------------------------------------------

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.entity_statistic;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.entity_statistic (
    src_table STRING,
    ctl_loading BIGINT,
    loading_date TIMESTAMP
)
partitioned by (dst_table STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/entity_statistic';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.entity_statistic;

------------------------------------------------------------------------------------------------------------------------
-- partitions_statistic table
------------------------------------------------------------------------------------------------------------------------

drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.partitions_statistic;

create external table usl_{{ logical_env_prefix }}vspc_idl_stg.partitions_statistic (
    partition_value STRING,
    ctl_loading BIGINT,
    loading_date TIMESTAMP
)
partitioned by (dst_table STRING)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/partitions_statistic';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.partitions_statistic;


drop table if exists usl_{{ logical_env_prefix }}vspc_idl_stg.chg_div_h;
create external table usl_{{ logical_env_prefix }}vspc_idl_stg.chg_div_h(
    fulldivcode string,
    fullname string,
    shortdivcode string,
    closedate date,
    opendate date,
    headdivisioncode string,
    subbranch_code string,
    requisitesdivision_bic string,
    requisitesdivision_registrationnum string,
    address_locationaddress string,
    address_addrfact_fiascode string,
    address_addrfact_timezone string,
    serving_isdisabledequip boolean,
    gold_fias_code string,
    address_addrfact_postcode string,
    address_postcode string,
    address_addrfact_addrarea_areaname string,
    address_addrfact_addrplace_placename string,
    address_addrfact_addrcity_cityname string,
    address_addrfact_addrhouse_housenumbers string,
    address_addrfact_addrstreet_streetname string,
    address_country string,
    address_addrfact_okatocode string,
    address_addrfact_oktmocode string,
    address_subjectcode string,
    address_legaladdrdoc string,
    gold_okato string,
    gold_oktmo string,
    start_dt DATE,
    end_dt DATE
)
stored as parquet
location '/data/usl/{{ logical_env_prefix_with_slash }}vspc_idl/stg/chg_div_h';

msck repair table usl_{{ logical_env_prefix }}vspc_idl_stg.chg_div_h;