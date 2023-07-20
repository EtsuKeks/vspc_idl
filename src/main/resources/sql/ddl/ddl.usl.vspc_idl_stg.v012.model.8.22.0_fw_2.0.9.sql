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

drop table if exists usl_vspc_idl_stg.addr;

create external table usl_vspc_idl_stg.addr (
    addr_dp_code STRING,
    addr_sk BIGINT,
    city_name STRING,
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    deleted_flag BOOLEAN,
    dirty_flag BOOLEAN,
    district_name STRING,
    fias_code STRING,
    house_num STRING,
    locty_name STRING,
    reg_addr_name STRING,
    rus_fed_subj_dp_code STRING,
    rus_fed_subj_sk BIGINT,
    sid STRING,
    src_txt STRING,
    street_name STRING
)

stored as parquet
location '/data/usl/vspc_idl/stg/addr';

msck repair table usl_vspc_idl_stg.addr;

-- Адрес: satellite table

drop table if exists usl_vspc_idl_stg.addr_h;

create external table usl_vspc_idl_stg.addr_h (
    addr_dp_code STRING,
    addr_sk BIGINT,
    ctl_loading BIGINT,
    ctl_validfrom TIMESTAMP,
    end_dt DATE,
    standard_addr_txt STRING,
    start_dt DATE
)
partitioned by (p1day STRING)
stored as parquet
location '/data/usl/vspc_idl/stg/addr_h';

msck repair table usl_vspc_idl_stg.addr_h;

-- Подразделение территориальной структуры: hub table

drop table if exists usl_vspc_idl_stg.div_;

create external table usl_vspc_idl_stg.div_ (
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
    sid STRING,
    subbranch_code STRING
)

stored as parquet
location '/data/usl/vspc_idl/stg/div_';

msck repair table usl_vspc_idl_stg.div_;

-- Подразделение территориальной структуры: satellite table

drop table if exists usl_vspc_idl_stg.div_h;

create external table usl_vspc_idl_stg.div_h (
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
partitioned by (p1day STRING)
stored as parquet
location '/data/usl/vspc_idl/stg/div_h';

msck repair table usl_vspc_idl_stg.div_h;

