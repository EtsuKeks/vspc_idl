-- RT-FW-style leftovers as seen on DEV (audit G1), seeded after U0 and before U1
/*opt*/create table if not exists usl_vspc_idl_stg.div__diff like usl_vspc_idl_diff.div_ location '/data/usl/vspc_idl/stg/div__diff';
/*opt*/create table if not exists usl_vspc_idl_stg.div_h_diff like usl_vspc_idl_diff.div_h location '/data/usl/vspc_idl/stg/div_h_diff';
/*opt*/create table if not exists usl_vspc_idl_stg.addr_h_diff like usl_vspc_idl_diff.addr_h location '/data/usl/vspc_idl/stg/addr_h_diff';
/*opt*/create table if not exists usl_vspc_idl_stg.div_type_diff like usl_vspc_idl_diff.div_type location '/data/usl/vspc_idl/stg/div_type_diff';
insert into usl_vspc_idl_stg.div__diff partition(ctl_loading=9000, ctl_do)
select concat('LEFT_', id), 'I', timestamp'2026-06-03 10:00:00', false, false, cast(null as date), 'usl_vspc_idl',
       date'2020-01-01', 900000000 + id, cast(null as string), cast(null as bigint), cast(null as string), cast(null as string),
       cast(null as bigint), cast(null as string), cast(null as bigint), cast(null as bigint), '01', concat('LEFT_', id), concat('0LEFT_', id),
       case when id % 2 = 0 then 'R' else 'U' end
from range(1996);
insert into usl_vspc_idl_stg.div__diff partition(ctl_loading=9000, ctl_do)
select code, 'U', timestamp'2026-06-03 10:00:00', deleted_flag, dirty_flag, div_close_dt, div_dp_code, div_open_dt, div_sk,
       div_time_zone_time_zone_rf_dp_code, div_time_zone_time_zone_rf_sk, full_reg_num, legal_addr_dp_code, legal_addr_sk,
       parent_div_dp_code, parent_div_sk, reg_ord, 'LO', sid, subbranch_code, d
from usl_vspc_idl_snp.div_ lateral view explode(array('R', 'U')) x as d where sid in ('U11_1', 'U11_2');
insert into usl_vspc_idl_stg.div_h_diff partition(ctl_loading=9000, ctl_do)
select 'I', timestamp'2026-06-03 10:00:00', false, 'usl_vspc_idl', concat('LEFTOVER ', id), 900000000 + id, date'9999-12-31',
       cast(null as string), cast(null as bigint), '999912', cast(null as string), cast(null as bigint), date'2026-06-02',
       case when id % 2 = 0 then 'R' else 'U' end
from range(1996);
insert into usl_vspc_idl_stg.div_h_diff partition(ctl_loading=9000, ctl_do)
select 'U', timestamp'2026-06-03 10:00:00', disabled_access_equipment_flag, div_dp_code, 'LEFTOVER real sk', div_sk, end_dt,
       fact_addr_dp_code, fact_addr_sk, p1month, pay_sys_prtcpnt_dp_code, pay_sys_prtcpnt_sk, date'2026-06-02', d
from usl_vspc_idl_snp.div_h lateral view explode(array('R', 'U')) x as d
where div_sk in (select div_sk from usl_vspc_idl_snp.div_ where sid in ('U11_1', 'U11_2'));
/*opt*/create table if not exists usl_vspc_idl_stg.chg_stg location '/data/usl/vspc_idl/stg/chg_stg' as select * from usl_vspc_idl_stg.chg_div_h;
/*opt*/insert into usl_vspc_idl_stg.ctl_log partition(registry_month='202606') values ('usl_ebx_rep', 'sfsdivision', 'usl_vspc_idl', 'div', 1, 1, 1, 1, timestamp'2026-06-03 10:00:00', '1');
/*opt*/insert into usl_vspc_idl_stg.ctl_log partition(registry_month='202606') values ('usl_ebx_rep', 'sfsdivision', 'usl_vspc_idl', 'div', 9000, 9000, 9000, 9000, timestamp'2026-06-03 10:00:00', '1');
