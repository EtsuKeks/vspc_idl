/*opt*/show tables in usl_vspc_idl_stg;
/*opt*/select 'div__diff' t, ctl_loading, ctl_do, count(*) n, min(ctl_validfrom) vf_min, max(ctl_validfrom) vf_max from usl_vspc_idl_stg.div__diff group by ctl_loading, ctl_do order by 2, 3;
/*opt*/select 'div_h_diff' t, ctl_loading, ctl_do, count(*) n, min(ctl_validfrom) vf_min, max(ctl_validfrom) vf_max from usl_vspc_idl_stg.div_h_diff group by ctl_loading, ctl_do order by 2, 3;
/*opt*/select 'addr_h_diff' t, count(*) n from usl_vspc_idl_stg.addr_h_diff;
/*opt*/select 'div_type_diff' t, count(*) n from usl_vspc_idl_stg.div_type_diff;
/*opt*/select 'chg_stg' t, count(*) n from usl_vspc_idl_stg.chg_stg;
/*opt*/select 'ctl_log' t, * from usl_vspc_idl_stg.ctl_log;
/*opt*/select 'loader_cache_div_type' t, * from usl_vspc_idl_stg.loader_cache_div_type;
