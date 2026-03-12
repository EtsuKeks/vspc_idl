package ru.sberbank.bigdata.dffess.vspc.tables.chgdivh;

import static ru.sberbank.bigdata.essa.common.constant.CommonConst.LoadMode;
import static ru.sberbank.bigdata.essa.common.constant.CommonConst.LoadMode.INCR;

public class ChgDivHSql {

    public static String getPreselectSQL(LoadMode loadMode) {

        String sql;

        if (loadMode.equals(INCR)) {
//             Берем из snp
            sql = " SELECT  " +
                    "     fulldivcode, " +
                    "     fullname, " +
                    "     shortdivcode, " +
                    "     cast(closedate as date) as closedate, " +
                    "     cast(opendate as date) as opendate,  " +
                    "     headdivisioncode, " +
                    "     '0'||fulldivcode as subbranch_code, " +
                    "     pay_sys_prtcpnt.sid as requisitesdivision_bic, " +
                    "     requisitesdivision_registrationnum, " +
                    "     nullif(trim(address_locationaddress),'') as address_locationaddress, " +
                    "     lower(address_addrfact_fiascode) as address_addrfact_fiascode, " +
                    "     address_addrfact_timezone,  " +
                    "     case when serving_isdisabledequip = 1 then true else false end serving_isdisabledequip,  " +
                    "     gar_fias_addr.sid as gold_fias_code, " +
                    "     nullif(trim(address_addrfact_postcode),'') as address_addrfact_postcode, " +
                    "     nullif(trim(address_postcode),'') as address_postcode, " +
                    "     nullif(trim(address_addrfact_addrarea_areaname),'') as address_addrfact_addrarea_areaname, " +
                    "     nullif(trim(address_addrfact_addrplace_placename),'') as address_addrfact_addrplace_placename, " +
                    "     nullif(trim(address_addrfact_addrcity_cityname),'') as address_addrfact_addrcity_cityname, " +
                    "     nullif(trim(address_addrfact_addrhouse_housenumbers),'') as address_addrfact_addrhouse_housenumbers, " +
                    "     nullif(trim(address_addrfact_addrstreet_streetname),'') as address_addrfact_addrstreet_streetname, " +
                    "     nullif(trim(address_country),'') as address_country, " +
                    "     nullif(trim(address_addrfact_okatocode),'') as address_addrfact_okatocode, " +
                    "     nullif(trim(address_addrfact_oktmocode),'') as address_addrfact_oktmocode, " +
                    "     nullif(trim(address_subjectcode),'') as address_subjectcode, " +
                    "     nullif(trim(address_legaladdrdoc),'') as address_legaladdrdoc, " +
                    "     okato2_gold.sid as gold_okato, " +
                    "     oktmo2_gold.sid as gold_oktmo, " +
                    "     cast(current_date() as date) as start_dt, " +
                    "     cast('9999-12-31' as date) end_dt " +
                    " FROM  " +
                    "     usl_vspc_idl_in_snp.ebx_rep_m_sfs_0002_sfsdivision vsp_fias  " +
                    " left join (  " +
                    "     select " +
                    "         distinct " +
                    "             sid  " +
                    "     from " +
                    "         usl_gar_fias_idl_snp.addr  " +
                    "     WHERE  " +
                    "         NOT COALESCE(deleted_flag, FALSE) " +
                    "         AND NOT COALESCE(dirty_flag, FALSE)  " +
                    " ) gar_fias_addr  " +
                    " on vsp_fias.address_addrfact_fiascode = gar_fias_addr.sid " +
                    " left join (  " +
                    "     select   " +
                    "         distinct sid" +
                    "     from   " +
                    "         usl_rdm_idl_spark_snp.okato2  " +
                    "         where   " +
                    "             NOT COALESCE(deleted_flag, FALSE)    " +
                    "             AND NOT COALESCE(dirty_flag, FALSE)    " +
                    " ) okato2_gold  " +
                    "     on okato2_gold.sid = vsp_fias.address_addrfact_okatocode  " +
                    " left join (  " +
                    "     select   " +
                    "         distinct sid  " +
                    "     from   " +
                    "         usl_rdm_idl_spark_snp.oktmo2  " +
                    "         where   " +
                    "             NOT COALESCE(deleted_flag, FALSE)   " +
                    "             AND NOT COALESCE(dirty_flag, FALSE)    " +
                    " ) oktmo2_gold  " +
                    "     on oktmo2_gold.sid = vsp_fias.address_addrfact_oktmocode " +
                    " left join (  " +
                    "     select " +
                    "         distinct sid" +
                    "     from " +
                    "         usl_rdm_idl_spark_snp.pay_sys_prtcpnt  " +
                    "     WHERE  " +
                    "         NOT COALESCE(deleted_flag, FALSE) " +
                    "         AND NOT COALESCE(dirty_flag, FALSE)  " +
                    " ) pay_sys_prtcpnt  " +
                    " on vsp_fias.requisitesdivision_bic = pay_sys_prtcpnt.sid ";;
        } else {
            sql = "  SELECT" +
                    "     fulldivcode, " +
                    "     fullname, " +
                    "     shortdivcode, " +
                    "     cast(closedate as date) as closedate, " +
                    "     cast(opendate as date) as opendate,  " +
                    "     headdivisioncode, " +
                    "     div_subbranch_code as subbranch_code, " +
                    "     pay_sys_prtcpnt.sid as requisitesdivision_bic, " +
                    "     requisitesdivision_registrationnum, " +
                    "     address_locationaddress, " +
                    "     address_addrfact_fiascode, " +
                    "     address_addrfact_timezone," +
                    "     serving_isdisabledequip," +
                    "     gar_fias_addr.sid as gold_fias_code,  " +
                    "     address_postcode, " +
                    "     address_addrfact_addrarea_areaname, " +
                    "     address_addrfact_addrplace_placename, " +
                    "     address_addrfact_addrcity_cityname, " +
                    "     address_addrfact_addrhouse_housenumbers, " +
                    "     address_addrfact_addrstreet_streetname, " +
                    "     address_country, " +
                    "     address_addrfact_okatocode, " +
                    "     address_addrfact_oktmocode, " +
                    "     address_subjectcode, " +
                    "     address_legaladdrdoc, " +
                    "     okato2_gold.sid as gold_okato, " +
                    "     oktmo2_gold.sid as gold_oktmo, " +
                    "     start_dt, " +
                    "     end_dt " +
                    "     FROM ( " +
                    "         SELECT " +
                    "         fulldivcode, " +
                    "         fullname, " +
                    "         shortdivcode, " +
                    "         closedate, " +
                    "         opendate, " +
                    "         headdivisioncode, " +
                    "         div_subbranch_code, " +
                    "         requisitesdivision_bic, " +
                    "         requisitesdivision_registrationnum, " +
                    "         address_locationaddress, " +
                    "         lower(address_addrfact_fiascode) as address_addrfact_fiascode, " +
                    "         address_addrfact_timezone," +
                    "         case when serving_isdisabledequip = 1 then true else false end serving_isdisabledequip," +
                    "         address_addrfact_postcode, " +
                    "         address_postcode, " +
                    "         address_addrfact_addrarea_areaname," +
                    "         address_addrfact_addrplace_placename," +
                    "         address_subjectcode," +
                    "         address_addrfact_addrcity_cityname," +
                    "         address_addrfact_addrhouse_housenumbers," +
                    "         address_addrfact_addrstreet_streetname," +
                    "         address_country," +
                    "         address_addrfact_okatocode," +
                    "         address_addrfact_oktmocode," +
                    "         address_legaladdrdoc, " +
                    "         start_dt," +
                    "         end_dt " +
                    "         FROM  " +
                    "         ( " +
                    "             SELECT " +
                    "                 fulldivcode, " +
                    "                 fullname, " +
                    "                 shortdivcode, " +
                    "                 closedate, " +
                    "                 opendate, " +
                    "                 headdivisioncode, " +
                    "                 div_subbranch_code, " +
                    "                 requisitesdivision_bic, " +
                    "                 requisitesdivision_registrationnum, " +
                    "                 address_locationaddress, " +
                    "                 address_addrfact_fiascode, " +
                    "                 address_addrfact_timezone," +
                    "                 serving_isdisabledequip," +
                    "                 address_addrfact_postcode," +
                    "                 address_postcode," +
                    "                 address_addrfact_addrarea_areaname," +
                    "                 address_addrfact_addrplace_placename," +
                    "                 address_subjectcode," +
                    "                 address_addrfact_addrcity_cityname," +
                    "                 address_addrfact_addrhouse_housenumbers," +
                    "                 address_addrfact_addrstreet_streetname," +
                    "                 address_country," +
                    "                 address_addrfact_okatocode," +
                    "                 address_addrfact_oktmocode," +
                    "                 address_legaladdrdoc, " +
                    "                 start_dt," +
                    "                 COALESCE(CAST(lead(start_dt) OVER(PARTITION BY fulldivcode ORDER BY start_dt) AS DATE) - INTERVAL '1' DAY,to_date('9999-12-31')) AS end_dt " +
                    "             FROM  " +
                    "             ( " +
                    "                 SELECT " +
                    "                     fulldivcode, " +
                    "                     fullname, " +
                    "                     shortdivcode, " +
                    "                     closedate, " +
                    "                     opendate, " +
                    "                     headdivisioncode, " +
                    "                     div_subbranch_code, " +
                    "                     requisitesdivision_bic, " +
                    "                     requisitesdivision_registrationnum, " +
                    "                     address_locationaddress, " +
                    "                     address_addrfact_fiascode, " +
                    "                     address_addrfact_timezone," +
                    "                     serving_isdisabledequip," +
                    "                     address_addrfact_postcode," +
                    "                     address_postcode," +
                    "                     address_addrfact_addrarea_areaname," +
                    "                     address_addrfact_addrplace_placename," +
                    "                     address_subjectcode," +
                    "                     address_addrfact_addrcity_cityname," +
                    "                     address_addrfact_addrhouse_housenumbers," +
                    "                     address_addrfact_addrstreet_streetname," +
                    "                     address_country," +
                    "                     address_addrfact_okatocode," +
                    "                     address_addrfact_oktmocode," +
                    "                     address_legaladdrdoc, " +
                    "                     hash, " +
                    "                     COALESCE(lag(hash) OVER(PARTITION BY fulldivcode ORDER BY start_dt asc),'-9999999999') AS lag_hsh, " +
                    "                     start_dt " +
                    "                 FROM " +
                    "                 ( " +
                    "                     SELECT " +
                    "                         fulldivcode, " +
                    "                         fullname, " +
                    "                         shortdivcode, " +
                    "                         closedate, " +
                    "                         opendate, " +
                    "                         headdivisioncode, " +
                    "                         '0'||fulldivcode as div_subbranch_code, " +
                    "                         requisitesdivision_bic, " +
                    "                         requisitesdivision_registrationnum, " +
                    "                         nullif(trim(address_locationaddress), '') as address_locationaddress, " +
                    "                         nullif(trim(address_addrfact_fiascode), '') as address_addrfact_fiascode, " +
                    "                         nullif(trim(address_addrfact_timezone), '') as address_addrfact_timezone," +
                    "                         nullif(trim(serving_isdisabledequip), '') as serving_isdisabledequip," +
                    "                         nullif(trim(address_addrfact_postcode),'') as address_addrfact_postcode, " +
                    "                         nullif(trim(address_postcode),'') as address_postcode, " +
                    "                         nullif(trim(address_addrfact_addrarea_areaname), '') as address_addrfact_addrarea_areaname," +
                    "                         nullif(trim(address_addrfact_addrplace_placename), '') as address_addrfact_addrplace_placename," +
                    "                         nullif(trim(address_subjectcode), '') as address_subjectcode," +
                    "                         nullif(trim(address_addrfact_addrcity_cityname), '') as address_addrfact_addrcity_cityname," +
                    "                         nullif(trim(address_addrfact_addrhouse_housenumbers), '') as address_addrfact_addrhouse_housenumbers," +
                    "                         nullif(trim(address_addrfact_addrstreet_streetname), '') as address_addrfact_addrstreet_streetname," +
                    "                         nullif(trim(address_country), '') as address_country," +
                    "                         nullif(trim(address_addrfact_okatocode), '') as address_addrfact_okatocode," +
                    "                         nullif(trim(address_addrfact_oktmocode), '') as address_addrfact_oktmocode," +
                    "                         nullif(trim(address_legaladdrdoc), '') as address_legaladdrdoc, " +
                    "                         hash( " +
                    "                             fulldivcode, " +
                    "                             fullname, " +
                    "                             requisitesdivision_bic, " +
                    "                             nullif(trim(address_locationaddress), ''), " +
                    "                             nullif(trim(address_addrfact_fiascode), ''), " +
                    "                             serving_isdisabledequip  " +
                    "                         ) hash, " +
                    "                         ctl_validfrom AS start_dt " +
                    "                     FROM " +
                    "                         usl_vspc_idl_in_diff.ebx_rep_d_sfs_0002_sfsdivision " +
                    "                     WHERE " +
                    "                         snapshot_ LIKE '%DIF%' " +
                    "                         AND ctl_do = 'R' " +
                    " " +
                    "                     UNION " +
                    " " +
                    "                     SELECT " +
                    "                         fulldivcode, " +
                    "                         fullname, " +
                    "                         shortdivcode, " +
                    "                         closedate, " +
                    "                         opendate, " +
                    "                         headdivisioncode, " +
                    "                         '0'||fulldivcode as div_subbranch_code, " +
                    "                         requisitesdivision_bic, " +
                    "                         requisitesdivision_registrationnum, " +
                    "                         nullif(trim(address_locationaddress), '') as address_locationaddress, " +
                    "                         nullif(trim(address_addrfact_fiascode), '') as address_addrfact_fiascode, " +
                    "                         nullif(trim(address_addrfact_timezone), '') as address_addrfact_timezone," +
                    "                         serving_isdisabledequip," +
                    "                         nullif(trim(address_addrfact_postcode),'') as address_addrfact_postcode, " +
                    "                         nullif(trim(address_postcode),'') as address_postcode, " +
                    "                         nullif(trim(address_addrfact_addrarea_areaname), '') as address_addrfact_addrarea_areaname," +
                    "                         nullif(trim(address_addrfact_addrplace_placename), '') as address_addrfact_addrplace_placename," +
                    "                         nullif(trim(address_subjectcode), '') as address_subjectcode," +
                    "                         nullif(trim(address_addrfact_addrcity_cityname), '') as address_addrfact_addrcity_cityname," +
                    "                         nullif(trim(address_addrfact_addrhouse_housenumbers), '') as address_addrfact_addrhouse_housenumbers," +
                    "                         nullif(trim(address_addrfact_addrstreet_streetname), '') as address_addrfact_addrstreet_streetname," +
                    "                         nullif(trim(address_country), '') as address_country," +
                    "                         nullif(trim(address_addrfact_okatocode), '') as address_addrfact_okatocode," +
                    "                         nullif(trim(address_addrfact_oktmocode), '') as address_addrfact_oktmocode," +
                    "                         nullif(trim(address_legaladdrdoc), '') as address_legaladdrdoc," +
                    "                         hash(" +
                    "                             fulldivcode, " +
                    "                             fullname, " +
                    "                             requisitesdivision_bic, " +
                    "                             nullif(trim(address_locationaddress), ''), " +
                    "                             nullif(trim(address_addrfact_fiascode), ''), " +
                    "                             serving_isdisabledequip " +
                    "                         ) hash," +
                    "                         CURRENT_DATE() AS start_dt " +
                    "                     FROM " +
                    "                         usl_vspc_idl_in_snp.ebx_rep_m_sfs_0002_sfsdivision " +
                    "                 ) t " +
                    "             ) t1  " +
                    "             WHERE " +
                    "                 CAST(COALESCE(lag_hsh, '-9999999999') AS string) <> CAST(hash AS string)  " +
                    "         ) t2  " +
                    "         WHERE  " +
                    "             datediff(end_dt, start_dt) >= 0  " +
                    "     ) vsp_fias  " +
                    "     left join (  " +
                    "         select distinct  " +
                    "             sid " +
                    "         from  " +
                    "             usl_gar_fias_idl_snp.addr  " +
                    "         WHERE " +
                    "             NOT COALESCE(deleted_flag, FALSE) " +
                    "             AND NOT COALESCE(dirty_flag, FALSE) " +
                    "     ) gar_fias_addr  " +
                    "     on vsp_fias.address_addrfact_fiascode = gar_fias_addr.sid " +
                    " left join (  " +
                    "     select   " +
                    "         distinct sid  " +
                    "     from   " +
                    "         usl_rdm_idl_spark_snp.okato2  " +
                    "         where   " +
                    "             NOT COALESCE(deleted_flag, FALSE)    " +
                    "             AND NOT COALESCE(dirty_flag, FALSE)    " +
                    " ) okato2_gold  " +
                    "     on okato2_gold.sid = vsp_fias.address_addrfact_okatocode  " +
                    " left join (  " +
                    "     select   " +
                    "         distinct sid  " +
                    "     from   " +
                    "         usl_rdm_idl_spark_snp.oktmo2  " +
                    "         where   " +
                    "             NOT COALESCE(deleted_flag, FALSE) " +
                    "             AND NOT COALESCE(dirty_flag, FALSE)    " +
                    " ) oktmo2_gold  " +
                    "     on oktmo2_gold.sid = vsp_fias.address_addrfact_oktmocode " +
                    " left join (  " +
                    "     select " +
                    "          distinct sid  " +
                    "     from " +
                    "         usl_rdm_idl_spark_snp.pay_sys_prtcpnt  " +
                    "     WHERE  " +
                    "         NOT COALESCE(deleted_flag, FALSE) " +
                    "         AND NOT COALESCE(dirty_flag, FALSE)  " +
                    " ) pay_sys_prtcpnt  " +
                    " on vsp_fias.requisitesdivision_bic = pay_sys_prtcpnt.sid ";;
        }
        return sql;
    }
}