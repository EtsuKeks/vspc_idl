package ru.sberbank.bigdata.dffess.vspc.constant;

public final class ColumnsName {

    //    SOURCE

    // EBX_REP_M_SFS_0002_SFSDIVISION
    public static final String ADDRESS_ADDRFACT_ADDRAREA_AREANAME = "address_addrfact_addrarea_areaname";
    public static final String ADDRESS_ADDRFACT_ADDRCITY_CITYNAME = "address_addrfact_addrcity_cityname";
    public static final String ADDRESS_ADDRFACT_ADDRSTREET_HOUSENUMBERS = "address_addrfact_addrhouse_housenumbers";
    public static final String ADDRESS_ADDRFACT_ADDPLACE_PLACENAME = "address_addrfact_addrplace_placename";
    public static final String ADDRESS_ADDRFACT_ADDRSTREET_STREETNAME = "address_addrfact_addrstreet_streetname";
    public static final String ADDRESS_ADDRFACT_FIASCODE = "address_addrfact_fiascode";
    public static final String ADDRESS_OKATO2_CODE = "address_addrfact_okatocode";
    public static final String ADDRESS_ADDRFACT_POSTCODE = "address_addrfact_postcode";
    public static final String ADDRESS_ADDRFACT_TIMEZONE = "address_addrfact_timezone";
    public static final String ADDRESS_LEGAL_LOCATIONADDRESS = "address_legaladdrdoc";
    public static final String ADDRESS_LOCATIONADDRESS = "address_locationaddress";
    public static final String ADDRESS_POSTCODE = "address_postcode";
    public static final String ADDRESS_SUBJECTCODE = "address_subjectcode";
    public static final String ADDRESS_COUNTRY = "address_country";
    public static final String CLOSEDATE = "closedate";
    public static final String DIVISIONTYPE00 = "divisiontype00";
    public static final String FULLDIVCODE = "fulldivcode";
    public static final String FULLNAME = "fullname";
    public static final String HEADDIVISIONCODE = "headdivisioncode";
    public static final String OPENDATE = "opendate";
    public static final String REQUSITEDIVISION_BIC = "requisitesdivision_bic";
    public static final String DIV_REQUISITEDIVISION_REGISTRATIONNUM = "requisitesdivision_registrationnum";
    public static final String SHORTDIVCODE = "shortdivcode";

    // EBX_REP_M_RFCZONETIME_0002_RFCZONETIME
    public static final String ABBREVIATION = "abbreviation";
    public static final String CODE = "code";
    public static final String TIMEDESIGNATION = "timedesignation";
    public static final String NAME00 = "name00";
    public static final String UTCFORMAT = "utcformat";

    //    TARGET

    // common
    public static final String ADDR_DP_CODE = "addr_dp_code";
    public static final String ADDR_SK = "addr_sk";
    public static final String DIRTY_FLAG = "dirty_flag";
    public static final String DIV_DP_CODE = "div_dp_code";
    public static final String DIV_SK = "div_sk";

    // ADDR
    public static final String ADDR_CITY_NAME = "city_name";
    public static final String ADDR_CNTRY_DP_CODE = "cntry_dp_code";
    public static final String ADDR_CNTRY_SK = "cntry_sk";
    public static final String ADDR_DISTRICT_NAME = "district_name";
    public static final String ADDR_FIAS_CODE = "fias_code";
    public static final String ADDR_HOUSE_NUM = "house_num";
    public static final String ADDR_IDX_NUM = "idx_num";
    public static final String ADDR_LOCTY_NAME = "locty_name";
    public static final String ADDR_OKATO2_DP_CODE = "okato2_dp_code";
    public static final String ADDR_OKATO2_SK = "okato2_sk";
    public static final String ADDR_REG_ADDR_NAME = "reg_addr_name";
    public static final String ADDR_REG_FED_SUBJ_DP_CODE = "rus_fed_subj_dp_code";
    public static final String ADDR_RUS_FED_SUBJ_SK = "rus_fed_subj_sk";
    public static final String ADDR_SRC_TXT = "src_txt";
    public static final String ADDR_STREET_NAME = "street_name";

    // ADDR_H
    public static final String ADDR_STANDART_ADDRESS_TXT = "standard_addr_txt";

    // DIV_
    public static final String DIV_CODE = "code";
    public static final String DIV_CLOSEDATE = "div_close_dt";
    public static final String DIV_OPENDATE = "div_open_dt";
    public static final String DIV_TIME_ZONE_TIME_ZONE_DP_CODE = "div_time_zone_time_zone_rf_dp_code";
    public static final String DIV_TIME_ZONE_TIME_ZONE_SK = "div_time_zone_time_zone_rf_sk";
    public static final String DIV_FULL_REG_NUM = "full_reg_num";
    public static final String DIV_LEGAL_ADDR_DP_CODE = "legal_addr_dp_code";
    public static final String DIV_LEGAL_ADDR_SK = "legal_addr_sk";
    public static final String DIV_PARENT_DIV_DP_CODE = "parent_div_dp_code";
    public static final String DIV_PARENT_DIV_SK = "parent_div_sk";
    public static final String DIV_REG_ORD = "reg_ord";
    public static final String DIV_SHORT_DIV_CODE = "short_div_code";

    // DIV_H
    public static final String DIV_SUBBRANCH_CODE = "subbranch_code";
    public static final String DIV_H_NAME = "div_name";
    public static final String DIV_FACT_ADDR_DP_CODE = "fact_addr_dp_code";
    public static final String DIV_FACT_ADDR_SK = "fact_addr_sk";
    public static final String DIV_PAY_SYS_PRTCPNT_DP_CODE = "pay_sys_prtcpnt_dp_code";
    public static final String DIV_PAY_SYS_PRTCPNT_SK = "pay_sys_prtcpnt_sk";

    // DIV_TYPE
    public static final String DIV_TYPE_TERR_DIV_TYPE_DP_CODE = "div_type_terr_div_type_dp_code";
    public static final String DIV_TYPE_TERR_DIV_TYPE_SK = "div_type_terr_div_type_sk";

    // TERR_DIV_TYPE
    public static final String DIV_TYPE_CODE = "div_type_code";
    public static final String TERR_DIV_TYPE_DP_CODE = "terr_div_type_dp_code";
    public static final String TERR_DIV_TYPE_SK = "terr_div_type_sk";

    // TIME_ZONE_RF
    public static final String TIME_ZONE_ABBR_CODE = "time_zone_abbr_code";
    public static final String TIME_ZONE_CODE = "time_zone_code";
    public static final String TIME_ZONE_MSK_SHIFT_VAL = "time_zone_msk_shift_val";
    public static final String TIME_ZONE_NAME = "time_zone_name";
    public static final String TIME_ZONE_DP_CODE = "time_zone_rf_dp_code";
    public static final String TIME_ZONE_SK = "time_zone_rf_sk";
    public static final String TIME_ZONE_WRLD_SHIFT_VAL = "time_zone_wrld_shift_val";

    private ColumnsName() {
        //no_op
    }
}
