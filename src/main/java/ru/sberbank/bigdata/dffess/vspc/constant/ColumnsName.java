package ru.sberbank.bigdata.dffess.vspc.constant;

public final class ColumnsName {
    //target
    public static final String SID = "sid";

    public static final String DIRTY_FLAG = "dirty_flag";

    public static final String IFRS_ASST_BSNS_MODEL_TYPE_SK = "ifrs_asst_bsns_model_type_sk";
    public static final String IFRS_ASST_BSNS_MODEL_TYPE_DP_CODE = "ifrs_asst_bsns_model_type_dp_code";
    public static final String IFRS_SUBSQNT_ACC_MODEL_TYPE_SK = "ifrs_subsqnt_acc_model_type_sk";

    public static final String ADDR_CITY_NAME = "city_name";
    public static final String ADDR_FIAS_CODE = "fias_code";
    public static final String ADDR_HOUSE_NUM = "house_num";
    public static final String ADDR_LOCTY_NAME = "locty_name";
    public static final String ADDR_REG_ADDR_NAME = "reg_addr_name";
    public static final String ADDR_REG_FED_SUBJ_DP_CODE = "rus_fed_subj_dp_code";
    public static final String ADDR_RUS_FED_SUBJ_SK = "rus_fed_subj_sk";
    public static final String ADDR_SRC_TEXT = "src_text";
    public static final String ADDR_STREET_NAME = "street_name";
    //source
    public static final String ADDRESS_ADDRFACT_ADDRSTREET_STREETNAME = "address_addrfact_addrstreet_streetname";
    public static final String ADDRESS_ADDRFACT_ADDRSTREET_HOUSENUMBERS = "address_addrfact_addrhouse_housenumbers";
    public static final String ADDRESS_ADDRFACT_FIASCODE = "address_addrfact_fiascode";
    public static final String ADDRESS_ADDRFACT_ADDRCITY_CITYNAME = "address_addrfact_addrcity_cityname";
    public static final String ADDRESS_LOCATIONADDRESS = "address_locationaddress";
    public static final String ID = "id";
    public static final String ADDINFORMATION = "address_addrfact_addinformation";
    public static final String SYSTEM_ID = "address_addrfact_propertymanagmentsystemid";


    private ColumnsName() {
        //no_op
    }
}
