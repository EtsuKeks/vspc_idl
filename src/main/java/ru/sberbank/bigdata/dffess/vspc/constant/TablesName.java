package ru.sberbank.bigdata.dffess.vspc.constant;

public class TablesName {

    //target
    public static final String ADDRH = "addr_h";
    public static final String ADDR = "addr";
    public static final String DIVH = "div_h";
    public static final String DIV = "div";
    public static final String DIVTYPE = "div_type";
    public static final String TERRDIVTYPE = "terr_div_type";
    public static final String TIMEZONE = "time_zone_rf";
    public static final String DIVDIVRELATION = "div_div_relation";

    //target dataset code
    public static final String ADDRH_DS_CODE = ADDR + ".sat=h";
    public static final String ADDR_DS_CODE = ADDR + ".hub";
    public static final String DIVH_DS_CODE = DIV + ".sat=h";
    public static final String DIV_DS_CODE = DIV + ".hub";
    public static final String DIVTYPE_DS_CODE = DIV + ".sat=type";
    public static final String TERRDIVTYPE_DS_CODE = TERRDIVTYPE + ".hub";
    public static final String TIMEZONE_DS_CODE = TIMEZONE + ".hub";
    public static final String DIVDIVRELATION_DS_CODE = "div&div_relation.link";

    //source
    public static final String EBX_REP_M_SFS_0002_SFSDIVISION = "ebx_rep_m_sfs_0002_sfsdivision";
    public static final String EBX_REP_M_SFS_0002_SFSDIVISION_SUCCESSORS = "ebx_rep_m_sfs_0002_sfsdivision_successors";
    public static final String EBX_REP_M_RFCZONETIME_0002_RFCZONETIME = "ebx_rep_m_rfzonetime_0002_rfzonetime";

    //temporary
    public static final String CHG_DIVH = "chg_div_h";

    private TablesName() {
        //no_op
    }
}
