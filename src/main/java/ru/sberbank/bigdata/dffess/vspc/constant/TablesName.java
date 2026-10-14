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
