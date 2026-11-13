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
    // Коды входных наборов из meta (providerDatasetCode) — по ним читает reader RT-FW. Без суффикса слоя: reader сам добавляет _snp/_diff.
    public static final String EBX_REP_M_SFS_0002_SFSDIVISION = "usl_vspc_idl_in.ebx_rep_m_sfs_0002_sfsdivision";
    public static final String EBX_REP_M_SFS_0002_SFSDIVISION_SUCCESSORS = "usl_vspc_idl_in.ebx_rep_m_sfs_0002_sfsdivision_successors";
    public static final String EBX_REP_M_RFZONETIME_0002_RFZONETIME = "usl_vspc_idl_in.ebx_rep_m_rfzonetime_0002_rfzonetime";

    //temporary
    public static final String CHG_DIVH = "chg_div_h";

    private TablesName() {
        //no_op
    }
}
