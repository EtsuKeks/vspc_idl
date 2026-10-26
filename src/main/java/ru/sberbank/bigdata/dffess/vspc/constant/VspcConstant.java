package ru.sberbank.bigdata.dffess.vspc.constant;

public class VspcConstant {

    public static final String DATA_PRODUCT = "vspc_idl";
    public static final String RDM_IDL_SPARK = "rdm_idl_spark";
    public static final String GAR_FIAS_IDL = "gar_fias_idl";

    // Коды входных наборов данных (providerDatasetCode из design-time metadata) для чтения через RT-FW Reader.
    // Без суффикса слоя: Reader сам добавляет _snp/_diff в зависимости от выбранной опции чтения.
    public static final String SRC_PROVIDER_SFSDIVISION = "usl_vspc_idl_in.ebx_rep_m_sfs_0002_sfsdivision";
    public static final String SRC_PROVIDER_SFSDIVISION_SUCCESSORS =
            "usl_vspc_idl_in." + TablesName.EBX_REP_M_SFS_0002_SFSDIVISION_SUCCESSORS;
    public static final String SRC_PROVIDER_RFCZONETIME = "usl_vspc_idl_in.ebx_rep_m_rfzonetime_0002_rfzonetime";
}
