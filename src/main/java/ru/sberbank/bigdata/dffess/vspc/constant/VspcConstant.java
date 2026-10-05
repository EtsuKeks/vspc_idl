package ru.sberbank.bigdata.dffess.vspc.constant;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static ru.sberbank.bigdata.sl.rt.core.Constants.TIMESTAMP_PATTERN;

public class VspcConstant {

    public static final String CTL_ENTITY_ID = "801005314";
    public static final String DATA_PRODUCT = "vspc_idl";
    public static final String DP_CODE = "usl_" + DATA_PRODUCT;
    public static final String MIN_BUSINESS_DT = "1900-01-01";
    public static final String RDM_IDL_SPARK = "rdm_idl_spark";
    public static final String GAR_FIAS_IDL = "gar_fias_idl";
    public static final String VSPC_SNP_SRC_SCHEMA = "unsi_sp_sfs_006_snp_snp";

    // Коды входных наборов данных (providerDatasetCode из design-time metadata) для чтения через RT-FW Reader.
    // Без суффикса слоя: Reader сам добавляет _snp/_diff в зависимости от выбранной опции чтения.
    // Источник переведён с ebx-реплик (usl_vspc_idl_in.*) на УНСИ (unsi_sp_sfs_006 / unsi_sp_rfznt_002), well v022.
    public static final String SRC_PROVIDER_SFSDIVISION = "unsi_sp_sfs_006." + TablesName.R_M_SFS_0006_SFSDIVISION;
    public static final String SRC_PROVIDER_SFSDIVISION_SUCCESSORS =
            "unsi_sp_sfs_006." + TablesName.R_M_SFS_0006_SUCCESSORS;
    public static final String SRC_PROVIDER_RFCZONETIME = "unsi_sp_rfznt_002." + TablesName.R_M_RFCZONETIME_0002_RFCZONETIME;

    public static final DateTimeFormatter DF_TS = DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN);

    public static String getCtlValidFromStr() {
        return LocalDateTime.now().format(DF_TS);
    }
}
