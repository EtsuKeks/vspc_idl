package ru.sberbank.bigdata.dffess.vspc.constant;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static ru.sberbank.bigdata.sl.rt.core.Constants.TIMESTAMP_PATTERN;

public class VspcConstant {

    public static final String CTL_ENTITY_ID = "801005314";
    public static final String DATA_PRODUCT = "vspc_idl";
    public static final String DP_CODE = "usl_" + DATA_PRODUCT;
    public static final int DEPTH_DELTA = 40;
    public static final String VSPC_IDL_CUTOFF_DATE = "2021-01-01 00:00:00.0";
    public static final String RDM_IDL_SPARK = "rdm_idl_spark";
    public static final String GAR_FIAS_IDL = "gar_fias_idl";
    public static final String VSPC_SNP_SRC_SCHEMA = "usl_vspc_idl_in_snp";

    public static final DateTimeFormatter DF_TS = DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN);

    public static String getCtlValidFromStr() {
        return LocalDateTime.now().format(DF_TS);
    }
}
