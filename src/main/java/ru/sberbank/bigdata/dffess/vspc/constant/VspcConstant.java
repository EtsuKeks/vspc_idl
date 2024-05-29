package ru.sberbank.bigdata.dffess.vspc.constant;

import ru.sberbank.bigdata.essa.common.constant.CommonConst;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

public class VspcConstant {
    public static final String CTL_ENTITY_ID = "801005314";
    public static final String DATA_PRODUCT = "vspc_idl";
    public static final int DEPTH_DELTA = 40;
    public static final String VSPC_IDL_CUTOFF_DATE = "2021-01-01 00:00:00.0";
    public static final String RDM_IDL_SPARK = "rdm_idl_spark";

    public static final DateTimeFormatter DF_TS = DateTimeFormatter.ofPattern(CommonConst.DATE_TIME_FORMAT);

    public static String getCtlValidFromStr(){
        String rec = LocalDateTime.now().format(DF_TS);
        return rec;
    }
}
