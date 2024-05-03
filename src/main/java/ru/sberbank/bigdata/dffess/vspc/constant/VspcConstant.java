package ru.sberbank.bigdata.dffess.vspc.constant;

import java.util.Calendar;
import java.util.Date;

public class VspcConstant {
    public static final String CTL_ENTITY_ID = "801005314";
    public static final String DATA_PRODUCT = "vspc_idl";
    public static final int DEPTH_DELTA = 40;
    public static final String VSPC_IDL_CUTOFF_DATE = "2021-01-01 00:00:00.0";
    public static final String RDM_IDL_SPARK = "rdm_idl_spark";

    public static Date getCurrentHourCutOffDate(){


        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }
}
