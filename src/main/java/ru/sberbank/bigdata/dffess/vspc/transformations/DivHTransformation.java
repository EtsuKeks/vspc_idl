package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class DivHTransformation extends BaseTransformation {

    public static final String CODE = DIVH;
    public static final String SETS = format("%s.%s", DP_CODE, DIVH_DS_CODE);

    public DivHTransformation(App app) {
        super(app, CODE, SETS);
    }

    @Override
    public void run() {
        Dataset<Row> div_raw = readTable(STG_SCHEMA, CHG_DIVH);

        Dataset<Row> divh_raw = div_raw.select(
                col(FULLDIVCODE).as(DIV_SID),
                col(FULLNAME).as(DIV_H_NAME),
                col(FULLDIVCODE).as(DIV_CODE),
                col(REQUSITEDIVISION_BIC).as(DIV_PAY_SYS_PRTCPNT_SID),

                to_date(col(CLOSEDATE)).as(DIV_CLOSEDATE),
                to_date(col(OPENDATE)).as(DIV_OPENDATE),

                col(START_DT_FIELD_NAME).cast("date").as(START_DT_FIELD_NAME),
                col(END_DT_FIELD_NAME).cast("date").as(END_DT_FIELD_NAME),

                //col(ColumnsName.ADDRESS_LOCATIONADDRESS).as(ColumnsName.DIV_FACT_ADDR_SID),

                col("serving_isdisabledequip").as(DIV_DISABLED_ACCESS_EQUIPMENT),

                col(ADDRESS_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                lit(null).as(ADDRESS_FIASCODE_DURTY),
                lit(null).as(ADDR_OKATO2_SID),
                lit(null).as(ADDR_OKTMO2_SID)

        ).distinct();

        Dataset<Row> divh = divh_raw.select(
                col(DIV_SID).as(DIV_SID),
                col(DIV_H_NAME).as(DIV_H_NAME),
                col(DIV_CLOSEDATE).as(DIV_CLOSEDATE),
                col(DIV_OPENDATE).as(DIV_OPENDATE),
                col(DIV_CODE).as(DIV_CODE),
                lit(DATA_PRODUCT).as(DIV_DP_CODE),

                when(col(DIV_PAY_SYS_PRTCPNT_SID).isNotNull(), col(DIV_PAY_SYS_PRTCPNT_SID)).otherwise(null).as(DIV_PAY_SYS_PRTCPNT_SID),
                when(col(DIV_PAY_SYS_PRTCPNT_SID).isNotNull(), lit(RDM_IDL_SPARK)).otherwise(null).as(DIV_PAY_SYS_PRTCPNT_DP_CODE),


                when(col(ADDR_SRC_TXT).isNotNull(), AddrTransformation.getKey("|")).otherwise(null).as(DIV_FACT_ADDR_SID),
                when(col(ADDR_SRC_TXT).isNotNull(), lit(DATA_PRODUCT)).otherwise(null).as(DIV_FACT_ADDR_DP_CODE),

                col(DIV_DISABLED_ACCESS_EQUIPMENT).as(DIV_DISABLED_ACCESS_EQUIPMENT),

                lit(false).as(DIRTY_FLAG),
                lit(false).as(DELETED_FLAG),
                col(START_DT_FIELD_NAME).as(START_DT_FIELD_NAME),
                col(END_DT_FIELD_NAME).as(END_DT_FIELD_NAME)
        ).distinct();

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(DIV_SID, DIV);
        columnEntities.put(DIV_PAY_SYS_PRTCPNT_SID, "pay_sys_prtcpnt");
        columnEntities.put(DIV_FACT_ADDR_SID, ADDR);

        Dataset<Row> result = addSk(divh, DIVH, columnEntities);

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }

    public static void main(String[] args) {
        runApp(args, DivHTransformation::new);
    }
}
