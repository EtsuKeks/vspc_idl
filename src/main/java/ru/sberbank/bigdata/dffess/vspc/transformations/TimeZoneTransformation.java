package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_M_RFCZONETIME_0002_RFCZONETIME;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TIMEZONE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TIMEZONE_DS_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.VSPC_SNP_SRC_SCHEMA;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;

public class TimeZoneTransformation extends BaseTransformation {

    public static final String PU_CODE = TIMEZONE;
    public static final String SETS = format("%s.%s", DP_CODE, TIMEZONE_DS_CODE);

    public TimeZoneTransformation(App app) {
        super(app, PU_CODE, SETS);
    }

    @Override
    public void run() {
        Dataset<Row> slice = selectFrom(VSPC_SNP_SRC_SCHEMA, EBX_REP_M_RFCZONETIME_0002_RFCZONETIME)
                .select(
                        lit(false).as(DELETED_FLAG),
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(SID),
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(TIME_ZONE_ABBR_CODE),
                        col(CODE).as(TIME_ZONE_CODE),
                        col(TIMEDESIGNATION).as(TIME_ZONE_MSK_SHIFT_VAL),
                        col(NAME00).as(TIME_ZONE_NAME),
                        lit(DATA_PRODUCT).as(TIME_ZONE_DP_CODE),
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(TIME_ZONE_SID),
                        col(UTCFORMAT).as(TIME_ZONE_WRLD_SHIFT_VAL),
                        lit(false).as(DIRTY_FLAG)
                ).where(col(ABBREVIATION).isNotNull())
                .distinct();

        Dataset<Row> result = addSk(slice, TIMEZONE, "@time_zone_rf");

        publish(result)
            .recordsetMasks(sets)
            .launch();
    }

    public static void main(String[] args) {
        runApp(args, TimeZoneTransformation::new);
    }
}
