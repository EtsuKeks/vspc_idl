package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.PublisherMode;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Recordset;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TIMEZONE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.SRC_PROVIDER_RFCZONETIME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;

public class TimeZoneTransformation extends BaseTransformation {

    public static final String PU_CODE = TIMEZONE;

    public TimeZoneTransformation(App app) {
        super(app, PU_CODE, "usl_vspc_idl.time_zone_rf.hub");
    }

    @Override
    public void run() {
        Dataset<Row> time_zone = read(SRC_PROVIDER_RFCZONETIME).kap803Usl().snp().get()
                .select(
                        lit(false).as(DELETED_FLAG),
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(SID),
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(TIME_ZONE_ABBR_CODE),
                        col(CODE).cast("string").as(TIME_ZONE_CODE),
                        col(TIMEDESIGNATION).as(TIME_ZONE_MSK_SHIFT_VAL),
                        col(NAME00).as(TIME_ZONE_NAME),
                        lit(DATA_PRODUCT).as(TIME_ZONE_DP_CODE),
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(TIME_ZONE_SID),
                        col(UTCFORMAT).as(TIME_ZONE_WRLD_SHIFT_VAL),
                        lit(false).as(DIRTY_FLAG)
                ).where(col(ABBREVIATION).isNotNull())
                .distinct();

        // Аббревиатура USZ1 переводится в KALT — так же в 3.0 (TimeZoneTransformer.java:32,33,38);
        // отражение повторяется трижды, потому что SID, abbr_code и sid берут одно и то же значение.
        Dataset<Row> result = addSk(time_zone, TIMEZONE, Arrays.asList("@time_zone_rf")).dropDuplicates("time_zone_rf_sk");
        publish(result)
                .calculationMode(PublisherMode.sparkRebalance())
                .recordsetMasks("usl_vspc_idl.time_zone_rf.hub")
                .launch();
        registerChangedSets(Recordset.recordsets(getApp(), "usl_vspc_idl.time_zone_rf.hub"));
    }
}
