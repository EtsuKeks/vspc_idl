package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TIMEZONE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TIMEZONE_DS_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.SRC_PROVIDER_RFCZONETIME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;

public class TimeZoneTransformation extends BaseTransformation {

    public static final String PU_CODE = TIMEZONE;
    public static final String SETS = format("%s.%s", DP_CODE, TIMEZONE_DS_CODE);

    public TimeZoneTransformation(App app) {
        super(app, PU_CODE, SETS);
    }

    @Override
    public void run() {
        Dataset<Row> slice = read(SRC_PROVIDER_RFCZONETIME).kap803Usl().snp().get()
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

        // time_zone_rf_sk — НАТУРАЛЬНЫЙ ключ, строго как в 3.0: essa initSk удалял @"time_zone_rf"
        // из атрибутов ССК, а у хаба нет FK-атрибутов -> в skClient.map не уходит ничего, ключ остаётся
        // значением SELECT (abbreviation/USZ1->KALT). Материализуем его напрямую (аналог эталона R3),
        // _sid дропаем.
        Dataset<Row> result = slice
                .withColumn("time_zone_rf_sk", col(SID))
                .drop(TIME_ZONE_SID);

        publish(result)
            .recordsetMasks(sets)
            .launch();
    }

    public static void main(String[] args) {
        runApp(args, TimeZoneTransformation::new);
    }
}
