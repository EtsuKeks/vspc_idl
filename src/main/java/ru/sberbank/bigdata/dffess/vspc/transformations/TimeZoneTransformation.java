package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;
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
        Dataset<Row> slice = read(SRC_PROVIDER_RFCZONETIME).kap803Usl().snp().get()
                .select(
                        lit(false).as(DELETED_FLAG),
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(SID),
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(TIME_ZONE_ABBR_CODE),
                        col(CODE).as(TIME_ZONE_CODE),
                        col(TIMEDESIGNATION).as(TIME_ZONE_MSK_SHIFT_VAL),
                        col(NAME00).as(TIME_ZONE_NAME),
                        lit(DATA_PRODUCT).as(TIME_ZONE_DP_CODE),

                        // *_sid — натуральный ключ из источника; addSk отдаёт его в ССК и заменяет на суррогатный *_sk подчищая *_sid после себя.
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(TIME_ZONE_SID),

                        col(UTCFORMAT).as(TIME_ZONE_WRLD_SHIFT_VAL),
                        lit(false).as(DIRTY_FLAG)
                ).where(col(ABBREVIATION).isNotNull())
                .distinct();

        Dataset<Row> result = addSk(slice, TIMEZONE, Arrays.asList("@time_zone_rf"));

        // Просто вызвать publish нельзя даже с пропатченным RT-FW: до его патча публикатор молча выбросит колонку с другим типом
        // и сразу выйдет на пустом инкременте. Поэтому сначала приводим и проверяем типы и корректно обрабатываем пустой INCR в своем собственном методе.
        // Там же вдобавок публикуем статистики в CTL: помечаем набор изменённым, и по этой пометке RT-FW их публикует.
        publishChecked(result);
    }
}
