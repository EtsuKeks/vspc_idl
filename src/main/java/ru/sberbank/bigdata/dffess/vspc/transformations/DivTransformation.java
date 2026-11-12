package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;

public class DivTransformation extends BaseTransformation {

    public static final String CODE = DIV;

    public DivTransformation(App app) {
        super(app, CODE, "usl_vspc_idl.div.hub");
    }

    @Override
    public void run() {
        // В RT-FW чтение источников (read) и запись наборов (publish) идут по meta. chg_div_h — наша промежуточная таблица в stg,
        // в meta её нет, поэтому читаем её напрямую.
        Dataset<Row> div_raw = readTable(STG_SCHEMA, CHG_DIVH);

        div_raw = div_raw.select(
                col(FULLDIVCODE).as(SID),
                // *_sid — натуральный ключ из источника; addSk отдаёт его в ССК и заменяет на суррогатный *_sk подчищая *_sid после себя.
                col(FULLDIVCODE).as(DIV_SID),
                col(FULLDIVCODE).as(DIV_CODE),
                col(SHORTDIVCODE).as(DIV_SHORT_DIV_CODE),
                col(CLOSEDATE).as(DIV_CLOSEDATE),
                col(OPENDATE).as(DIV_OPENDATE),
                concat(lit("0"), col(FULLDIVCODE)).as(DIV_SUBBRANCH_CODE),
                col(HEADDIVISIONCODE).as(DIV_PARENT_DIV_SID),

                when(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                        .otherwise(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM)).as(DIV_FULL_REG_NUM),

                when(col(FULLNAME).equalTo("ПАО Сбербанк"),
                        when(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null).otherwise(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM)))
                        .otherwise(
                                when(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                                        .otherwise(substring_index(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM), "/", -1))).cast(IntegerType).as(DIV_REG_ORD),

                when(col(ADDRESS_ADDRFACT_TIMEZONE).equalTo("USZ1"), "KALT")
                        .otherwise(col(ADDRESS_ADDRFACT_TIMEZONE)).as(DIV_TIME_ZONE_TIME_ZONE_SID),

                col(ADDRESS_LEGAL_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                col("address_addrfact_fiascode").as(ADDRESS_FIASCODE_DURTY),
                col("gold_okato").as(ADDR_OKATO2_SID),
                col("gold_oktmo").as(ADDR_OKTMO2_SID)

        ).filter(" coalesce(end_dt, '9999-12-31') = '9999-12-31' ").distinct();

        Dataset<Row> div = div_raw.select(
                lit(false).as(DELETED_FLAG),
                col(SID).as(SID),
                col(DIV_SID).as(DIV_SID),
                lit(SHORT_PRODUCT_CODE).as(DIV_DP_CODE),
                col(DIV_CODE).as(DIV_CODE),
                col(DIV_SHORT_DIV_CODE).as(DIV_SHORT_DIV_CODE),
                col(DIV_CLOSEDATE).as(DIV_CLOSEDATE),
                col(DIV_OPENDATE).as(DIV_OPENDATE),
                col(DIV_SUBBRANCH_CODE).as(DIV_SUBBRANCH_CODE),
                col(DIV_PARENT_DIV_SID).as(DIV_PARENT_DIV_SID),
                lit(SHORT_PRODUCT_CODE).as(DIV_PARENT_DIV_DP_CODE),

                when(col(ADDR_SRC_TXT).isNotNull(), AddrTransformation.getKey("|")).otherwise(null).as(DIV_LEGAL_ADDR_SID),
                when(col(ADDR_SRC_TXT).isNotNull(), lit(SHORT_PRODUCT_CODE)).otherwise(null).as(DIV_LEGAL_ADDR_DP_CODE),

                lit(false).as(DIRTY_FLAG),

                col(DIV_FULL_REG_NUM).as(DIV_FULL_REG_NUM),

                col(DIV_REG_ORD).as(DIV_REG_ORD),

                col(DIV_TIME_ZONE_TIME_ZONE_SID).as(DIV_TIME_ZONE_TIME_ZONE_SID),
                lit(SHORT_PRODUCT_CODE).as(DIV_TIME_ZONE_TIME_ZONE_DP_CODE)
        ).distinct();

        List<String> attributes = new ArrayList<>(
                Arrays.asList("parent@div", "legal@addr", "div_time_zone@time_zone_rf", "@div")
        );
        Dataset<Row> result = addSk(div, DIV, attributes);

        // Просто вызвать publish нельзя даже с пропатченным RT-FW: до его патча публикатор молча выбросит колонку с другим типом
        // и сразу выйдет на пустом инкременте. Поэтому сначала приводим и проверяем типы и корректно обрабатываем пустой INCR в своем собственном методе.
        // Там же вдобавок публикуем статистики в CTL: помечаем набор изменённым, и по этой пометке RT-FW их публикует.
        publishChecked(result);
    }
}
