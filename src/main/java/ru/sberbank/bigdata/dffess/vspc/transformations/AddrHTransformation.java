package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.VspcLoadDate;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.ADDRH;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.CHG_DIVH;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.RDM_IDL_SPARK;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.MINUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class AddrHTransformation extends BaseTransformation {

    public static final String CODE = ADDRH;

    public AddrHTransformation(App app) {
        super(app, CODE, "usl_vspc_idl.addr.sat=h");
    }

    @Override
    public void run() {
        // В RT-FW чтение источников (read) и запись наборов (publish) идут по meta. chg_div_h — наша промежуточная таблица в stg,
        // в meta её нет, поэтому читаем её напрямую.
        Dataset<Row> addr_raw = readTable(STG_SCHEMA, CHG_DIVH);

        Dataset<Row> addr_fact = addr_raw.select(
                col(ADDRESS_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                col(ADDRESS_LOCATIONADDRESS).as(ADDRESS_STANDARD_ADDRESS_TXT),

                lit(null).as(ADDR_FIAS_CODE),
                lit(null).as(ADDRESS_FIASCODE_DURTY),

                // *_sid — натуральный ключ из источника; addSk отдаёт его в ССК и заменяет на суррогатный *_sk подчищая *_sid после себя.
                lit(null).as(ADDR_OKATO2_SID),
                lit(null).as(ADDR_OKTMO2_SID)

        ).where(col(ADDRESS_LOCATIONADDRESS).isNotNull());

        Dataset<Row> addr_legal = addr_raw.select(
                col(ADDRESS_LEGAL_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                col(ADDRESS_LEGAL_LOCATIONADDRESS).as(ADDRESS_STANDARD_ADDRESS_TXT),
                col("gold_fias_code").as(ADDR_FIAS_CODE),
                col("address_addrfact_fiascode").as(ADDRESS_FIASCODE_DURTY),
                col("gold_okato").as(ADDR_OKATO2_SID),
                col("gold_oktmo").as(ADDR_OKTMO2_SID)

        ).where(col(ADDRESS_LEGAL_LOCATIONADDRESS).isNotNull());

        Dataset<Row> addr = addr_fact.union(addr_legal).distinct();

        addr = addr.select(
                AddrTransformation.getKey("|").as(ADDR_SID),

                col(ADDRESS_STANDARD_ADDRESS_TXT).as(ADDRESS_STANDARD_ADDRESS_TXT),
                col(ADDR_FIAS_CODE).as(ADDR_FIAS_CODE),
                col(ADDR_OKATO2_SID).as(ADDR_OKATO2_SID),
                col(ADDR_OKTMO2_SID).as(ADDR_OKTMO2_SID)

        ).distinct();

        Dataset<Row> addrh = addr.select(
                // Технические колонки (ctl_loading, ctl_validfrom и др.) — не наши: RT-FW выбрасывает их из входа и ставит сам.
                // Даты версии end_dt и start_dt — наши: публикатор требует их во входе.
                lit(PLUS_INFINITY_DT).cast("date").as(END_DT_FIELD_NAME),
                col(ADDR_SID).as(ADDR_SID),
                lit(VspcConstant.DATA_PRODUCT).as(ADDR_DP_CODE),
                col(ADDRESS_STANDARD_ADDRESS_TXT).as(ADDRESS_STANDARD_ADDRESS_TXT),
                lit(false).as(DIRTY_FLAG),
                lit(false).as(DELETED_FLAG),

                when(col(ADDR_OKATO2_SID).isNotNull(), col(ADDR_OKATO2_SID)).as(ADDRH_OKATO2_SID),
                when(col(ADDR_OKATO2_SID).isNotNull(), lit(RDM_IDL_SPARK)).as(ADDRH_OKATO2_DP_CODE),

                when(col(ADDR_OKTMO2_SID).isNotNull(), col(ADDR_OKTMO2_SID)).as(ADDRH_OKTMO2_SID),
                when(col(ADDR_OKTMO2_SID).isNotNull(), lit(RDM_IDL_SPARK)).as(ADDRH_OKTMO2_DP_CODE),

                lit(">non_stndrt").as(ADDRESS_TYPE_CODE)
        ).distinct();

        Dataset<Row> result = addSk(addrh, ADDRH, Arrays.asList("@addr", "address@okato2", "address@oktmo2"));

        // start_dt — тоже наша колонка. Ради совместимости с ESSA-версией витрины заполняем его так же, как её загрузчик:
        // INIT — 1900-01-01, INCR — день загрузки − 1.
        result = result.withColumn(START_DT_FIELD_NAME, getApp().ctx().getLoadMode() == App.LoadMode.INCREMENT
                                                        ? date_sub(lit(VspcLoadDate.sqlDate()), 1)
                                                        : lit(MINUS_INFINITY_DT).cast("date"));

        // Просто вызвать publish нельзя даже с пропатченным RT-FW: до его патча публикатор молча выбросит колонку с другим типом
        // и сразу выйдет на пустом инкременте. Поэтому сначала приводим и проверяем типы и корректно обрабатываем пустой INCR в своем собственном методе.
        // Там же вдобавок публикуем статистики в CTL: помечаем набор изменённым, и по этой пометке RT-FW их публикует.
        publishChecked(result);
    }
}
