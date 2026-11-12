package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.VspcLoadDate;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class DivHTransformation extends BaseTransformation {

    public static final String CODE = DIVH;

    public DivHTransformation(App app) {
        super(app, CODE, "usl_vspc_idl.div.sat=h");
    }

    @Override
    public void run() {
        // В RT-FW чтение источников (read) и запись наборов (publish) идут по meta. chg_div_h — наша промежуточная таблица в stg,
        // в meta её нет, поэтому читаем её напрямую.
        Dataset<Row> div_raw = readTable(STG_SCHEMA, CHG_DIVH);

        Dataset<Row> divh_raw = div_raw.select(
                // *_sid — натуральный ключ из источника; addSk отдаёт его в ССК и заменяет на суррогатный *_sk подчищая *_sid после себя.
                col(FULLDIVCODE).as(DIV_SID),
                col(FULLNAME).as(DIV_H_NAME),
                col(FULLDIVCODE).as(DIV_CODE),
                col(PAY_SYS_PRTCPNT_SRC_COLUMN).as(DIV_PAY_SYS_PRTCPNT_SID),

                to_date(col(CLOSEDATE)).as(DIV_CLOSEDATE),
                to_date(col(OPENDATE)).as(DIV_OPENDATE),

                // Технические колонки (ctl_loading, ctl_validfrom и др.) — не наши: RT-FW выбрасывает их из входа и ставит сам.
                // Даты версии end_dt и start_dt — наши: публикатор требует их во входе.
                col(START_DT_FIELD_NAME).as(START_DT_FIELD_NAME),
                col(END_DT_FIELD_NAME).as(END_DT_FIELD_NAME),

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
                lit(SHORT_PRODUCT_CODE).as(DIV_DP_CODE),

                when(col(DIV_PAY_SYS_PRTCPNT_SID).isNotNull(), col(DIV_PAY_SYS_PRTCPNT_SID)).otherwise(null).as(DIV_PAY_SYS_PRTCPNT_SID),
                when(col(DIV_PAY_SYS_PRTCPNT_SID).isNotNull(), lit("rdm_idl_spark")).otherwise(null).as(DIV_PAY_SYS_PRTCPNT_DP_CODE),


                when(col(ADDR_SRC_TXT).isNotNull(), AddrTransformation.getKey("|")).otherwise(null).as(DIV_FACT_ADDR_SID),
                when(col(ADDR_SRC_TXT).isNotNull(), lit(SHORT_PRODUCT_CODE)).otherwise(null).as(DIV_FACT_ADDR_DP_CODE),

                col(DIV_DISABLED_ACCESS_EQUIPMENT).as(DIV_DISABLED_ACCESS_EQUIPMENT),

                lit(false).as(DIRTY_FLAG),
                lit(false).as(DELETED_FLAG),
                col(START_DT_FIELD_NAME).as(START_DT_FIELD_NAME),
                col(END_DT_FIELD_NAME).as(END_DT_FIELD_NAME)
        ).distinct();

        // Даты версии заполняем так же, как загрузчик ESSA-версии витрины: INIT — история как пришла из chg_div_h,
        // INCR — start_dt = день загрузки − 1, end_dt = 9999-12-31.
        if (getApp().ctx().getLoadMode() == App.LoadMode.INCREMENT) {
            divh = divh.withColumn(START_DT_FIELD_NAME, date_sub(lit(VspcLoadDate.sqlDate()), 1))
                    .withColumn(END_DT_FIELD_NAME, lit(PLUS_INFINITY_DT).cast("date"));
        }

        Dataset<Row> result = addSk(divh, DIVH, Arrays.asList("@div", "@pay_sys_prtcpnt", "fact@addr"));

        // Просто вызвать publish нельзя даже с пропатченным RT-FW: до его патча публикатор молча выбросит колонку с другим типом
        // и сразу выйдет на пустом инкременте. Поэтому сначала приводим и проверяем типы и корректно обрабатываем пустой INCR в своем собственном методе.
        // Там же вдобавок публикуем статистики в CTL: помечаем набор изменённым, и по этой пометке RT-FW их публикует.
        publishChecked(result);
    }
}
