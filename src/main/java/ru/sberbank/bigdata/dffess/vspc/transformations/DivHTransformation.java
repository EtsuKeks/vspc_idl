package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
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
                col(PAY_SYS_PRTCPNT_SRC_COLUMN).as(DIV_PAY_SYS_PRTCPNT_SID),

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

        if (isIncrement()) {
            // ESSA-правило дат (DataLoaderHistMix, INCR): новый ключ — 1900-01-01, существующий — ctl_validfrom - 1;
            // end_dt остаётся passthrough из stg. Новизна ключа определяется ПОСЛЕ генерации СК по div_sk
            // (в SNP есть только *_sk, а *_sid на этом этапе уже дропнуты). Дедупликация защищает инкремент
            // от дублей реплики (тайбрейк end_dt — детерминированный выбор при равных opendate);
            // в INIT периоды бизнес-истории div_h сохраняются как есть (семантика release/3.0).
            // Отличие от ESSA HistMix: ESSA ставил входящим end_dt=9999 и сам закрывал прошлую версию
            // (another_ctl_validfrom−2); здесь то же делает rt-fw-publisher (PERIOD_WITH_REWRITE) — штатные
            // сценарии совпадают, а переоткрытие ранее закрытого ключа даёт start=ctl_validfrom−1 с разрывом
            // вместо ESSA-перекрытия 1900 — целевое поведение подтверждается прогоном INIT+INCR на DEV.
            result = flagNewKeys(result, DIVH, DIV_SK);
            result = result
                    .withColumn(START_DT_FIELD_NAME,
                            when(col(NEW_KEY_FLAG), lit(MIN_BUSINESS_DT))
                                    .otherwise(date_sub(lit(getCtlValidFromStr()).cast("date"), 1))
                                    .cast("date"))
                    .drop(NEW_KEY_FLAG)
                    .withColumn("_dedup_rn",
                            row_number().over(Window.partitionBy(col(DIV_SK))
                                    .orderBy(col(DIV_OPENDATE).desc(), col(END_DT_FIELD_NAME).desc())))
                    .filter(col("_dedup_rn").equalTo(1))
                    .drop("_dedup_rn");
        }

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }

    @Override
    protected boolean skInitOnLoad() {
        // ESSA-лоадер этой таблицы вызывал incrementSk (map) безусловно, включая INIT
        return false;
    }

    public static void main(String[] args) {
        runApp(args, DivHTransformation::new);
    }
}
