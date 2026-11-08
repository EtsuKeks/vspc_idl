package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.PublisherMode;
import ru.sberbank.bigdata.dffess.vspc.common.VspcLoadDate;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Recordset;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_sub;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.CHG_DIVH;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVH;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.RDM_IDL_SPARK;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_VALIDFROM;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class DivHTransformation extends BaseTransformation {

    public static final String CODE = DIVH;

    /** Тех-колонки схлопывания «второй версии того же дня» (R-23, обе моды); в снап не публикуются. */
    private static final String SRC_VALIDFROM_COL = "_src_validfrom";
    private static final String DEDUP_RN_COL = "_dedup_rn";

    public DivHTransformation(App app) {
        super(app, CODE, "usl_vspc_idl.div.sat=h");
    }

    @Override
    public void run() {
        Dataset<Row> div_raw = readTable(STG_SCHEMA, CHG_DIVH);

        Dataset<Row> div_h_raw = div_raw.select(
                col(FULLDIVCODE).as(DIV_SID),
                col(FULLNAME).as(DIV_H_NAME),
                col(FULLDIVCODE).as(DIV_CODE),
                col(PAY_SYS_PRTCPNT_SRC_COLUMN).as(DIV_PAY_SYS_PRTCPNT_SID),

                to_date(col(CLOSEDATE)).as(DIV_CLOSEDATE),
                to_date(col(OPENDATE)).as(DIV_OPENDATE),

                col(START_DT_FIELD_NAME).cast("date").as(START_DT_FIELD_NAME),
                col(END_DT_FIELD_NAME).cast("date").as(END_DT_FIELD_NAME),

                col("serving_isdisabledequip").as(DIV_DISABLED_ACCESS_EQUIPMENT),

                col(ADDRESS_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                lit(null).as(ADDRESS_FIASCODE_DURTY),
                lit(null).as(ADDR_OKATO2_SID),
                lit(null).as(ADDR_OKTMO2_SID)

        ).distinct();

        Dataset<Row> div_h = div_h_raw.select(
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

        // vspc-3.0 parity (щель дат): на INCR HistMix 3.0 ПЕРЕЗАПИСЫВАЛ транзитный start_dt
        // (chg_div_h несёт current_date()) собственной формулой ctl_validfrom-1
        // (3.0 vspc_idl_old …/impl/loader/DataLoaderHistMix.java:42 «startDt = date_sub(to_date(
        // col(CTL_VALIDFROM_TO_START_DT)), N1)», :51 безусловный withColumn(START_DT, startDt));
        // закрытая версия получала end = ctl_validfrom-2 — стык [.., VF-2][VF-1, ..] без щели.
        // Литеральный транзит давал [.., today-2][today, ..] — однодневная дыра на today-1.
        // Якорь VspcLoadDate.sqlDate() — ОДНО значение на JVM, то же, что в закрывающей формуле
        // shadow-калькулятора (date_sub(lit(loadDate),2)). INIT-транзит сохранён: там chg_div_h несёт
        // реальную цепочку версий из ebx-diff (ctl_validfrom := start_dt восстанавливается несением
        // в INIT-ветке ниже).
        //
        // R-23 (temp/audit_v2/11_pk_dups.md §0, §2.2, §2.3, §5; на обе моды по Z1 §4.2-Б2-2): на INCR
        // после перезаписи все версии батча получают один и тот же start_dt, поэтому две версии одного
        // fulldivcode (два ctl_validfrom из chg_div_h) дали бы две строки с одним PK (div_sk, start_dt)
        // → LEFT join по PK в GeneralMergeCalculator размножил бы строки снапа и добавил лишние ноги в
        // diff. В 3.0 такой дубль схлопывался сам (окно SnpDataLoader rn=1/rn=2 —
        // essa-common-lib …/slave/loading/SnpDataLoader.java:196 row_number() по rnSpec; фильтр
        // END_DT >= START_DT отбрасывал «второе обновление за день» — DataLoaderHistMix.java:88).
        // Здесь схлопываем явно: до перезаписи запоминаем исходный validfrom и оставляем по одной строке
        // на (div_sid, start_dt) — выживает версия с более поздним исходным validfrom; при равенстве
        // validfrom выбор произволен, как и в 3.0 (там порядок окна при равном ctl_loading не определён).
        // Окно работает и на INIT — там перезаписи нет (INIT-транзит несёт реальную цепочку версий,
        // см. абзац выше), легитимные версии отличаются start_dt и живут в разных группах окна, а
        // группа с общим start_dt — это по определению дубль PK; фактически INIT-преселект уже
        // схлопывает строки одного дня сам (end := lead(start_dt)-1 даёт всем «не-последним» строкам дня
        // end < start, их снимает datediff-фильтр преселекта), так что на INIT окно — страховочный no-op.
        // Тех-колонки удаляются до addSk, схема публикуемого снапа не меняется.
        div_h = div_h.withColumn(SRC_VALIDFROM_COL, col(START_DT_FIELD_NAME));
        if (getApp().ctx().getLoadMode() == App.LoadMode.INCREMENT) {
            div_h = div_h.withColumn(START_DT_FIELD_NAME, date_sub(lit(VspcLoadDate.sqlDate()), 1));
        }
        div_h = div_h
                .withColumn(DEDUP_RN_COL, row_number().over(
                        Window.partitionBy(col(DIV_SID), col(START_DT_FIELD_NAME))
                                .orderBy(col(SRC_VALIDFROM_COL).desc_nulls_last())))
                .filter(col(DEDUP_RN_COL).equalTo(1))
                .drop(SRC_VALIDFROM_COL, DEDUP_RN_COL);

        // Контракт ctl_validfrom: на INIT несем start_dt версии (паритет 3.0 DataLoaderHistMix.java:109 —
        // withColumn(CTL_VALIDFROM, col(START_DT))). Транспорт уважает принесённое (shadow fill-if-absent),
        // на INCR не несём — NOW-штамп транспорта = паритет 3.0. См. temp/audit_v3.
        if (getApp().ctx().getLoadMode() == App.LoadMode.INITIAL) {
            div_h = div_h.withColumn(CTL_VALIDFROM, col(START_DT_FIELD_NAME).cast("timestamp"));
        }

        Dataset<Row> result = addSk(div_h, DIVH, Arrays.asList("@div", "@pay_sys_prtcpnt", "fact@addr"));

        publish(result)
                .calculationMode(PublisherMode.sparkRebalance())
                .recordsetMasks("usl_vspc_idl.div.sat=h")
                .launch();
        registerChangedSets(Recordset.recordsets(getApp(), "usl_vspc_idl.div.sat=h"));
    }
}
