package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.PublisherMode;
import ru.sberbank.bigdata.dffess.vspc.common.VspcLoadDate;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Recordset;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_sub;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVTYPE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.SRC_PROVIDER_SFSDIVISION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.MINUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class DivTypeTransformation extends BaseTransformation {

    public static final String CODE = DIVTYPE;

    /** Тех-колонка дедупа дубля реплики (аналог R-23 в DivHTransformation, обе моды); в снап не публикуется. */
    private static final String DEDUP_RN_COL = "_dedup_rn";

    public DivTypeTransformation(App app) {
        super(app, CODE, "usl_vspc_idl.div.sat=type");
    }

    @Override
    public void run() {
        Dataset<Row> div_type = read(SRC_PROVIDER_SFSDIVISION).kap803Usl().snp().get();

        div_type = div_type
                .select(
                        lit(false).as(DELETED_FLAG),
                        lit(PLUS_INFINITY_DT).cast("date").as(END_DT_FIELD_NAME),
                        col(FULLDIVCODE).as(DIV_SID),
                        lit(DATA_PRODUCT).as(DIV_DP_CODE),
                        col(DIVISIONTYPE00).as(DIV_TYPE_TERR_DIV_TYPE_SID),
                        lit(DATA_PRODUCT).as(DIV_TYPE_TERR_DIV_TYPE_DP_CODE),
                        lit(false).as(DIRTY_FLAG)
                ).distinct();

        // Дедуп дубля PK по sid источника (реставрация дедупа 095fbf3, удалённого 0b7dd57; аналог R-23
        // в DivHTransformation): в одной загрузке все строки получают один и тот же start_dt (INCR:
        // loadDate-1; INIT: 1900-01-01), поэтому повтор одного fulldivcode с другим divisiontype00 даёт
        // две строки с одним PK (div_sk, start_dt). На INCR такой дубль LEFT join по PK в
        // GeneralMergeCalculator разносит по снапу — на DEV такие пары лежат в usl_vspc_idl_snp.div_type
        // с прогона 16.10 (div_sk …045/…2005/…13053, ctl_loading пар одинаков). На INIT Publisher пишет
        // вход как есть (runInit = clearTargetTable + moveToTargetTable, калькуляторы изменений не
        // вызываются), т.е. трансформер — единственная точка, где INIT-дубль можно погасить. СК биективен
        // в рамках entity (одинаковый сид ⟹ одинаковый sk), значит дубль div_sk = дубль div_sid:
        // схлопываем по DIV_SID до addSk, чтобы ССК-map не получал дубли. Тай-брейк
        // div_type_terr_div_type_sid asc — единственной колонке, различающейся у реальных пар;
        // детерминизм обязателен (095fbf3; в 3.0 порядок окна при равном ctl_loading тоже не был
        // определён). Разные периоды одного ключа приходят РАЗНЫМИ загрузками и батчевым окном не
        // склеиваются.
        div_type = div_type
                .withColumn(DEDUP_RN_COL, row_number().over(
                        Window.partitionBy(col(DIV_SID))
                                .orderBy(col(DIV_TYPE_TERR_DIV_TYPE_SID).asc())))
                .filter(col(DEDUP_RN_COL).equalTo(1))
                .drop(DEDUP_RN_COL);

        Dataset<Row> result = addSk(div_type, DIVTYPE, Arrays.asList("@div", "div_type@terr_div_type"));

        // vspc-3.0 parity: start_dt историчной таблицы — в 3.0 её штамповал framework-лоадер
        // DataLoaderHistSnp (essa-common-lib, …/essa/common/slave/loading/DataLoaderHistSnp.java:
        // 44 — плоский to_date(lit(MIN_DATE))="1900-01-01" для INIT, 47 — date_sub(
        // ctl_validfrom_to_start_dt,1) для INCR, 56-57 — withColumn(START_DT, startDt)).
        // Штатный RT-FW Publisher даты не создаёт — castToTargetStructure доливает отсутствующие
        // колонки NULL (vendor/rt-fw-publisher-2.16.1-src/…/PublisherUtils.java:374-387, доливка на
        // :384), из-за чего PK [div_sk,start_dt] не сходится и D2/VANISH/DROPBAD shadow-калькулятора
        // ненаблюдаемы. Якорь «день загрузки» = VspcLoadDate.sqlDate() (ОДНО значение на JVM) — тот
        // же, что в закрывающих формулах shadow (end := loadDate-2 / VANISH end := loadDate-1):
        // стык [.., loadDate-2][loadDate-1, ..] без разрыва, как в 3.0.
        result = result.withColumn(START_DT_FIELD_NAME,
                getApp().ctx().getLoadMode() == App.LoadMode.INCREMENT
                        ? date_sub(lit(VspcLoadDate.sqlDate()), 1)
                        : lit(MINUS_INFINITY_DT).cast("date"));

        // R-14 (audit_v2/17_r14_empty_incr.md): пустой снапшот источника в INCR.
        // 3.0: DataLoaderHistSnp#fillTargetDatasets брал кандидатов на закрытие НЕ из инкремента, а из
        // ЦЕЛЕВОГО снапа (modifiableData = все открытые версии, end_dt=MAX_DATE), поэтому пустой
        // diffFromSrc закрывал ВСЮ открытую историю div_type (end_dt = today-1) и писал U/U-ноги в diff.
        // 4.0: стоковый PublisherImpl (rt-fw-publisher 2.16.1, :230-235) делает ранний выход
        //   if (loadMode == INCREMENT && builder.incSnp.rdd().isEmpty()) { complete(...); return; }
        // ⇒ калькулятор изменений не вызывается, VANISH-блок (единственный, кто закрыл бы открытые
        // версии при пустом входе) недостижим ⇒ тихий no-op: история div_type висит открытой вечно.
        // Легального обхода раннего выхода нет: publish(ds) — единственный путь с hist-семантикой,
        // publish(ds, recordsets...) и publish(snp, diff, ...) — simple-режим (clearTargetTable +
        // moveToTargetTable, полная замена таблицы), не «закрытие версий». Подсунуть непустой датасет
        // тоже нельзя: закрывающие строки совпадают с открытыми по бизнес-хэшу (EQUALS-retention
        // исключает start_dt/end_dt) и вдобавок глушат VANISH (incrMinDtPreRetention непуст).
        // Поэтому вместо тихого no-op — громкий отказ: оператор разбирает реплику источника и решает,
        // закрывать ли историю вручную (воспроизведение разрушительной семантики 3.0 — решение владельца).
        if (getApp().ctx().getLoadMode() == App.LoadMode.INCREMENT && result.isEmpty()) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                    "div_type INCR: снапшот источника пуст — в 3.0 это закрыло бы всю открытую историю div_type "
                            + "(end_dt = сегодня-1) и добавило U/U-ноги в diff; загрузка остановлена, требуется разбор реплики"));
        }

        publish(result)
                .calculationMode(PublisherMode.sparkRebalance())
                .recordsetMasks("usl_vspc_idl.div.sat=type")
                .launch();
        registerChangedSets(Recordset.recordsets(getApp(), "usl_vspc_idl.div.sat=type"));
    }
}
