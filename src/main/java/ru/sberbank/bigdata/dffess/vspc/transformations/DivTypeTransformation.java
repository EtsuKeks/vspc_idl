package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.LoadDate;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_sub;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.cols;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.oneOpenPerKey;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIRTY_FLAG;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIVISIONTYPE00;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_SID;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_TYPE_TERR_DIV_TYPE_DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_TYPE_TERR_DIV_TYPE_SID;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.FULLDIVCODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVTYPE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_M_SFS_0002_SFSDIVISION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.MINUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.scalaSeq;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.writeWorkTable;

public class DivTypeTransformation extends BaseTransformation {

    public static final String CODE = DIVTYPE;

    // Временная таблица входа в stgtmp на INCR, для сохранения результата - чтобы не пришлось ходить в ССК дважды.
    private static final String SOURCE_TABLE = "tmp_" + DIVTYPE;

    public DivTypeTransformation(App app) {
        super(app, CODE, "usl_vspc_idl.div.sat=type");
    }

    @Override
    public void run() {
        // Дата загрузки этого шага.
        LoadDate.start();

        Dataset<Row> divtype = read(EBX_REP_M_SFS_0002_SFSDIVISION).kap803Usl().snp().get();

        divtype = divtype
                .select(
                        lit(false).as(DELETED_FLAG),

                        // *_sid — натуральный ключ из источника; addSk отдаёт его в ССК и заменяет на суррогатный *_sk подчищая *_sid после себя.
                        col(FULLDIVCODE).as(DIV_SID),
                        lit(SHORT_PRODUCT_CODE).as(DIV_DP_CODE),
                        col(DIVISIONTYPE00).as(DIV_TYPE_TERR_DIV_TYPE_SID),
                        lit(SHORT_PRODUCT_CODE).as(DIV_TYPE_TERR_DIV_TYPE_DP_CODE),
                        lit(false).as(DIRTY_FLAG)
                ).distinct();

        Dataset<Row> result = addSk(divtype, DIVTYPE, Arrays.asList("@div", "div_type@terr_div_type"));

        // Технические колонки (ctl_loading, ctl_validfrom и др.) — не наши: RT-FW выбрасывает их из входа и ставит сам.
        // Даты версии — наши: публикатор требует их во входе. Заполняем их так же, как загрузчик ESSA-версии витрины:
        // start_dt: INIT — 1900-01-01, INCR — день загрузки − 1; end_dt — 9999-12-31 в обоих режимах.
        result = result
                .withColumn(START_DT_FIELD_NAME, getApp().ctx().getLoadMode() == App.LoadMode.INCREMENT
                        ? date_sub(lit(LoadDate.sqlDate()), 1)
                        : lit(MINUS_INFINITY_DT).cast("date"))
                .withColumn(END_DT_FIELD_NAME, lit(PLUS_INFINITY_DT).cast("date"));

        // INCR: открытые версии ключей, которых нет в сегодняшнем срезе, закрываем днём загрузки − 1, как ESSA
        // (на пустом источнике — все). Добавляем во вход их закрытые копии: по ним RT-FW даёт ровно (R,U)+(U,U), 
        // как ESSA. Без этого добавления закрытия не произойдет.
        CompatTables.Table table = compatTable();
        SparkSession spark = app.getSparkSession();
        if (getApp().ctx().getLoadMode() == App.LoadMode.INCREMENT) {
            // Вход фиксируем во временной таблице stgtmp, как в ESSA-версии: по нему ищем пропавшие ключи
            // и его же публикуем — ключи ССК при этом считаются один раз. Пишем как все рабочие таблицы — способом
            // RT-FW.
            Dataset<Row> source = writeWorkTable(STGTMP_SCHEMA, SOURCE_TABLE, result);
            List<String> bk = table.entityKey();

            // Открытые версии — по одной на ключ, той же, что выберет калькулятор (oneOpenPerKey).
            Dataset<Row> gone = oneOpenPerKey(table, spark.table(table.snpTable())
                .filter(col(table.partitionColumn()).equalTo(lit(table.openPartition()))))
                .join(source.select(cols(bk)), scalaSeq(bk), "left_anti")
                .withColumn(END_DT_FIELD_NAME, date_sub(lit(LoadDate.sqlDate()), 1));

            // deleted_flag и dirty_flag в snp div_type нет — в копии, как во всём входе, false.
            List<String> snpCols = Arrays.asList(gone.columns());
            result = source.unionByName(gone.select(Arrays.stream(source.columns())
                    .map(c -> snpCols.contains(c) ? col(c) : lit(false).as(c)).toArray(Column[]::new)));
        }

        // Просто вызвать publish нельзя даже с пропатченным RT-FW: до его патча публикатор молча выбросит колонку с другим типом.
        // Поэтому сначала приводим и проверяем типы и ключи версии в своем собственном методе.
        // Там же вдобавок публикуем статистики в CTL: помечаем набор изменённым, и по этой пометке RT-FW их публикует.
        publishChecked(result);
    }
}
