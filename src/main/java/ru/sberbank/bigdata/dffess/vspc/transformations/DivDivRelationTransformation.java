package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.VspcLoadDate;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.date_sub;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.trim;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVDIVRELATION;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.SRC_PROVIDER_SFSDIVISION;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.SRC_PROVIDER_SFSDIVISION_SUCCESSORS;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.MINUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class DivDivRelationTransformation extends BaseTransformation {

    public static final String CODE = DIVDIVRELATION;

    public DivDivRelationTransformation(App app) {
        super(app, CODE, "usl_vspc_idl.div&div_relation.link");
    }

    @Override
    public void run() {
        Dataset<Row> successors = read(SRC_PROVIDER_SFSDIVISION_SUCCESSORS).kap803Usl().snp().get();
        Dataset<Row> divisions = read(SRC_PROVIDER_SFSDIVISION).kap803Usl().snp().get();

        Dataset<Row> sfsSource = divisions.select(col("id").as("sfs_source_id"), col("fulldivcode").as("source_sid"));
        Dataset<Row> sfsSuccessor = divisions.select(col("id").as("sfs_successor_id"), col("fulldivcode").as("successor_sid"));

        Dataset<Row> divsuccessor = successors
                .select(col("id").as("source_id"),
                        col("successors_id_successor"),
                        col("successors_successorstype"))
                .join(sfsSource, col("source_id").equalTo(col("sfs_source_id")), "left")
                .join(sfsSuccessor, col("successors_id_successor").equalTo(col("sfs_successor_id")), "left")
                .where(trim(lower(col("successors_successorstype"))).equalTo(lit("правопреемник")))
                .select("source_id", "source_sid", "successor_sid")
                .distinct();

        divsuccessor = divsuccessor
                .select(
                        // Технические колонки (ctl_loading, ctl_validfrom и др.) — не наши: RT-FW выбрасывает их из входа и ставит сам.
                        // Даты версии end_dt и start_dt — наши: публикатор требует их во входе.
                        lit(PLUS_INFINITY_DT).cast("date").as(END_DT_FIELD_NAME),
                        getKey("|").as(SID),

                        lit(DATA_PRODUCT).as(DIV_DIV_RELATION_DP_CODE),

                        // *_sid — натуральный ключ из источника; addSk отдаёт его в ССК и заменяет на суррогатный *_sk подчищая *_sid после себя.
                        col("source_sid").as(SOURCE_DIV_SID),
                        lit(DATA_PRODUCT).as(SOURCE_DIV_DP_CODE),
                        col("successor_sid").as(SUCCESSOR_DIV_SID),
                        lit(DATA_PRODUCT).as(SUCCESSOR_DIV_DP_CODE),

                        lit(false).as(DIRTY_FLAG),
                        lit(false).as(DELETED_FLAG)

                ).distinct();

        divsuccessor = divsuccessor.withColumn(DIV_DIV_RELATION_SID, col(SID));

        List<String> attributes = new ArrayList<>(Arrays.asList("source@div", "successor@div", "@div_div_relation"));
        Dataset<Row> result = addSk(divsuccessor, DIVDIVRELATION, attributes);

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

    public static Column getKey(String sep) {
        return concat_ws(sep,
                coalesce(col("source_sid"), lit("NULL").cast("string")),
                coalesce(col("successor_sid"), lit("NULL").cast("string"))
        );
    }
}
