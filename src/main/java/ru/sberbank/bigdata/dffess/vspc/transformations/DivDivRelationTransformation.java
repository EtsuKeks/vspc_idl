package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.date_sub;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIV;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVDIVRELATION;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVDIVRELATION_DS_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.SRC_PROVIDER_SFSDIVISION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class DivDivRelationTransformation extends BaseTransformation {

    public static final String CODE = DIVDIVRELATION;
    public static final String SETS = format("%s.%s", DP_CODE, DIVDIVRELATION_DS_CODE);

    public DivDivRelationTransformation(App app) {
        super(app, CODE, SETS);
    }

    @Override
    public void run() {
        // Чтение через штатный RT-FW Reader (провайдеры объявлены в lineage metadata.json).
        // Эквивалент прежнего SQL: successor LEFT JOIN sfsdivision x2 + TRIM(lower(...))='правопреемник'.
        Dataset<Row> successors = read(VspcConstant.SRC_PROVIDER_SFSDIVISION_SUCCESSORS).kap803Usl().snp().get();
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
                        lit(PLUS_INFINITY_DT).cast("date").as(END_DT_FIELD_NAME),
                        getKey("|").as(SID),

                        lit(DATA_PRODUCT).as(DIV_DIV_RELATION_DP_CODE),

                        col("source_sid").as(SOURCE_DIV_SID),
                        lit(DATA_PRODUCT).as(SOURCE_DIV_DP_CODE),
                        col("successor_sid").as(SUCCESSOR_DIV_SID),
                        lit(DATA_PRODUCT).as(SUCCESSOR_DIV_DP_CODE),

                        lit(false).as(DIRTY_FLAG),
                        lit(false).as(DELETED_FLAG)

                ).distinct();

        Map<String, String> columnEntities = new HashMap<>();
        // Только FK source/successor генерируются через ССК-сервер (как в 3.0: columnEntities =
        // {SOURCE_DIV_SK→DIV, SUCCESSOR_DIV_SK→DIV}). Link-сущность div_div_relation в ССК НЕ онбордится,
        // её ключ материализуется натурально ниже.
        columnEntities.put(SOURCE_DIV_SID, DIV);
        columnEntities.put(SUCCESSOR_DIV_SID, DIV);

        Dataset<Row> result = addSk(divsuccessor, DIVDIVRELATION, columnEntities);

        // div_div_relation_sk — натуральный ключ (getKey("|") = concat_ws("|", source_sid, successor_sid)),
        // материализуется напрямую, НЕ через ССК. Соответствие 3.0, где essa initSk удалял @DIVDIVRELATION.
        result = result.withColumn(DIV_DIV_RELATION_SK, col(SID));

        // ESSA-правило дат (DataLoaderLinkHistDiff): INIT — 1900-01-01; INCR — новый ключ 1900-01-01,
        // существующий ctl_validfrom - 1. Новизна ключа определяется ПОСЛЕ генерации СК по
        // div_div_relation_sk: в SNP есть только *_sk, а *_sid на этом этапе уже дропнуты.
        Column startDt;
        if (isIncrement()) {
            result = flagNewKeys(result, DIVDIVRELATION, DIV_DIV_RELATION_SK);
            startDt = when(col(NEW_KEY_FLAG), lit(VspcConstant.MIN_BUSINESS_DT))
                    .otherwise(date_sub(lit(VspcConstant.getCtlValidFromStr()).cast("date"), 1))
                    .cast("date");
            result = result.drop(NEW_KEY_FLAG);
        } else {
            startDt = lit(VspcConstant.MIN_BUSINESS_DT).cast("date");
        }
        result = result.withColumn(START_DT_FIELD_NAME, startDt);

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }

    /**
     * Link-сущность div_div_relation в ССК НЕ онбордится (как в 3.0, где essa initSk удалял @DIVDIVRELATION):
     * в ССК генерируются только FK source_div_sk/successor_div_sk, а div_div_relation_sk — натуральный ключ.
     */
    @Override
    protected boolean skInitOnLoad() {
        return false;
    }


    public static Column getKey(String sep) {
        return concat_ws(sep,
                coalesce(col("source_sid"), lit("NULL").cast("string")),
                coalesce(col("successor_sid"), lit("NULL").cast("string"))
        );
    }

    public static void main(String[] args) {
        runApp(args, DivDivRelationTransformation::new);
    }
}
