package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.VspcLoadDate;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Recordset;

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
        super(app, CODE);
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

        divsuccessor = divsuccessor.withColumn(DIV_DIV_RELATION_SID, col(SID));

        List<String> attributes = new ArrayList<>(Arrays.asList("source@div", "successor@div", "@div_div_relation"));
        Dataset<Row> result = addSk(divsuccessor, DIVDIVRELATION, attributes);

        // vspc-3.0 parity: start_dt историчной таблицы — в 3.0 её штамповал локальный лоадер
        // DataLoaderLinkHistDiff (INIT: плоский to_date(lit(MIN_DATE))="1900-01-01", стр. 43+55-56;
        // INCR: date_sub(ctl_validfrom_to_start_dt,1), стр. 45-46). Штатный RT-FW Publisher даты не
        // создаёт — castToTargetStructure доливает отсутствующие колонки NULL (PublisherUtils.java:374-390),
        // из-за чего PK [start_dt,div_div_relation_sk] не сходится и D2/DROPBAD shadow-калькулятора
        // ненаблюдаемы. Якорь «день загрузки» = VspcLoadDate.sqlDate() (ОДНО значение на JVM) — тот же, что в
        // закрывающих формулах shadow (end := loadDate-2 ≡ another_ctl_validfrom-2): стык [.., loadDate-2][loadDate-1, ..] без разрыва.
        result = result.withColumn(START_DT_FIELD_NAME, getApp().ctx().getLoadMode() == App.LoadMode.INCREMENT
                                                        ? date_sub(lit(VspcLoadDate.sqlDate()), 1)
                                                        : lit(MINUS_INFINITY_DT).cast("date"));

        publish(result).recordsetMasks("usl_vspc_idl.div&div_relation.link").launch();
        registerChangedSets(Recordset.recordsets(getApp(), "usl_vspc_idl.div&div_relation.link"));
    }

    public static Column getKey(String sep) {
        return concat_ws(sep,
                coalesce(col("source_sid"), lit("NULL").cast("string")),
                coalesce(col("successor_sid"), lit("NULL").cast("string"))
        );
    }
}
