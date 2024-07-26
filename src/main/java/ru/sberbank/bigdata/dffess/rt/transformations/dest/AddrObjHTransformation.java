package ru.sberbank.bigdata.dffess.rt.transformations.dest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Recordset;
import ru.sberbank.bigdata.dffess.rt.transformations.BaseTransformation;

import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.rt.common.Constants.*;
import static ru.sberbank.bigdata.sl.rt.reader.constants.ReaderConstants.CTL_ACTION;
import static ru.sberbank.bigdata.sl.rt.reader.constants.ReaderConstants.CTL_LOADING;

public class AddrObjHTransformation extends BaseTransformation {

    public static final String TARGET_TABLE = "addr_obj_h";


    public AddrObjHTransformation(App app, String... sets) {
        super(app, AddrObjHTransformation.class, sets);
    }

    @Override
    public void run() {
        String sourceTable = "z_ft_money";
        if (app.ctx().getLoadMode() == App.LoadMode.INITIAL) {
            Dataset<Row> entityDataset = loadInitData(app.getSparkSession(), sourceTable);
            publish(entityDataset, sets);
        } else {
            List<Dataset<Row>> datasets = loadSnpAnpDiffData(app.getSparkSession(), sourceTable);
            app.startPublishing();
            publish(datasets.get(0), datasets.get(1), sets);
        }
        registerChangedSets(getSetMasks().toArray(Recordset[]::new));

        logger.logInfo(format("Завершилась трансформация %s с наборами данных %s из таблицы источника %s", code, Arrays.asList(sets), sourceTable));
    }

    protected Dataset<Row> loadInitData(SparkSession spark, String sourceTableName) {
        logger.logInfo(format("Начинается загрузка трансформации %s с наборами данных %s из таблицы источника %s", code, Arrays.asList(sets), sourceTableName));
        Dataset<Row> result = spark.sql(format(
                        "select cast (id as string) as addr_obj_object_code, " +
                                " c_code_iso as addr_obj_version_code " +
                                "from %1$s.%2$s_mirror vo", STGTMP_SCHEMA, sourceTableName)
                )
                .withColumn("addr_obj_dp_code", lit("rtfw_ndl_eks"))
                .withColumn(CTL_LOADING, lit(app.ctx().getLoadingId()).cast("bigint"))
                .withColumn(CTL_ACTION, lit("I"))
                .withColumn(CTL_VALIDFROM_FIELD_NAME, lit(ctlValidfromValue()).cast("timestamp"))
                .withColumn("deleted_flag", lit(false))
                .withColumn(END_DT_FIELD_NAME, lit("9999-12-31").cast("date"))
                .withColumn(START_DT_FIELD_NAME, lit("1900-01-01").cast("date"))
                .withColumn(TYPE_CODE_FIELD_NAME, lit("null").cast(DataTypes.StringType))
                .withColumn("p1month", date_format(col(END_DT_FIELD_NAME), "yyyyMM").cast(DataTypes.StringType))
                .cache();

        logger.logInfo(format("Набор данных из таблицы %s успешно загружен", sourceTableName));
        return result;
    }

    private List<Dataset<Row>> loadSnpAnpDiffData(SparkSession spark, String sourceTable) {
        Dataset<Row> dfDelta = spark.sql(format(
                        "SELECT cast (id as string) as addr_obj_object_code, c_code_iso as addr_obj_version_code, ctl_validfrom, ctl_action " +
                                "FROM %1$s.%2$s_delta vo ", STGTMP_SCHEMA, sourceTable)
                )
                .withColumn("addr_obj_dp_code", lit("rtfw_ndl_eks"))
                .withColumn(CTL_LOADING, lit(app.ctx().getLoadingId()).cast("bigint"))
                .withColumn(END_DT_FIELD_NAME, lit("9999-12-31").cast("date"))
                .withColumn(START_DT_FIELD_NAME, col(CTL_VALIDFROM_FIELD_NAME).cast("date"))
                .withColumn("deleted_flag", lit(false))
                .withColumn(TYPE_CODE_FIELD_NAME, lit(null).cast(DataTypes.StringType))
                .withColumn("p1month", date_format(col(CTL_VALIDFROM_FIELD_NAME), "yyyyMM").cast(DataTypes.StringType))
                .cache();

        return loadIncrementTechBus(TARGET_TABLE, dfDelta, new String[]{"addr_obj_object_code"}, START_DT_FIELD_NAME, END_DT_FIELD_NAME, true);
    }
}