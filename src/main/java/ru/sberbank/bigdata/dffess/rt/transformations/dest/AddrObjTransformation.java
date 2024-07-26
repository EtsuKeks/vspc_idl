package ru.sberbank.bigdata.dffess.rt.transformations.dest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Recordset;
import ru.sberbank.bigdata.dffess.rt.transformations.BaseTransformation;

import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.dffess.rt.common.Constants.CTL_VALIDFROM_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.reader.constants.ReaderConstants.CTL_ACTION;
import static ru.sberbank.bigdata.sl.rt.reader.constants.ReaderConstants.CTL_LOADING;

public class AddrObjTransformation extends BaseTransformation {

    public static final String TARGET_TABLE = "addr_obj";
    public static final String SOURCE_TABLE_NAME_Z_FT_MONEY = "z_ft_money";
    public static final String SOURCE_TABLE_NAME_AS_ADDR_OBJ = "as_addr_obj";

    public AddrObjTransformation(App app, String... sets) {
        super(app, AddrObjTransformation.class, sets);
    }

    @Override
    public void run() {
        if (app.ctx().getLoadMode() == App.LoadMode.INITIAL) {
            Dataset<Row> entityDataset = loadInitData(app.getSparkSession());
            publish(entityDataset, sets);
        } else {
            List<Dataset<Row>> datasets = loadSnpAnpDiffDataAddrObj(app.getSparkSession());

            publish(datasets.get(0), datasets.get(1), sets);
        }
        registerChangedSets(getSetMasks().toArray(Recordset[]::new));

        logger.logInfo(format("Завершилась трансформация %s с наборами данных %s из таблиц источника %s, %s"
                , code, Arrays.asList(sets), SOURCE_TABLE_NAME_Z_FT_MONEY, SOURCE_TABLE_NAME_AS_ADDR_OBJ));
    }

    private Dataset<Row> loadInitData(SparkSession spark) {
        logger.logInfo(format("Начинается загрузка трансформации %s с наборами данных %s из таблицы источника %s", code, Arrays.asList(sets), SOURCE_TABLE_NAME_Z_FT_MONEY));
        Dataset<Row> resultAddrObj = spark.sql(format(
                "select cast (id as string) as addr_obj_object_code, " +
                        " c_cur_short as addr_obj_objectguid_code, " +
                        " cast (NULL as string) as type_code " +
                        " from %1$s.%2$s_mirror vo", STGTMP_SCHEMA, SOURCE_TABLE_NAME_Z_FT_MONEY)
        );
        logger.logInfo(format("Набор данных из таблицы %s успешно загружен", SOURCE_TABLE_NAME_Z_FT_MONEY));

        logger.logInfo(format("Начинается загрузка трансформации %s с наборами данных %s из таблицы источника %s", code, Arrays.asList(sets), SOURCE_TABLE_NAME_AS_ADDR_OBJ));
        Dataset<Row> resultAddrObjFiasGar = spark.sql(format(
                "select concat(cast (objectid as string), '|fias_gar') as addr_obj_object_code, " +
                        " objectguid as addr_obj_objectguid_code, " +
                        " cast ('as_addr_obj' as string) as type_code " +
                        " from %1$s.%2$s_mirror vo", STGTMP_SCHEMA, SOURCE_TABLE_NAME_AS_ADDR_OBJ)
        );
        logger.logInfo(format("Набор данных из таблицы %s успешно загружен", SOURCE_TABLE_NAME_AS_ADDR_OBJ));

        Dataset<Row> result = resultAddrObj.union(resultAddrObjFiasGar)
                .withColumn("addr_obj_dp_code", lit("rtfw_ndl_eks"))
                .withColumn(CTL_LOADING, lit(app.ctx().getLoadingId()).cast("bigint"))
                .withColumn(CTL_ACTION, lit("I"))
                .withColumn(CTL_VALIDFROM_FIELD_NAME, lit(ctlValidfromValue()).cast("timestamp"))
                .withColumn("deleted_flag", lit(false))
                .cache();

        return result;
    }

    private List<Dataset<Row>> loadSnpAnpDiffDataAddrObj(SparkSession spark) {
        logger.logInfo(format("Начинается загрузка трансформации %s с наборами данных %s из таблицы источника %s", code, Arrays.asList(sets), SOURCE_TABLE_NAME_Z_FT_MONEY));
        Dataset<Row> dfDeltaAddrObj = spark.sql(format(
                "SELECT cast (id as string) as addr_obj_object_code, c_cur_short as addr_obj_objectguid_code, ctl_action, cast (NULL as string) as type_code  " +
                        "FROM %1$s.%2$s_delta vo ", STGTMP_SCHEMA, SOURCE_TABLE_NAME_Z_FT_MONEY)
        );
        logger.logInfo(format("Набор данных из таблицы %s успешно загружен", SOURCE_TABLE_NAME_Z_FT_MONEY));

        logger.logInfo(format("Начинается загрузка трансформации %s с наборами данных %s из таблицы источника %s", code, Arrays.asList(sets), SOURCE_TABLE_NAME_AS_ADDR_OBJ));
        Dataset<Row> dfDeltaAddrObjFiasGar = spark.sql(format(
                "SELECT concat(cast (objectid as string), '|fias_gar') as addr_obj_object_code, objectguid as addr_obj_objectguid_code," +
                        " ctl_action, cast ('as_addr_obj' as string) as type_code " +
                        "FROM %1$s.%2$s_delta vo ", STGTMP_SCHEMA, SOURCE_TABLE_NAME_AS_ADDR_OBJ)
        );
        logger.logInfo(format("Набор данных из таблицы %s успешно загружен", SOURCE_TABLE_NAME_AS_ADDR_OBJ));

        Dataset<Row> resultDelta = dfDeltaAddrObj.union(dfDeltaAddrObjFiasGar)
                .withColumn("addr_obj_dp_code", lit("rtfw_ndl_eks"))
                .withColumn(CTL_LOADING, lit(app.ctx().getLoadingId()).cast("bigint"))
                .withColumn(CTL_VALIDFROM_FIELD_NAME, lit(ctlValidfromValue()).cast("timestamp"))
                .withColumn("deleted_flag", lit(false))
                .cache();
        return loadIncrementV2(TARGET_TABLE, resultDelta, new String[]{"addr_obj_object_code"}, null, null);
    }
}