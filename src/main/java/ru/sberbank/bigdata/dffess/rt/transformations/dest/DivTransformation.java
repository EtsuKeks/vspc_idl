package ru.sberbank.bigdata.dffess.rt.transformations.dest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.dffess.rt.common.ColumnsName;
import ru.sberbank.bigdata.dffess.rt.transformations.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Recordset;

import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.apache.commons.collections.CollectionUtils.select;
import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.rt.common.Constants.CTL_VALIDFROM_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.reader.constants.ReaderConstants.CTL_LOADING;

public class DivTransformation extends BaseTransformation {

    public static final String TARGET_TABLE = "div_";
    public static final String SOURCE_TABLE_NAME_EBX = EBX_REP_M_SFS_0002_SFSDIVISION;

    public DivTransformation(App app, String... sets) {
        super(app, DivTransformation.class, sets);
    }

    @Override
    public void run() {
        if (app.ctx().getLoadMode() == App.LoadMode.INITIAL) {
            Dataset<Row> entityDataset = loadInitData(app.getSparkSession());
            publish(entityDataset, sets);
        } else {
            List<Dataset<Row>> datasets = loadSnpAnpDiffDataDiv(app.getSparkSession());

            publish(datasets.get(0), datasets.get(1), sets);
        }
        registerChangedSets(getSetMasks().toArray(Recordset[]::new));

        logger.logInfo(format("Завершилась трансформация %s с наборами данных %s из таблиц источника %s"
                , code, Arrays.asList(sets), SOURCE_TABLE_NAME_EBX));
    }

    private Dataset<Row> loadInitData(SparkSession spark) {
        logger.logInfo(format("Начинается загрузка трансформации %s с наборами данных %s из таблицы источника %s", code, Arrays.asList(sets), SOURCE_TABLE_NAME_EBX));
        Dataset<Row> resultDiv = spark.sql(format(
                "select * " +
                        " from %1$s.%2$s_mirror vo", STGTMP_SCHEMA, SOURCE_TABLE_NAME_EBX)
        );
        logger.logInfo(format("Набор данных из таблицы %s успешно загружен", SOURCE_TABLE_NAME_EBX));


        Dataset<Row> result = resultDiv

                .select(
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.SID),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SK),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_CODE),
                        col(ColumnsName.SHORTDIVCODE).as(ColumnsName.DIV_SHORT_DIV_CODE),
                        to_date(col(ColumnsName.CLOSEDATE)).as(ColumnsName.DIV_CLOSEDATE),
                        to_date(col(ColumnsName.OPENDATE)).as(ColumnsName.DIV_OPENDATE),
                        concat(lit("0"), col(ColumnsName.FULLDIVCODE)).as(ColumnsName.DIV_SUBBRANCH_CODE),
                        col(ColumnsName.HEADDIVISIONCODE).as(ColumnsName.DIV_PARENT_DIV_SK),

                        //col(ColumnsName.ADDRESS_LOCATIONADDRESS).as(ColumnsName.DIV_LEGAL_ADDR_SK),
                        //col(ColumnsName.ADDRESS_LEGAL_LOCATIONADDRESS).as(ColumnsName.DIV_LEGAL_ADDR_SK),

                        when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                                .otherwise(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM)).as(ColumnsName.DIV_FULL_REG_NUM),

                        when(col(ColumnsName.FULLNAME).equalTo("ПАО Сбербанк"),
                                when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null).otherwise(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM)))
                                .otherwise(
                                        when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                                                .otherwise(substring_index(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM), "/", -1))).cast(DataTypes.IntegerType).as(ColumnsName.DIV_REG_ORD),

                        col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).as(ColumnsName.DIV_TIME_ZONE_TIME_ZONE_SK),

                        lit(null).as(ColumnsName.ADDR_IDX_NUM),
                        lit(null).as(ColumnsName.ADDR_DISTRICT_NAME),
                        lit(null).as(ColumnsName.ADDR_LOCTY_NAME),
                        lit(null).as(ColumnsName.ADDR_REG_ADDR_NAME),
                        col(ColumnsName.ADDRESS_LEGAL_LOCATIONADDRESS).as(ColumnsName.ADDR_SRC_TXT),
                        lit(null).as(ColumnsName.ADDR_CITY_NAME),
                        lit(null).as(ColumnsName.ADDR_FIAS_CODE),
                        lit(null).as(ColumnsName.ADDR_HOUSE_NUM),
                        lit(null).as(ColumnsName.ADDR_STREET_NAME),
                        lit(null).as(ColumnsName.ADDR_CNTRY_SK),
                        lit(null).as(ColumnsName.ADDR_OKATO2_SK),
                        lit(null).as(ColumnsName.ADDR_RUS_FED_SUBJ_SK)
                )
                .cache();

        return result;
    }

    private List<Dataset<Row>> loadSnpAnpDiffDataDiv(SparkSession spark) {
        logger.logInfo(format("Начинается загрузка трансформации %s с наборами данных %s из таблицы источника %s", code, Arrays.asList(sets), SOURCE_TABLE_NAME_EBX));
        Dataset<Row> dfDeltaDiv = spark.sql(format(
                "SELECT * " +
                        "FROM %1$s.%2$s_delta vo ", STGTMP_SCHEMA, SOURCE_TABLE_NAME_EBX)
        );
        logger.logInfo(format("Набор данных из таблицы %s успешно загружен", SOURCE_TABLE_NAME_EBX));

        logger.logInfo(format("Начинается загрузка трансформации %s с наборами данных %s из таблицы источника %s", code, Arrays.asList(sets), SOURCE_TABLE_NAME_EBX));
        Dataset<Row> dfDeltaAddrObjFiasGar = spark.sql(format(
                "SELECT * FROM %1$s.%2$s_delta vo ", STGTMP_SCHEMA, SOURCE_TABLE_NAME_EBX)
        );
        logger.logInfo(format("Набор данных из таблицы %s успешно загружен", SOURCE_TABLE_NAME_EBX));

        Dataset<Row> resultDelta = dfDeltaDiv.select(
                col(ColumnsName.FULLDIVCODE).as(ColumnsName.SID),
                col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SK),
                col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_CODE),
                col(ColumnsName.SHORTDIVCODE).as(ColumnsName.DIV_SHORT_DIV_CODE),
                to_date(col(ColumnsName.CLOSEDATE)).as(ColumnsName.DIV_CLOSEDATE),
                to_date(col(ColumnsName.OPENDATE)).as(ColumnsName.DIV_OPENDATE),
                concat(lit("0"), col(ColumnsName.FULLDIVCODE)).as(ColumnsName.DIV_SUBBRANCH_CODE),
                col(ColumnsName.HEADDIVISIONCODE).as(ColumnsName.DIV_PARENT_DIV_SK),

                //col(ColumnsName.ADDRESS_LOCATIONADDRESS).as(ColumnsName.DIV_LEGAL_ADDR_SK),
                //col(ColumnsName.ADDRESS_LEGAL_LOCATIONADDRESS).as(ColumnsName.DIV_LEGAL_ADDR_SK),

                when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                        .otherwise(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM)).as(ColumnsName.DIV_FULL_REG_NUM),

                when(col(ColumnsName.FULLNAME).equalTo("ПАО Сбербанк"),
                        when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null).otherwise(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM)))
                        .otherwise(
                                when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                                        .otherwise(substring_index(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM), "/", -1))).cast(DataTypes.IntegerType).as(ColumnsName.DIV_REG_ORD),

                col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).as(ColumnsName.DIV_TIME_ZONE_TIME_ZONE_SK),

                lit(null).as(ColumnsName.ADDR_IDX_NUM),
                lit(null).as(ColumnsName.ADDR_DISTRICT_NAME),
                lit(null).as(ColumnsName.ADDR_LOCTY_NAME),
                lit(null).as(ColumnsName.ADDR_REG_ADDR_NAME),
                col(ColumnsName.ADDRESS_LEGAL_LOCATIONADDRESS).as(ColumnsName.ADDR_SRC_TXT),
                lit(null).as(ColumnsName.ADDR_CITY_NAME),
                lit(null).as(ColumnsName.ADDR_FIAS_CODE),
                lit(null).as(ColumnsName.ADDR_HOUSE_NUM),
                lit(null).as(ColumnsName.ADDR_STREET_NAME),
                lit(null).as(ColumnsName.ADDR_CNTRY_SK),
                lit(null).as(ColumnsName.ADDR_OKATO2_SK),
                lit(null).as(ColumnsName.ADDR_RUS_FED_SUBJ_SK)
        ).cache();
        return loadIncrementV2(TARGET_TABLE, resultDelta, new String[]{"addr_obj_object_code"}, null, null);
    }
}