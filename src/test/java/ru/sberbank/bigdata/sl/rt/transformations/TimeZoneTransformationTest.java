package ru.sberbank.bigdata.sl.rt.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.dffess.vspc.transformations.TimeZoneTransformation;
import ru.sberbank.bigdata.sl.rt.BaseTransformationTest;

import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIRTY_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.App.LoadMode.INCREMENT;
import static ru.sberbank.bigdata.sl.rt.core.App.LoadMode.INITIAL;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.reader.constants.ReaderConstants.*;
import static ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation.DIFF_SCHEMA;
import static ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation.SNP_SCHEMA;
import static ru.sberbank.bigdata.sl.rt.SourceTable.RFCZONETIME;
import static ru.sberbank.bigdata.sl.rt.transformations.TimeZoneTransformationTest.TimeZoneTransformationTestData.*;

class TimeZoneTransformationTest extends BaseTransformationTest {

    static class TimeZoneTransformationTestData {
        public static final Integer INIT_LOADING_ID = 0;
        public static final Integer INCREMENT_LOADING_ID = 13230002;
        public static final Set<String> IGNORED_COLUMNS = new HashSet<>(asList("time_zone_rf_sk", DIRTY_FLAG, CTL_LOADING, CTL_VALIDFROM));

        public static final StructType SNP_STRUCT_TYPE = new StructType()
                .add(CTL_LOADING, LongType)
                .add(CTL_VALIDFROM, TimestampType)
                .add(DELETED_FLAG, BooleanType)
                .add(DIRTY_FLAG, BooleanType)
                .add("sid", StringType)
                .add("time_zone_abbr_code", StringType)
                .add("time_zone_code", StringType)
                .add("time_zone_msk_shift_val", StringType)
                .add("time_zone_wrld_shift_val", StringType)
                .add("time_zone_rf_dp_code", StringType)
                .add("time_zone_rf_sk", LongType)
                .add("time_zone_name", StringType);


        public static final StructType DIFF_STRUCT_TYPE = SNP_STRUCT_TYPE
                .add(CTL_DO, StringType)
                .add(CTL_ACTION, StringType);
    }

    @BeforeEach
    protected void beforeEach() {
        super.beforeEach();

        // создаём пустые таблицы diff и snapshot
        writeExternalTable(spark.createDataFrame(emptyList(), DIFF_STRUCT_TYPE), DIFF_SCHEMA, TimeZoneTransformation.PU_CODE);
        writeExternalTable(spark.createDataFrame(emptyList(), SNP_STRUCT_TYPE), SNP_SCHEMA, TimeZoneTransformation.PU_CODE);
    }

    @Test
    void runInit() {
        // готовим таблицу источник
        RFCZONETIME.writeD0();

        // запускаем трансформацию
        TimeZoneTransformation transformation = new TimeZoneTransformation(getApp(INITIAL, INIT_LOADING_ID));
        transformation.run();

        Dataset<Row> actual = spark.table(SNP_SCHEMA + "." + TimeZoneTransformation.PU_CODE);
        Dataset<Row> expected = readOutputCsv(SNP_STRUCT_TYPE, "d0", format("%s-%s.csv", SNP_SCHEMA, TimeZoneTransformation.PU_CODE));
        assertDataFrameEquals(expected, actual, IGNORED_COLUMNS);
    }

    @Test
    @Disabled("Вызывается вручную")
    void runIncrement() {
        // запускаем инит d0
        runInit();

        // запускаем инкремент d1

        // готовим таблицы источники
        // snp
        RFCZONETIME.writeD1();

        // запускаем трансформацию
        TimeZoneTransformation transformation = new TimeZoneTransformation(getApp(INCREMENT, INCREMENT_LOADING_ID));
        transformation.run();

        // сравниваем результаты
        // snp
        Dataset<Row> snpExpected = readOutputCsv(SNP_STRUCT_TYPE, "d1", format("%s-%s.csv", SNP_SCHEMA, TimeZoneTransformation.PU_CODE));
        Dataset<Row> snpActual = spark.table(SNP_SCHEMA + "." + TimeZoneTransformation.PU_CODE);

        assertDataFrameEquals(snpExpected, snpActual, IGNORED_COLUMNS);

        // diff
        Dataset<Row> diffExpected = readOutputCsv(DIFF_STRUCT_TYPE, "d1", format("%s-%s.csv", DIFF_SCHEMA, TimeZoneTransformation.PU_CODE));
        Dataset<Row> diffActual = spark.table(DIFF_SCHEMA + "." + TimeZoneTransformation.PU_CODE);

        assertDataFrameEquals(diffExpected, diffActual, IGNORED_COLUMNS);
    }
}