package ru.sberbank.bigdata.sl.rt;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.coordinator.Coordinator;
import ru.sberbank.bigdata.sl.rt.coordinator.CoordinatorInitResult;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.core.AppContextLoadingParams;
import ru.sberbank.bigdata.sl.rt.core.CoordinatorFactoryMock;
import ru.sberbank.bigdata.sl.rt.dfs.MiniDFSClusterExtension;
import scala.Option;

import java.io.IOException;
import java.util.*;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.*;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.collections.CollectionUtils.removeAll;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.VSPC_SNP_SRC_SCHEMA;
import static ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation.DIFF_SCHEMA;
import static ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation.SNP_SCHEMA;
import static ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation.STGTMP_SCHEMA;
import static ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation.STG_SCHEMA;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_LOADING;
import static ru.sberbank.bigdata.sl.rt.reader.constants.ReaderConstants.EMPTY_STRING;
import static ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters.*;
import static scala.collection.JavaConverters.asJavaCollection;

@ExtendWith({MiniDFSClusterExtension.class})
public class BaseTransformationTest {
    protected static Coordinator coordinator;

    private static final String USL_HDFS_PATH = "/data/usl/";
    private static final long TF_D1_LOADING_ID = 99999999L;

    private static String hdfsUri;

    protected static SparkSession spark;

    public static final String LOG_ENV = "";
    public static final StructType DUMMY_SCHEMA = new StructType().add("dummy", StringType);
    static final String CODE = "code_value";
    static final String SHORT_PRODUCT_CODE = DATA_PRODUCT;
    static final String PRODUCT_CODE = "usl_" + SHORT_PRODUCT_CODE;

    @BeforeAll
    public static void beforeAll() {
        hdfsUri = MiniDFSClusterExtension.getHdfsUri();

        spark = SparkSession.builder()
                .appName(BaseTransformationTest.class.getName())
                .config("spark.sql.warehouse.dir", hdfsUri + "/target")
                .config("spark.hadoop.fs.defaultFS", hdfsUri)
                .config("spark.local.dir", "./target/spark-temp")
                .config("spark.default.parallelism", "2")
                .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
                .config("spark.sql.storeAssignmentPolicy", "LEGACY")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.storeAssignmentPolicy", "LEGACY")
                .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
                .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        BaseTransformation.initStaticEnv(AppContext.Builder.create().setLogEnv(LOG_ENV).setShortProductCode(SHORT_PRODUCT_CODE).get());

        createDatabase(VSPC_SNP_SRC_SCHEMA);
        createDatabase(STGTMP_SCHEMA);
        createDatabase(STG_SCHEMA);
        createDatabase(SNP_SCHEMA);
        createDatabase(DIFF_SCHEMA);

        // создаём dummy-таблицы в target-схемах для корректной работы move в Publisher-е
        writeManagedTable(spark.createDataFrame(emptyList(), DUMMY_SCHEMA), SNP_SCHEMA, "dummy");
        writeManagedTable(spark.createDataFrame(emptyList(), DUMMY_SCHEMA), DIFF_SCHEMA, "dummy");
    }

    @BeforeEach
    void prepareTest() throws IOException {
        DistributedFileSystem fileSystem = MiniDFSClusterExtension.getFileSystem();
        fileSystem.delete(new Path(USL_HDFS_PATH), true);
        fileSystem.finalizeUpgrade();
    }

    private static void createDatabase(String dbName) {
        executeSql(format("create database %s", dbName));
    }

    private static void dropDatabase(String dbName) {
        executeSql(format("drop database if exists %s cascade", dbName));
    }

    private static void executeSql(String sql) {
        spark.sql(sql);
    }

    @AfterAll
    public static void afterAll() {
        dropDatabase(STGTMP_SCHEMA);
        dropDatabase(STG_SCHEMA);
        dropDatabase(SNP_SCHEMA);
        dropDatabase(DIFF_SCHEMA);
        dropDatabase(VSPC_SNP_SRC_SCHEMA);
        spark.catalog().clearCache();
    }

    /**
     * Создание  External таблицы spark
     *
     * @param ds       dataset
     * @param dbSchema имя БД схемы
     * @param table    имя таблицы
     */
    protected static void writeExternalTable(Dataset<?> ds, String dbSchema, String table, String... partitions) {
        String path = spark.sessionState().catalog().getDatabaseMetadata(dbSchema).locationUri().getPath() + "/" + table;
        if (dbSchema.endsWith("_diff")) {
            // добавляем суффикс к пути diff таблицы для корректной работы moveToTarget в Publisher-е
            // проблема с перемещением партиций в локальной файловой системе
            path = path + "_diff";
        }
        ds.write().partitionBy(partitions).mode(SaveMode.Overwrite).option("path", path).format("parquet").saveAsTable(format("%s.%s", dbSchema, table));
    }

    /**
     * Создание Managed таблицы spark
     *
     * @param ds       dataset
     * @param dbSchema имя БД схемы
     * @param table    имя таблицы
     */
    protected static void writeManagedTable(Dataset<?> ds, String dbSchema, String table) {
        ds.write().mode(SaveMode.Overwrite).format("parquet").saveAsTable(format("%s.%s", dbSchema, table));

    }

    protected static void writeManagedTableDiff(Dataset<?> ds, String dbSchema, String table) {
        ds.write().partitionBy(CTL_LOADING, CTL_DO).mode(SaveMode.Overwrite).format("parquet").saveAsTable(format("%s.%s", dbSchema, table));
    }

    public static Properties getValidProperties() {
        // Добавляем обязательные параметры
        Properties parameters = new Properties();
        parameters.setProperty(SL_LOGGER_KAFKATOPIC.getParamName(), "topic");
        parameters.setProperty(SL_LOGGER_KAFKABROKERLIST.getParamName(), "localhost:9092");
        parameters.setProperty(DATA_SETS.getParamName(), DATA_SETS.getDefaultValue());
        parameters.setProperty(OOZIE_WF_APPLICATION_PATH.getParamName(), "/oozie/wf/application/path");
        parameters.setProperty(USER_KEYTAB_PATH.getParamName(), "/user/keytab/path/");
        parameters.setProperty(YARN_QUEUE.getParamName(), "default");
        parameters.setProperty(USER_NAME.getParamName(), "user_name");
        parameters.setProperty(KERBEROS_REALM.getParamName(), "DEV.DF.SBRF.RU");
        parameters.setProperty(SL_CTL_AUTO_RANGE.getParamName(), SL_CTL_AUTO_RANGE.getDefaultValue());
        parameters.setProperty(SL_CTL_DELAY_MS.getParamName(), SL_CTL_DELAY_MS.getDefaultValue());
        parameters.setProperty(SL_CTL_MANUAL_RANGE.getParamName(), SL_CTL_MANUAL_RANGE.getDefaultValue());
        parameters.setProperty(SL_CTL_PARENT_ID.getParamName(), SL_CTL_PARENT_ID.getDefaultValue());
        parameters.setProperty(SL_CTL_RESPONSE_TIMEOUT_MS.getParamName(), SL_CTL_RESPONSE_TIMEOUT_MS.getDefaultValue());
        parameters.setProperty(SL_CTL_RETRIES.getParamName(), SL_CTL_RETRIES.getDefaultValue());
        parameters.setProperty(SL_LOGGER_APPID.getParamName(), "logger_app_id");
        parameters.setProperty(SL_LOGGER_KAFKASENDSYNC.getParamName(), SL_LOGGER_KAFKASENDSYNC.getDefaultValue());
        parameters.setProperty(SL_META_DELAY_MS.getParamName(), SL_META_DELAY_MS.getDefaultValue());
        parameters.setProperty(SL_META_HOST.getParamName(), "http://metahost");
        parameters.setProperty(SL_META_RESPONSE_TIMEOUT_MS.getParamName(), SL_META_RESPONSE_TIMEOUT_MS.getDefaultValue());
        parameters.setProperty(SL_META_RETRIES.getParamName(), SL_META_RETRIES.getDefaultValue());
        parameters.setProperty(SL_SK_SERVICE_HOST.getParamName(), SL_SK_SERVICE_HOST.getDefaultValue());
        parameters.setProperty(SL_CEAS.getParamName(), SL_CEAS.getDefaultValue());
        parameters.setProperty(SL_CESTAND.getParamName(), "CI03474088");
        parameters.setProperty(DEBUG_SK_OFF.getParamName(), "true"); // выключаем CСК
        parameters.setProperty(SL_APP_ENV.getParamName(), "dev");
        return parameters;
    }

    protected void beforeEach() {
        coordinator = Mockito.mock(Coordinator.class);
        when(coordinator.init(any())).thenReturn(new CoordinatorInitResult(false, emptySet()));
        CoordinatorFactoryMock.setCoordinatorMock(coordinator);
    }

    protected App getApp(App.LoadMode loadMode, Integer loadingId) {
        HashMap<String, String> appContextConstants = new HashMap<>();
        return getApp(loadMode, loadingId, appContextConstants);
    }

    protected App getApp(App.LoadMode loadMode, Integer loadingId, HashMap<String, String> appContextConstants) {
        when(coordinator.loadParams()).thenReturn(new BaseTransformationTestLoadingParams(loadMode, loadingId));
        when(coordinator.getLoadingIds(any(), any(), any())).thenReturn(singleton(TF_D1_LOADING_ID));
        App app = App.create(CODE).autoSparkSession(false).setAppContextConstants(appContextConstants).build();
        app.initSparkSession(spark);
        return app;
    }

    static protected Dataset<Row> readInputCsv(StructType schema, String day, String fileName) {
        return readCsv(schema, format("src/test/resources/InputData/%1$s/%2$s", day, fileName));
    }

    protected Dataset<Row> readOutputCsv(StructType schema, String day, String fileName) {
        return readCsv(schema, format("src/test/resources/OutputData/%1$s/%2$s", day, fileName));
    }

    private static Dataset<Row> readCsv(StructType schema, String path) {
        return spark.read()
                .option("header", "true")
                .option("charset", "UTF8")
                .option("nullValue", "NULL")
                .option("delimiter", ";")
                .schema(schema)
                .csv("file:///" + System.getProperty("user.dir") + "/" + path);
    }

    protected void assertDataFrameEquals(Dataset<Row> expected, Dataset<Row> actual, Set<String> ignoredColumns) {
        assertSchemaEquals(expected.schema(), actual.schema());
        Column[] expectedCols = Arrays.stream(expected.columns())
                .filter(column -> !ignoredColumns.contains(column))
                .sorted()
                .map(functions::col)
                .toArray(Column[]::new);
        List<Row> expectedRows = expected.select(expectedCols).sort(expectedCols).collectAsList();
        List<Row> actualRows = actual.select(expectedCols).sort(expectedCols).collectAsList();

        assertEquals(expectedRows.size(), actualRows.size(),
                format("Наборы данных отличаются по количеству строк\nНе найдены строки:\n%s\nЛишние строки:\n%s",
                        removeAll(expectedRows, actualRows).stream().map(Object::toString).collect(joining("\n")),
                        removeAll(actualRows, expectedRows).stream().map(Object::toString).collect(joining("\n"))
                )
        );

        // проверка корректности маппинга CSV - иначе все поля null
        boolean failedCsvMapping = true;
        for (int colNum = 0; colNum < expectedRows.get(0).length(); colNum++) {
            if (expectedRows.get(0).get(colNum) != null) {
                failedCsvMapping = false;
                break;
            }
        }
        if (failedCsvMapping) {
            fail("Ошибка в схеме данных для CSV - все поля null: " + expectedRows.get(0));
        }

        List<String> diff = new ArrayList<>();
        for (int rowNum = 0; rowNum < expectedRows.size(); rowNum++) {
            boolean newLine = true;
            Row expectedRow = expectedRows.get(rowNum);
            Row actualRow = actualRows.get(rowNum);
            for (int colNum = 0; colNum < expectedRow.length(); colNum++) {
                String colName = expectedCols[colNum].toString();
                Object expectedValue = expectedRow.get(colNum);
                Object actualValue = actualRow.get(colNum);
                if (!Objects.equals(expectedValue, actualValue)) {
                    if (newLine) {
                        diff.add(format("Expected: \t%s", expectedRow));
                        diff.add(format("Actual: \t%s", actualRow));
                        diff.add("Diff: \t\t'column name': expected | actual");
                        newLine = false;
                    }
                    diff.add(format("\t\t\t'%s': %s | %s", colName, expectedValue, actualValue));
                }
            }
        }
        if (!diff.isEmpty()) {
            fail("Наборы данных отличаются по значению полей\n" + join("\n", diff));
        }
        assertEquals(expectedRows, actualRows, "Наборы данных отличаются по значению полей");

    }

    protected void assertDataFrameEquals(Dataset<Row> expected, Dataset<Row> actual) {
        assertDataFrameEquals(expected, actual, emptySet());
    }

    protected void assertSchemaEquals(StructType expected, StructType actual) {
        List<StructField> cleanExpected = asJavaCollection(expected).stream().sorted(comparing(StructField::name)).collect(toList());
        List<StructField> cleanResult = asJavaCollection(actual).stream().sorted(comparing(StructField::name)).collect(toList());
        assertEquals(cleanExpected, cleanResult, "Наборы данных отличаются по составу полей");
    }

    /**
     * Класс содержит только те параметры, которые нужны для BaseTransformationTest
     */
    protected static class BaseTransformationTestLoadingParams extends AppContextLoadingParams {
        private final App.LoadMode loadMode;
        private final Integer loadingId;

        public BaseTransformationTestLoadingParams(App.LoadMode loadMode, Integer loadingId) {
            this.loadMode = loadMode;
            this.loadingId = loadingId;
        }

        @Override
        public String getCtlUrl() {
            return null;
        }

        @Override
        public Integer getLoadingId() {
            return loadingId;
        }

        @Override
        public String getInstance() {
            return EMPTY_STRING;
        }

        @Override
        public String getProductCode() {
            return PRODUCT_CODE;
        }

        @Override
        public Properties getParameters() {
            return getValidProperties();
        }

        @Override
        public App.LoadMode getMode() {
            return loadMode;
        }

        @Override
        public String getLogicalEnv() {
            return LOG_ENV;
        }

        @Override
        public String getShortProductCode() {
            return SHORT_PRODUCT_CODE;
        }

    }

    private static FileSystem getHdfs() {
        try {
            return FileSystem.get(spark.sparkContext().hadoopConfiguration());
        } catch (IOException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    static CatalogTable getCatalogTable(String databaseName, String tableName) {
        try {
            return spark.sessionState().catalog()
                    .getTableMetadata(
                            new TableIdentifier(tableName,
                                    Option.apply(databaseName))
                    );
        } catch (NoSuchTableException | NoSuchDatabaseException e) {
            String logMessage = join(": ", e.getClass().getCanonicalName(), join(".", databaseName, tableName), e.getMessage());
            System.out.println(logMessage);
            throw new RuntimeException(logMessage);
        }
    }

    static void ensureCtlLoadingNotEmpty(String databaseName, String tableName, String ctlLoading) {
        // добавляем пустую партицию ctl_do=EMPTY_DIR внутри указанной партиции /ctl_loading=<ctl_loading>
        // для корректной работы с перемещением каталогов в эмуляции файловой системы реализацией miniDFScluster
        // проблема с перемещением партиций в локальной файловой системе
        FileSystem fs = getHdfs();
        CatalogTable targetTableCatalog = getCatalogTable(databaseName, tableName);
        try {
            fs.mkdirs(new Path(targetTableCatalog.location().getPath() + "/ctl_loading=" + ctlLoading + "/ctl_do=EMPTY_DIR"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
