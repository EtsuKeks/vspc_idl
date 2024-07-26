package ru.sberbank.bigdata.dffess.rt.transformations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import ru.sberbank.bigdata.dffess.rt.Logger;
import ru.sberbank.bigdata.dffess.rt.common.SkService;
import ru.sberbank.bigdata.dffess.rt.model.SrcLog;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.core.Transformation;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.reader.SourceTable.SourceType;
import ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters;
import ru.sberbank.bigdata.sl.sk.client.SkClientException;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.net.URI;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.sl.rt.logger.audit.AuditLogMessageBuilder.AUDIT_005;
import static ru.sberbank.bigdata.sl.rt.reader.SourceTable.SourceType.KAP803_USL;
import static ru.sberbank.bigdata.sl.rt.reader.constants.ReaderConstants.*;
import static ru.sberbank.bigdata.sl.rt.util.Constants.UNDERLINING_SYMBOL;

public abstract class BaseTransformation extends Transformation {
    protected static final String SRC_LOG_TABLE = "src_log";
    public static final String OBJECT_NAME_FORMAT = "%1$s.%2$s";
    public static final String ID = "id";
    private static final Set<String> SLICE_DIFF_IGNORED_COLUMNS = new HashSet<>(Arrays.asList(CTL_VALIDFROM, CTL_LOADING));
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    public static final String TABLE_FORMAT = "%s.%s";
    protected static String SHORT_PRODUCT_CODE;
    public static final String INTERNAL_VSPC_IDL = "USL_VSPC_IDL";
    public static final String EBX_REP_M_SFS_0002_SFSDIVISION = "ebx_rep_m_sfs_0002_sfsdivision";

    public static final String EXTERNAL_FIAS_GAR_SCHEMA = "external_fias_gar";
    public static final String EXTERNAL_FIAS_GAR_SCHEMA_HIST = "external_fias_gar_hist";
    static String internalEksIbsFile;
    public static String STGTMP_SCHEMA;
    public static String STG_SCHEMA;
    private static FileSystem fileSystem = getFileSystem();
    public static String SNP_SCHEMA;
    public static String DIFF_SCHEMA;
    public static String HIST_POSTFIX = "_" + HIST_STRING;
    protected static int SRC_LOAD_LIMIT = 10_000;
    private static SkService skService = null;
    private final Encoder<SrcLog> srcLogEncoder = Encoders.bean(SrcLog.class);
    protected App app;
    protected String code;
    protected String[] sets;
    protected Logger logger;

    protected BaseTransformation(App app, String code, String... sets) {
        super(app, code, sets);
        this.code = code;
        this.app = app;
        this.sets = sets;

        logger = new Logger(app, this.getClass());
    }

    protected BaseTransformation(App app, Class clazz, String... sets) {
        this(app, clazz.getSimpleName(), sets);
    }

    /**
     * Метод, позволяющий привести структуру левой таблицы к структуре правой таблицы для успешной записи в таблицу с учётом партиций
     * Скопирован метод из rdm_idl_spark
     *
     * @param ds_for_convertation
     * @param ds_target_struct
     * @return структура набора данных, соответствующая требуемой
     */
    public static Dataset<Row> castLeftDsStructToRightDs(Dataset<Row> ds_for_convertation, Dataset<Row> ds_target_struct) {
        StructField[] cols_target_struct = ds_target_struct.schema().fields();
        List<String> cols_ds_for_convertation = Arrays.asList(ds_for_convertation.schema().fieldNames());
        Column[] expr = new Column[cols_target_struct.length];
        int i = 0;
        for (StructField f : cols_target_struct) {
            if (cols_ds_for_convertation.contains(f.name()))
                expr[i] = col(f.name()).cast(f.dataType());
            else
                expr[i] = lit(null).cast(f.dataType()).alias(f.name());
            i++;
        }
        List<Column> col_list = new LinkedList<Column>();
        for (int j = 0; j < expr.length; j++) col_list.add(expr[j]);
        Seq<Column> select_cols = JavaConverters.asScalaIteratorConverter(col_list.iterator()).asScala().toSeq();
        Dataset<Row> finalDF = ds_for_convertation.select(select_cols);
        return finalDF;
    }

    public static FileSystem getFileSystem() {
        if (fileSystem == null) {
            try {
                fileSystem = FileSystem.get(URI.create(""), new Configuration());
            } catch (Exception e) {
                throw new RuntimeException("Не удалось инициализировать FileSystem, причина: " + e.getCause().getMessage(), e);
            }
        }
        return fileSystem;
    }

    public static void initStaticEnv(AppContext context) {
        String logEnvPrefix = !context.getLogEnv().isEmpty() ? context.getLogEnv() + "_" : "";
        SHORT_PRODUCT_CODE = context.getShortProductCode();
        internalEksIbsFile = "internal_" + logEnvPrefix + "eks_ibs";
        STGTMP_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stgtmp";
        STG_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stg";
        SNP_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_snp";
        DIFF_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_diff";
    }

    protected Dataset<Row> loadPrevSlice(SparkSession spark, String tableName) {
        return spark.sql(format("select * from %1$s.%2$s_slice", STG_SCHEMA, tableName));
    }

    // Очистка таблицы с данными
    protected void clearTable(String targetSchema, String targetTable) {
        String objectName = format(TABLE_FORMAT, targetSchema, targetTable);
        try {
            if (targetSchema.equals(DIFF_SCHEMA) || targetSchema.equals(SNP_SCHEMA)) {
                // пишем аудит сообщение об удалении данных в таблицах таргет схем
                logger.log(AUDIT_005(objectName, objectName), true);
            }
            truncateTable(targetSchema, targetTable);
            logger.logInfo(format("Таблица %s очищена", objectName));
        } catch (IOException e) {
            logger.logError(format("Ошибка очистки таблицы %s: %s", objectName, e));
        }
    }

    // Сохранение таблицы с перезаписью
    protected void saveTable(Dataset<?> entityDataset, String targetSchema, String targetTable) {
        saveTable(entityDataset, targetSchema, targetTable, SaveMode.Overwrite);
    }

    protected void saveTable(Dataset<?> entityDataset, String targetSchema, String targetTable, SaveMode saveMode) {
        if (entityDataset != null) {
            SparkSession spark = app.getSparkSession();
            String tableName = targetSchema + "." + targetTable;
            String schemaPath = spark.sessionState().catalog().getDatabaseMetadata(targetSchema).locationUri().getPath();
            entityDataset
                    .write().option("path", format("%1$s/%2$s", schemaPath, targetTable))
                    .format("parquet")
                    .mode(saveMode)
                    .saveAsTable(tableName);
            spark.sql("REFRESH TABLE " + tableName);
            spark.sql("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS");
            logger.logInfo(format("Таблица %s сохранена", tableName));
        }
    }

    /**
     * Публикация данных. Метод используется для той части логики, которая ещё не реализована в RT-FW Publisher
     *
     * @param entityDataset набор данных для публикации
     * @param targetObject  полное имя таблицы схема.имя_таблицы
     */
    @Deprecated
    protected void publishData(Dataset<Row> entityDataset, String targetObject) {
        if (entityDataset != null) {
            Dataset<Row> emptyDs = app.getSparkSession().sql(format("select * from %1$s where 1=0", targetObject));
            Dataset<Row> resultDs = castLeftDsStructToRightDs(entityDataset, emptyDs).coalesce(1);
            DataFrameWriter<Row> writer = resultDs.write();
            writer.mode(SaveMode.Append).insertInto(targetObject);
            app.getSparkSession().sql("REFRESH TABLE " + targetObject);
            app.getSparkSession().sql("ANALYZE TABLE " + targetObject + " COMPUTE STATISTICS");
        }
        logger.logInfo(format("Набор данных %s опубликован", targetObject));
    }

    public String getCode() {
        return code;
    }

    protected String ctlValidfromValue() {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .format(java.time.LocalDateTime.now());
    }

    protected void truncateTable(String schema, String table) throws IOException {
        String tablePathString = getTablePath(schema, table);
        if (tablePathString != null) {
            Path tablePath = new Path(tablePathString);
            CatalogTableType tableType = getTableType(schema, table);
            if (tableType == CatalogTableType.EXTERNAL()) {
                if (fileSystem.exists(tablePath)) {
                    fileSystem.delete(tablePath, true);
                }
                if (!fileSystem.exists(tablePath)) {
                    FsPermission fsPermission = FsPermission.getDirDefault();
                    fileSystem.mkdirs(tablePath, fsPermission);
                }
                if (!getPartitionKeys(schema, table).isEmpty()) {
                    dropPartitions(schema, table);
                    app.getSparkSession().sql(format("MSCK REPAIR TABLE %s.%s", schema, table));
                }
            } else if (tableType != null) {
                app.getSparkSession().sql(format("TRUNCATE TABLE %s.%s", schema, table));
            }
        }
    }

    public Optional<CatalogTable> getTableInfo(String schema, String table) {
        try {
            return Optional.of(app.getSparkSession().sessionState().catalog().getTableMetadata(new TableIdentifier(table, Option.apply(schema))));
        } catch (NoSuchTableException | NoSuchDatabaseException e) {
            logger.logInfo(format("Таблица %s.%s не найдена", schema, table));
        }
        return Optional.empty();
    }

    public String getTablePath(String schema, String table) {
        Optional<CatalogTable> tableInfo = getTableInfo(schema, table);
        return tableInfo.map(t -> t.location().getPath()).orElse(null);
    }

    public CatalogTableType getTableType(String schema, String table) {
        Optional<CatalogTable> tableInfo = getTableInfo(schema, table);
        return tableInfo.map(CatalogTable::tableType).orElse(null);
    }

    public List<String> getPartitionKeys(String schema, String table) {
        Optional<CatalogTable> tableInfo = getTableInfo(schema, table);
        return tableInfo.map(t -> scala.collection.JavaConversions.seqAsJavaList(t.partitionColumnNames())).orElse(Collections.emptyList());
    }

    void dropPartitions(String schema, String table) {
        SparkSession spark = app.getSparkSession();
        List<Row> rows = spark.sql(format("show partitions %1$s.%2$s", schema, table))
                .collectAsList();
        if (!rows.isEmpty()) {
            String partitions = rows
                    .stream()
                    .map(r -> r.get(0).toString())
                    .collect(Collectors.joining(",", "partition(", "\")"))
                    .replace(",", "\"), partition(")
                    .replace("/", "\", ")
                    .replace("=", "=\"");

            spark.sql(format("alter table %1$s.%2$s drop %3$s", schema, table, partitions));
        } else {
            logger.logInfo(format("No discovered partitions for table %1$s.%2$s", schema, table));
        }
    }

    protected Column fixDateNotHighYear(Column dateBegin) {
        return when(substring(dateBegin, 6, 10).like("%02-29%")
                        .and(substring(dateBegin, 0, 4).cast("Int").mod(lit(4)).notEqual(lit("0"))),
                concat(substring(dateBegin, 0, 4), lit("-03-01"))
        ).otherwise(dateBegin);
    }

    protected Column stringFromDecimalWOZeros(Column clmn) {
        return regexp_replace(clmn.cast(DataTypes.StringType), "\\.0+$", "");
    }

    protected void selectMirror(String tableSchema, String tableName, SourceType sourceType) {
        String recordset = format(TABLE_FORMAT, tableSchema, tableName);

        Dataset<Row> table;
        switch (sourceType) {
            case KAP802_STORK:
                table = read().kap802Stork().snp().recordset(recordset).get();
                break;
            case KAP803_USL:
                table = read().kap803Usl().snp().recordset(recordset).get();
                break;
            case KAP802_ETL4HADOOP:
                table = read().kap802Etl4Hadoop().snp().recordset(recordset).get();
                break;
            case KAP802_XSTREAM:
                table = read().kap802Xstream().snp().recordset(recordset).get();
                break;
            default:
                throw new RuntimeException("Не удалось определить тип источника при чтении snp-таблицы");

        }

        // мы используем takeAsList вместо limit, тк spark для расчёта limit вычитывает все файлы,
        // в то время как takeAsList останавливается на набранном кол-ве строк
        List<Row> tableRows = table.takeAsList(SRC_LOAD_LIMIT);

        Dataset<Row> df = app.getSparkSession().createDataFrame(tableRows, tableRows.isEmpty() ? table.schema() : tableRows.get(0).schema());
        saveIntermediateSourceTableSnp(df, STGTMP_SCHEMA, tableName);
    }

    protected void saveIntermediateSourceTableSnp(Dataset<Row> df, String tableSchema, String tableName) {
        saveTable(df, tableSchema, tableName + "_mirror");
        afterSaveLogging(df, tableName, app.ctx().getLoadMode().name(), "mirror");
    }

    protected void selectDeltaDiff(String tableSchema, String tableName, SourceType sourceType) {
        String recorset = format(TABLE_FORMAT, tableSchema, tableName);

        if (tableSchema.endsWith("_diff")) {
            recorset = format(TABLE_FORMAT, tableSchema.substring(0, tableSchema.indexOf("_diff")), tableName);
        }
        Long maxCTLLoading = getMaxCTLLoadingEntryFinished(tableName);

        Dataset<Row> table;
        switch (sourceType) {
            case KAP802_STORK:
                table = read().kap802Stork().diff().recordset(recorset).get();
                break;
            case KAP802_ETL4HADOOP:
                table = read().kap802Etl4Hadoop().diff().recordset(recorset).get();
                break;
            case KAP802_XSTREAM:
                table = read().kap802Xstream().diff().recordset(recorset).get();
                break;
            default:
                throw new RuntimeException("Не удалось определить тип источника при чтении дифф-таблицы");
        }

        // мы используем takeAsList вместо limit, тк spark для расчёта limit вычитывает все файлы,
        // в то время как takeAsList останавливается на набранном кол-ве строк
        List<Row> tableRows = table.takeAsList(SRC_LOAD_LIMIT);

        Dataset<Row> df = app.getSparkSession().createDataFrame(tableRows, tableRows.isEmpty() ? table.schema() : tableRows.get(0).schema())
                .filter(col(CTL_LOADING).$greater(maxCTLLoading));
        Dataset<Row> dfDelta = transformDelta(df, singletonList(ID));
        saveIntermediateSourceTableDiff(dfDelta, STGTMP_SCHEMA, tableName);
    }

    private Dataset<Row> transformDelta(Dataset<Row> df, List<String> pkColumns) {
        List<Column> columns = pkColumns.stream().flatMap(name -> Stream.of(col(name), lit(","))).collect(toList());
        columns.add(col(CTL_LOADING));
        Dataset<Row> dfCut = df.withColumn("combined", concat(columns.toArray(new Column[0])));
        WindowSpec windowSpec = Window.partitionBy("combined");
        Dataset<Row> dfExp = dfCut.withColumn(
                "max_ctl_pa_loading",
                max(col(CTL_PA_LOADING)).over(windowSpec)
        ).withColumn("min_ctl_pa_loading", min(col(CTL_PA_LOADING)).over(windowSpec));

        Dataset<Row> dfDMaxCtlPaLoading = dfExp.filter(col(CTL_PA_LOADING).$eq$eq$eq(col("max_ctl_pa_loading")))
                .filter(col(CTL_ACTION).$eq$eq$eq("D"));

        Column joinCondition = expr("one.ctl_pa_loading = two.min_ctl_pa_loading and one.combined = two.combined");
        Dataset<Row> dfMinCtlLoad = dfExp.alias("one")
                .join(dfDMaxCtlPaLoading.alias("two"), joinCondition, "leftsemi")
                .withColumn(CTL_ACTION, lit("D"));

        Dataset<Row> dfMaxCtlLoad = dfExp.filter(col(CTL_PA_LOADING).$eq$eq$eq(col("max_ctl_pa_loading")))
                .filter(col(CTL_ACTION).notEqual("D"));

        Dataset<Row> dfDeltaFirstStage = dfMinCtlLoad.union(dfMaxCtlLoad)
                .drop("max_ctl_pa_loading", "min_ctl_pa_loading", "combined");

        Column[] ts = pkColumns.stream().map(functions::col).toArray(Column[]::new);
        WindowSpec windowSpecPK = Window.partitionBy(ts);
        return dfDeltaFirstStage.withColumn("max_ctl_loading", max(col(CTL_LOADING)).over(windowSpecPK))
                .where(col(CTL_LOADING).$eq$eq$eq(col("max_ctl_loading"))).drop("max_ctl_loading");
    }

    protected void selectHistDiff(String tableSchema, String tableName, SourceType sourceType) {
        String hst_0_ = "_hst_0_";
        String realTableName = tableName;
        if (tableName.contains(hst_0_)) {
            realTableName = tableName.substring(0, tableName.indexOf(hst_0_));
        }

        String recorset = format(TABLE_FORMAT, tableSchema, tableName);

        if (tableSchema.endsWith(HIST_POSTFIX)) {
            recorset = format(TABLE_FORMAT, tableSchema.substring(0, tableSchema.indexOf(HIST_POSTFIX)), realTableName);
        }

        Dataset<Row> histTable;
        Dataset<Row> snpTable;
        switch (sourceType) {
            case KAP802_STORK:
                histTable = read().kap802Stork().hist().recordset(recorset).get();
                snpTable = read().kap802Stork().snp().recordset(recorset).get();
                break;
            case KAP802_ETL4HADOOP:
                histTable = read().kap802Etl4Hadoop().hist().recordset(recorset).get();
                snpTable = read().kap802Etl4Hadoop().snp().recordset(recorset).get();
                break;
            case KAP802_XSTREAM:
                histTable = read().kap802Xstream().hist().recordset(recorset).get();
                snpTable = read().kap802Xstream().snp().recordset(recorset).get();
                break;
            default:
                throw new RuntimeException("Не удалось определить тип источника при чтении хист-таблицы");
        }

//Достаём delete-записи из hist-а
        Long maxCTLLoading = getMaxCTLLoadingEntryFinished(realTableName);

        List<Row> tableRowsHist = histTable.takeAsList(SRC_LOAD_LIMIT);
        Dataset<Row> dfHistTable = app.getSparkSession().createDataFrame(tableRowsHist, tableRowsHist.isEmpty() ? histTable.schema() : tableRowsHist.get(0).schema())
                .filter(col(CTL_LOADING).$greater(maxCTLLoading));
        Dataset<Row> deletesFromHist = transformHist(dfHistTable, ID);

        //Достаём update/insert-записи из snp
        List<Row> tableRowsSnp = snpTable.takeAsList(SRC_LOAD_LIMIT);
        Dataset<Row> updatesInsertsFromSnp = app.getSparkSession().createDataFrame(tableRowsSnp, tableRowsSnp.isEmpty() ? snpTable.schema() : tableRowsSnp.get(0).schema())
                .filter(col(CTL_LOADING).$greater(maxCTLLoading).and(col(CTL_ACTION).notEqual(lit("D"))));

        Dataset<Row> dfDelta = updatesInsertsFromSnp.union(castLeftDsStructToRightDs(deletesFromHist, updatesInsertsFromSnp));
        saveIntermediateSourceTableDiff(dfDelta, STGTMP_SCHEMA, realTableName);
    }

    private Dataset<Row> transformHist(Dataset<Row> dfHistTable, String PK) {
        Dataset<Row> dfHistTableDeleteRedo = dfHistTable.filter(col(CTL_ACTION).equalTo(lit("D")));
        Column condition = col(format("Redo.%s", PK)).equalTo(col(format("Undo.%s", PK)))
                .and(col(format("Redo.%s", CTL_LOADING)).equalTo(col(format("Undo.%s", CTL_LOADING))))
                .and(col(format("Undo.%s", CTL_ACTION)).notEqual(lit("D")));

        return dfHistTableDeleteRedo.alias("Redo")
                .join(dfHistTable.alias("Undo"), condition, "inner")
                .select("Undo.*")
                .withColumn(CTL_ACTION, lit("D"));
    }

    private void saveIntermediateSourceTableDiff(Dataset<Row> df, String tableSchema, String tableName) {
        saveTable(df, tableSchema, tableName + "_delta");
        afterSaveLogging(df, tableName, app.ctx().getLoadMode().name(), "delta");
    }

    private <R> R readLock(Supplier<R> action) {
        lock.readLock().lock();
        try {
            return action.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    private <R> R writeLock(Supplier<R> action) {
        lock.writeLock().lock();
        try {
            return action.get();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Long getMaxCTLLoadingEntryFinished(String tableName) {
        return readLock(() -> getMaxCTLLoadingEntry(tableName, "1"));
    }

    void afterSaveLogging(Dataset<Row> df, String tableName, String loadType, String stage) {
        SrcLog srcLog = new SrcLog();
        srcLog.setTable_name(tableName.toLowerCase());
        srcLog.setMax_src_ctl_loading(getMaxCTLLoading(df, tableName));
        srcLog.setDatetime(getTimestampNow());
        srcLog.setLoad_type(loadType);
        srcLog.setStatus("1");
        srcLog.setCtl_loading(valueOf(app.ctx().getLoadingId()));
        srcLog.setStage(stage);
        writeLog(srcLog);
    }
    //чтение лога с инфой о source таблицах в таблице логов src_log

    private Dataset<SrcLog> readLog(String tableName, String status) {
        return app.getSparkSession()
                .table(STG_SCHEMA + "." + SRC_LOG_TABLE)
                .filter(col("table_name").equalTo(tableName.toLowerCase())
                        .and(col("status").equalTo(lit(status))
                                .or(lit(status).isNull())
                        )
                )
                .map((MapFunction<Row, SrcLog>) row -> {
                    SrcLog result = new SrcLog();
                    result.setTable_name(row.getAs("table_name"));
                    result.setMax_src_ctl_loading(row.getAs("max_src_ctl_loading"));
                    result.setDatetime(row.getAs("datetime"));
                    result.setLoad_type(row.getAs("load_type"));
                    result.setStatus(row.getAs("status"));
                    result.setCtl_loading(row.getAs(CTL_LOADING));
                    result.setStage(row.getAs("stage"));
                    return result;
                }, srcLogEncoder);
    }

    private void writeLog(SrcLog data) {
        writeLock(() -> {
                    Dataset<SrcLog> dataset = app.getSparkSession().createDataset(singletonList(data), srcLogEncoder);
                    if (app.getSparkSession().catalog().tableExists(STG_SCHEMA + "." + SRC_LOG_TABLE)) {
                        saveTable(dataset, STG_SCHEMA, SRC_LOG_TABLE, SaveMode.Append);
                    } else {
                        saveTable(dataset, STG_SCHEMA, SRC_LOG_TABLE, SaveMode.Overwrite);
                    }
                    return null;
                }
        );
    }

    private String getTimestampNow() {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(java.time.LocalDateTime.now());
    }

    protected Long getMaxCTLLoading(Dataset<Row> df, String tableName) {
        return readLock(() -> {
            List<Row> result = df.agg(max(abs(col(CTL_LOADING)).cast("bigint"))).collectAsList();
            if (result.isEmpty() || result.get(0).isNullAt(0)) {
                logger.logInfo(format("В поле ctl_loading таблицы %s NULL, в src_log идёт 0", tableName));
                return 0L;
            } else {
                return result.get(0).getAs(0);
            }
        });
    }
    // чтение лога с инфой о source таблицах выявление загружаемого MaxCTLLoading в таблице логов src_log

    protected Long getMaxCTLLoadingEntry(String tableName, String status) {
        return readLock(() -> {
            List<Row> result = readLog(tableName, status)
                    .agg(max(abs(col("max_src_ctl_loading").cast("bigint")))).collectAsList();
            if (result.isEmpty() || result.get(0).isNullAt(0)) {
                logger.logInfo(format("Не нашёл таблицу %s в src_log", tableName));
                return 0L;
            } else {
                return result.get(0).getAs(0);
            }
        });
    }

    protected List<Dataset<Row>> loadIncrementV2(String tableName,
                                                 Dataset<Row> dfDelta,
                                                 String[] seqJoinColumns,
                                                 String startDt,
                                                 String endDt) {
        SparkSession spark = app.getSparkSession();

        dfDelta.cache();

        Dataset<Row> dfSNP = getOrCreateDfFromBackup(tableName) // делается копия snp таргета
                .withColumn(CTL_ACTION, lit(null).cast("string"));

        Column[] SNPColumnOrder = Arrays.stream(dfSNP.columns()).map(functions::col).toArray(Column[]::new);
        Column[] diffColumnOrder = Arrays.stream(spark.sql(format("select * from %1$s.%2$s limit 0", DIFF_SCHEMA, tableName))
                .columns()).map(functions::col).toArray(Column[]::new);

        Column[] joinSeqExtended = Arrays.stream(seqJoinColumns).map(functions::col).toArray(Column[]::new);
        if (startDt != null) {
            List<String> list = Arrays.stream(seqJoinColumns).collect(toList());
            list.add(startDt);
            joinSeqExtended = list.stream().map(functions::col).toArray(Column[]::new);
        }

        Dataset<Row> dfAntiSNPDelta = dfDelta.alias("delta") // те строки которых нет в таргете
                .join(dfSNP.alias("snp"),
                        joinBySeqKeepBoth(joinSeqExtended, "delta", "snp"),
                        "leftanti");

        WindowSpec joinColumnWindow = Window.partitionBy(joinSeqExtended);

        Dataset<Row> dfDeltaInsertsUnchecked = dfAntiSNPDelta // записи которые идут на insert
                .withColumn("count_D", count(when(col(CTL_ACTION).equalTo("D"), 1)).over(joinColumnWindow))
                .filter(col("count_D").equalTo(0)).drop("count_D")
                .withColumn(CTL_ACTION, lit("I")).distinct();

        Dataset<Row> dfDeltaInserts = startDt != null ? dfDeltaInsertsUnchecked
                : dfDeltaInsertsUnchecked.withColumn("deleted_flag", lit(false));

        Dataset<Row> dfDiffI = dfDeltaInserts.withColumn(CTL_DO, lit("R"))// записи insert в diff
                .union(dfDeltaInserts
                        .withColumn(CTL_ACTION, lit("D"))
                        .withColumn(CTL_DO, lit("U"))
                ).select(diffColumnOrder);

        Dataset<Row> dfSNPWith_I = dfSNP.unionByName(dfDeltaInserts.select(SNPColumnOrder)); // таргетный snp с insert

        Dataset<Row> dfInnerSNPDelta = dfDelta.alias("new")
                .join(dfSNP.alias("old"), joinBySeqKeepBoth(joinSeqExtended, "new", "old"), "inner")
                .cache(); // выделяем строки которые есть в таргет snp и пришедшей дельте

        Dataset<Row> dfInner_U = dfInnerSNPDelta
                .filter(col("new.ctl_action").equalTo("U")); // выделяем то что на update

        Dataset<Row> dfDiffUUnchecked = dfInner_U.select("old.*")// записи update в diff
                .withColumn(CTL_DO, lit("U"))
                .withColumn(CTL_ACTION, lit("U")).select(diffColumnOrder)
                .unionByName(
                        dfInner_U.select("new.*")
                                .withColumn(CTL_DO, lit("R"))
                                .withColumn(CTL_ACTION, lit("U")).select(diffColumnOrder));

        Dataset<Row> dfDiff_U = (startDt != null) ? dfDiffUUnchecked : dfDiffUUnchecked.withColumn("deleted_flag", lit(false)).select(diffColumnOrder);

        Dataset<Row> df_U = (startDt != null) ? dfInner_U.select("new.*") : dfInner_U.select("new.*").withColumn("deleted_flag", lit(false));

        Dataset<Row> dfTgtSNPAnti_U = dfSNPWith_I.alias("old").join(
                df_U.alias("new"),
                joinBySeqKeepBoth(joinSeqExtended, "old", "new"),
                "leftanti"); // выделяем то, что не будем изменять и положем в snp

        Dataset<Row> dfTgtSNP_U = dfTgtSNPAnti_U.unionByName(df_U.select(SNPColumnOrder)); // выделяем то, что не будем изменять и положем в snp

        Dataset<Row> dfInner_D = dfInnerSNPDelta // то что на удаление
                .filter(col("new.ctl_action").equalTo(lit("D")));

        // записи delete в diff
        Dataset<Row> dfDiff_D;
        if (startDt != null) {
            dfDiff_D = dfInner_D.select("new.*")
                    .withColumn(CTL_ACTION, lit("D"))
                    .withColumn(CTL_DO, lit("R")).select(diffColumnOrder)
                    .unionByName(
                            dfInner_D.select("old.*")
                                    .withColumn(CTL_ACTION, lit("I"))
                                    .withColumn(CTL_DO, lit("U"))).select(diffColumnOrder);
        } else {
            dfDiff_D = dfInner_D.select("new.*")
                    .withColumn(CTL_ACTION, lit("U"))
                    .withColumn(CTL_DO, lit("R"))
                    .withColumn("deleted_flag", lit(true)).select(diffColumnOrder)
                    .unionByName(
                            dfInner_D.select("old.*")
                                    .withColumn(CTL_ACTION, lit("U"))
                                    .withColumn(CTL_DO, lit("U"))
                                    .withColumn("deleted_flag", lit(false)).select(diffColumnOrder));
        }


        dfDiff_D.dropDuplicates(CTL_DO, Arrays.stream(joinSeqExtended).map(Column::toString).toArray(String[]::new));

        Dataset<Row> df_D = (startDt != null)
                ? spark.createDataFrame(emptyList(), dfInner_D.select("old.*").schema())
                : dfInner_D.select("old.*")
                .withColumn(CTL_ACTION, lit("D")).withColumn("deleted_flag", lit(true))
                .withColumn(CTL_LOADING, lit(app.ctx().getLoadingId()).cast("bigint"))
                .withColumn(CTL_VALIDFROM, lit(getTimestampNow()).cast("timestamp"));


        Column[] deleteJoinColumn = joinSeqExtended;
        if (endDt != null) {
            List<Column> columns = Arrays.asList(deleteJoinColumn);
            columns.add(col("end_dt"));
            deleteJoinColumn = columns.toArray(new Column[0]);
        }

        // убирает строки которые идут на удаление
        Dataset<Row> dfTgtSNP_UAnti_D = dfTgtSNP_U.alias("old")
                .join(dfInner_D.select("new.*"),
                        joinBySeqKeepBoth(deleteJoinColumn, "old", "new"),
                        "leftanti");

        // если есть добавляем строки с deleted_flag true
        Dataset<Row> dfSNPUpdated = dfTgtSNP_UAnti_D.unionByName(df_D.select(SNPColumnOrder));

        Dataset<Row> dfDiffIU = dfDiffI.unionByName(dfDiff_U);
        Dataset<Row> dfDiffIUD = dfDiffIU.unionByName(dfDiff_D);
        Dataset<Row> dfUnifiedDelta = dfDiffIUD.withColumn(CTL_LOADING, lit(app.ctx().getLoadingId()).cast("bigint"));

        return Arrays.asList(dfSNPUpdated, dfUnifiedDelta);
    }

    protected List<Dataset<Row>> loadIncrementTechBus(
            String tableName,
            Dataset<Row> dfDelta,
            String[] seqJoinColumns,
            String startDt,
            String endDt,
            Boolean propagateEarliestRecord) {

        SparkSession spark = app.getSparkSession();
        dfDelta.cache();

        Set<String> dfDeltaColumns = Arrays.stream(dfDelta.columns()).collect(Collectors.toSet());
        String format = "yyyy-MM-dd";
        Column dateColumn = col(endDt);
        if (dfDeltaColumns.contains("p1month")) {
            format = "yyyyMM";
            dateColumn = col("p1month");
        } else if (dfDeltaColumns.contains("p1day")) {
            format = "yyyyMMdd";
            dateColumn = col("p1day");
        }

        String minDateSubtracted = getMinOrStartDT(
                // format for partitions
                dfDelta.agg(date_format(min(date_sub(col(startDt), 1)), format)).collectAsList()
        ); // get min date -1

        // Выбор строк таргет снапшота, которые пойдут без изменений по условию: end_dt снапшота < минимальной start_dt-1

        String schema = getSchema();
        String baseSchema = schema.substring(0, schema.indexOf("_snp"));
        String recordset = format(TABLE_FORMAT, baseSchema, tableName);
        Dataset<Row> DF2 = read()
                .kap803Usl()
                .snp()
                .recordset(recordset)
                .get()
                .filter(dateColumn.$less(minDateSubtracted))
                .withColumn(CTL_ACTION, lit(null).cast("string"));

        // делается копия snp таргета
        Dataset<Row> dfSNP = read()
                .kap803Usl()
                .snp()
                .recordset(recordset)
                .get()
                .filter(dateColumn.$greater$eq(minDateSubtracted))
                .withColumn(CTL_ACTION, lit(null).cast("string"))
                .cache();

        Column[] snpColumnOrder = Arrays.stream(dfSNP.columns()).map(functions::col).toArray(Column[]::new);

        // Выбор строк снапшота, для которых нет соответствующих ключей в Дельте
        Column[] joinSeqColumns = Arrays.stream(seqJoinColumns).map(functions::col).toArray(Column[]::new);
        List<Column> joinListColumns = Arrays.stream(seqJoinColumns).map(functions::col).collect(toList());
        joinListColumns.add(col(startDt));
        Column[] joinSeqColumnsExtended = joinListColumns.toArray(new Column[0]);

        Dataset<Row> DF3 = dfSNP
                .alias("snp")
                .join(dfDelta.alias("delta"), joinBySeqKeepBoth(joinSeqColumns, "snp", "delta"), "leftanti");

        Dataset<Row> SNPInnerDeleted = dfSNP
                .alias("snp")
                .join(
                        dfDelta.alias("delta").filter(col(CTL_ACTION).$eq$eq$eq("D")),
                        joinBySeqKeepBoth(joinSeqColumnsExtended, "snp", "delta"),
                        "leftsemi"
                );

        // Соединение строк снапшота и дельты по inner
        Dataset<Row> SNPInnerDelta = dfSNP
                .alias("snp")
                .join(
                        SNPInnerDeleted.alias("snp_deleted"),
                        joinBySeqKeepBoth(joinSeqColumnsExtended, "snp", "snp_deleted"),
                        "leftanti"
                )
                .join(dfDelta.alias("delta"), joinBySeqKeepBoth(joinSeqColumns, "snp", "delta"), "inner");

        WindowSpec windowSpec =
                Window.partitionBy(Arrays.stream(seqJoinColumns).map(column -> col(format("delta.%s", column))).toArray(Column[]::new));

        // Добавление аттрибута минимального start_dt-1 дельты в пределах SK
        Dataset<Row> dfSNPMinDate = SNPInnerDelta.withColumn(
                "minDateForSK",
                min(col(format("delta.%s", startDt))).over(windowSpec).minus(expr("INTERVAL 1 DAYS"))
        );

        // Выделение строк Снапшота, которые пойдут без изменений, по условию: end_dt снапшота < минимального start_dt-1 дельты в пределах SK
        Dataset<Row> DF4 =
                dfSNPMinDate.filter(col(format("snp.%s", endDt)).$less(col("minDateForSK"))).select("snp.*");
        // Выделение строк для обработки, по условию (обратное прошлому): end_dt снапшота >= минимального start_dt-1 дельты в пределах SK
        Dataset<Row> SNPForProcessing =
                dfSNPMinDate.filter(col(format("snp.%s", endDt)).$greater$eq(col("minDateForSK")));

        // Выделение строк в которых start_dt снапшота < start_dt дельты текущего ключа. Добавление (нового столбца) даты закрытия для снапшота равного start_dt-1 Дельты
        Dataset<Row> DF5_1 = SNPForProcessing
                .filter(col(format("snp.%s", startDt)).$less(col(format("delta.%s", startDt))))
                .withColumn("new_end", date_sub(col(format("delta.%s", startDt)), 1));

        // Выделение атрибутов снапшота, замена end_dt на новую дату закрытия, замена всех ctl_action на U
        Dataset<Row> DF5 = DF5_1
                .select("snp.*", "new_end")
                .drop(endDt)
                .withColumnRenamed("new_end", endDt)
                .drop("new_end", "minDateForSK")
                .withColumn(CTL_ACTION, lit("U"))
                .withColumn(CTL_VALIDFROM, lit(getTimestampNow()).cast("timestamp"))
                .withColumn(CTL_LOADING, lit(ctx().getLoadingId()).cast("bigint"))
                .select(snpColumnOrder);

        Dataset<Row> DF5OldState =
                DF5_1.select("snp.*").drop("minDateForSK").withColumn(CTL_ACTION, lit("U"));

        // Выделение строк для сохранения в удаленные в DIFF
        Dataset<Row> DF5Deleted = SNPForProcessing
                .filter(col(format("snp.%s", startDt)).$greater$eq(col(format("delta.%s", startDt))))
                .select("snp.*")
                .drop("minDateForSK")
                .withColumn(CTL_ACTION, lit("D"));

        // Объединение Дельты с DF5
        Dataset<Row> DF6 = DF5
                .union(
                        SNPInnerDelta
                                .select("delta.*")
                                .filter(col(CTL_ACTION).notEqual("D"))
                                .withColumn(CTL_ACTION, lit("I"))
                                .select(snpColumnOrder)
                )
                .union(
                        dfDelta
                                .alias("delta")
                                .join(dfSNP.alias("snp"), joinBySeqKeepBoth(joinSeqColumns, "delta", "snp"), "leftanti")
                                .filter(col(CTL_ACTION).notEqual("D"))
                                .withColumn(CTL_ACTION, lit("I"))
                                .withColumn(
                                        startDt,
                                        propagateEarliestRecord ? lit("1900-01-01").cast("date") : col(startDt))
                                .select(snpColumnOrder)
                );

        // Схлопывание одинаковых полей
        Dataset<Row> DF7 = DF6.dropDuplicates();

        // Объединение всех отброшенных до этого частей снапшота (по причине отсутствия изменений), измененных частей, а так же новых значений из дельты
        Dataset<Row> satelliteSNP = DF2.union(DF3).union(DF4).union(DF7)
                .drop(CTL_ACTION).withColumn(dateColumn.toString(), date_format(col(endDt), format));

        Column[] diffColumnsOrder = Arrays.stream(spark.sql(format("select * from %1$s.%2$s limit 0", DIFF_SCHEMA, tableName))
                        .columns())
                .map(functions::col)
                .toArray(Column[]::new);

        // Выделение diff. Добавление Новых записей из Дельты (RI + UD). Добавление обновленных записей. Добавление удаленных
        Dataset<Row> satelliteDIFF =
                DF7
                        .withColumn(CTL_DO, lit("R"))
                        .select(diffColumnsOrder)
                        .union(
                                DF7
                                        .filter(col(CTL_ACTION).$eq$eq$eq("I"))
                                        .withColumn(CTL_ACTION, lit("D"))
                                        .withColumn(CTL_DO, lit("U"))
                                        .select(diffColumnsOrder)
                        )
                        .union(
                                DF5OldState
                                        .withColumn(CTL_DO, lit("U"))
                                        .select(diffColumnsOrder)
                        )
                        .union(
                                DF5Deleted
                                        .withColumn(CTL_DO, lit("R"))
                                        .select(diffColumnsOrder)
                        )
                        .union(
                                DF5Deleted
                                        .withColumn(CTL_ACTION, lit("I"))
                                        .withColumn(CTL_DO, lit("U"))
                                        .select(diffColumnsOrder)
                        )
                        .union(
                                SNPInnerDeleted
                                        .withColumn(CTL_DO, lit("R"))
                                        .withColumn(CTL_ACTION, lit("D"))
                                        .select(diffColumnsOrder)
                        )
                        .union(
                                SNPInnerDeleted
                                        .withColumn(CTL_ACTION, lit("I"))
                                        .withColumn(CTL_DO, lit("U"))
                                        .select(diffColumnsOrder)
                        )
                        .withColumn(CTL_LOADING, lit(ctx().getLoadingId()).cast("bigint"))
                        .withColumn(dateColumn.toString(), date_format(col(endDt), format));

        return Arrays.asList(satelliteSNP, satelliteDIFF);
    }

    private String getSchema() {
        String schema = SNP_SCHEMA;
        String logEnv = app.ctx().getLogEnv();
        if (!EMPTY_STRING.equals(logEnv)) {
            schema = SNP_SCHEMA.replaceFirst(UNDERLINING_SYMBOL + logEnv, EMPTY_STRING);
        }
        return schema;
    }

    private String getMinOrStartDT(List<Row> df) {
        if (df.isEmpty() || df.get(0).get(0) == null) {
            return "1900-01-01";
        } else {
            return df.get(0).get(0).toString();
        }
    }

    protected Dataset<Row> getOrCreateDfFromBackup(String tableName) {
        String schema = getSchema();
        selectMirror(schema.substring(0, schema.indexOf("_snp")), tableName, KAP803_USL);
        return app.getSparkSession().sql(format("select * from %1$s.%2$s_mirror", STGTMP_SCHEMA, tableName));
    }

    protected Column joinBySeqKeepBoth(Column[] columnSeq, String leftAlias, String rightAlias) {
        return expr(Arrays.stream(columnSeq)
                .map(colName -> format("%1$s.%3$s = %2$s.%3$s", leftAlias, rightAlias, colName))
                .collect(Collectors.joining(" and ")));
    }


    protected Dataset<Row> getSlicesDiff(
            Dataset<Row> dfNew,
            Dataset<Row> dfOld,
            List<String> joinColumnsList) {
        List<Dataset<Row>> pairWithMD5Hash = createPairWithMD5Hash(dfNew, dfOld);

        Dataset<Row> dfNewWithMd5 = pairWithMD5Hash.get(0);
        Dataset<Row> dfOldWithMd5 = pairWithMD5Hash.get(1);

        Seq<String> joinColumns = JavaConversions.asScalaIterator(joinColumnsList.iterator()).toSeq();
        Dataset<Row> dfInserts = getInserts(dfNewWithMd5, dfOldWithMd5, joinColumns);
        Dataset<Row> dfUpdates = getUpdates(dfNewWithMd5, dfOldWithMd5, joinColumns);
        Dataset<Row> dfDeletes = getDeletes(dfNewWithMd5, dfOldWithMd5, joinColumns);

        return dfInserts.unionByName(dfUpdates).unionByName(dfDeletes);
    }

    private Dataset<Row> getInserts(
            Dataset<Row> dfNewWithMd5,
            Dataset<Row> dfOldWithMd5,
            Seq<String> joinColumn) {
        return dfNewWithMd5.alias("new").join(
                dfOldWithMd5.alias("old"),
                joinColumn,
                "leftanti"
        ).withColumn(CTL_ACTION, lit("I"));
    }

    private Dataset<Row> getUpdates(
            Dataset<Row> dfNewWithMd5,
            Dataset<Row> dfOldWithMd5,
            Seq<String> joinColumn) {
        return dfNewWithMd5.alias("new").join(
                        dfOldWithMd5.alias("old"),
                        joinColumn,
                        "inner"
                ).filter(col("old.md5hash").notEqual(col("new.md5hash"))).select("new.*")
                .withColumn(CTL_ACTION, lit("U"));
    }

    private Dataset<Row> getDeletes(
            Dataset<Row> dfNewWithMd5,
            Dataset<Row> dfOldWithMd5,
            Seq<String> joinColumn) {
        return dfOldWithMd5.alias("old").join(
                dfNewWithMd5.alias("new"),
                joinColumn,
                "leftanti"
        ).withColumn(CTL_ACTION, lit("D"));
    }

    private List<Dataset<Row>> createPairWithMD5Hash(
            Dataset<Row> dfNew,
            Dataset<Row> dfOld) {
        Column[] columnsForConcat = Arrays.stream(dfNew.columns())
                .filter(col -> !SLICE_DIFF_IGNORED_COLUMNS.contains(col))
                .map(functions::col).toArray(Column[]::new);
        return Arrays.asList(
                dfNew.withColumn("md5hash", md5(concat_ws("|", columnsForConcat))),
                dfOld.withColumn("md5hash", md5(concat_ws("|", columnsForConcat)))
        );
    }

    protected Dataset<Row> addSk(Dataset<Row> mappedDfwc,
                                 String tableName,
                                 List<String> attributeCodes,
                                 Map<String, String> skColumnCodeEntityMap) {
        try {
            if (skService == null) {
                skService = new SkService();
            }
            Dataset<Row> fixedToNewSkDf = fixDfStructure(mappedDfwc);

            List<String> attributeCodesResolved = attributeCodes.isEmpty() ? getAttributeCodes(
                    Arrays.stream(fixedToNewSkDf.columns()).filter(cn -> cn.endsWith(SkService.SOURCE_SK_SUFFIX)).collect(toList()),
                    skColumnCodeEntityMap) : attributeCodes;

            List<String> columnsFromAttributes = skService.getColumnsFromAttributes(attributeCodesResolved);

            Dataset<Row> castedSids = castSidsToString(fixedToNewSkDf, columnsFromAttributes);

            saveTable(castedSids, STGTMP_SCHEMA, tableName + "_mapped");
            Dataset<Row> readDf = app.getSparkSession().sql(format("select * from %1$s.%2$s_mapped", STGTMP_SCHEMA, tableName)).cache();

            // Заполняем датасет сгенерированными суррогатными ключами

            Dataset<Row> responseDf = skService.runSkGen(
                    "false".equals(ctx().getProperty(ApplicationParameters.DEBUG_SK_OFF.getParamName())),
                    tableName.trim().toLowerCase(),
                    readDf,
                    attributeCodesResolved
            );

            return castSidsToBigint(responseDf, columnsFromAttributes);
        } catch (SkClientException e) {
            logger.logError("Ошибка интеграции с ССК: " + e.getMessage());
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                    "Произошла ошибка интеграции с ССК"));
        }

    }

    /**
     * Renaming all _sk columns to _sid columns for back compatability
     *
     * @param df
     * @return
     */
    private Dataset<Row> fixDfStructure(Dataset<Row> df) {
        Dataset<Row> resDataSet = df;
        List<String> skColListRename = Arrays.stream(df.columns()).filter(cn -> cn.endsWith(SkService.TARGET_SK_SUFFIX)).collect(toList());
        for (String name : skColListRename) {
            resDataSet = resDataSet.withColumnRenamed(name, format("%ssid", name.substring(0, name.length() - 2)));
        }
        return resDataSet;
    }

    /**
     * Принудительно кастуем все _sk поля в String, попутно отрезая .000
     *
     * @param df
     * @return
     */
    private Dataset<Row> castSidsToString(Dataset<Row> df, List<String> skColList) {
        Dataset<Row> resDataSet = df;
        for (String cn : skColList) {
            resDataSet = resDataSet
                    .withColumn(cn, regexp_replace(col(cn).cast(DataTypes.StringType), "\\.0+$", ""));
        }
        return resDataSet;
    }

    /**
     * Принудительно кастуем все _sk поля в Bigint
     *
     * @param df
     * @param skColList
     * @return
     */
    private Dataset<Row> castSidsToBigint(Dataset<Row> df, List<String> skColList) {
        Dataset<Row> resDataSet = df;
        for (String cn : skColList) {
            String cn_sk = cn.replace("_sid", "_sk");
            resDataSet = resDataSet.withColumn(cn_sk, col(cn_sk).cast(DataTypes.LongType));
        }
        return resDataSet;
    }

    /**
     * Сбор кодов аттрибутов на основании названий колонок и skColumnCodeEntityMap
     *
     * @param skColList
     * @param skColumnCodeEntityMap
     * @return
     */
    private List<String> getAttributeCodes(List<String> skColList, Map<String, String> skColumnCodeEntityMap) {
        return skColList.stream()
                .map(cn -> {
                    String attrCode = format("@%s", cn.substring(0, cn.length() - 4));
                    if (skColumnCodeEntityMap.containsKey(cn)) {
                        String entityTp = skColumnCodeEntityMap.get(cn);
                        attrCode = format("%s@%s", cn.substring(0, cn.indexOf(entityTp) - 1), entityTp);
                    }
                    return attrCode;
                }).collect(toList());
    }

    public String getInternalEksIbsFile() {
        return internalEksIbsFile;
    }

    public Column getSkColKalita() {
        return concat(
                lit("kalita"),
                lit("|"),
                coalesce(col("form_key_"), lit("")),
                lit("|"),
                coalesce(col("proc_def_id_"), lit("")),
                lit("|"),
                coalesce(col("handler_cfg_"), lit("")))
                .cast("string");
    }
}