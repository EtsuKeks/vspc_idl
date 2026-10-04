package ru.sberbank.bigdata.dffess.vspc.common;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.core.Transformation;
import ru.sberbank.bigdata.sl.rt.core.exceptions.AppException;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters;
import ru.sberbank.bigdata.sl.sk.SkCommonException;
import ru.sberbank.bigdata.sl.sk.client.SkClient;
import ru.sberbank.bigdata.sl.sk.client.SkClientException;

import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.sl.rt.reader.constants.ReaderConstants.*;

public abstract class BaseTransformation extends Transformation {
    public static final String SID = "sid";
    public static final String NEW_KEY_FLAG = "_etl_new_key";
    protected static String SHORT_PRODUCT_CODE;
    public static String STGTMP_SCHEMA;
    public static String STG_SCHEMA;
    public static String SNP_SCHEMA;
    public static String DIFF_SCHEMA;
    public static String HIST_POSTFIX = "_" + HIST_STRING;
    public static final String SOURCE_SK_SUFFIX = "_sid";
    public static final String TARGET_SK_SUFFIX = "_sk";
    protected App app;
    protected String code;
    protected String[] sets;
    protected Logger logger;
    private SkClient skClient;

    protected BaseTransformation(App app, String code, String... sets) {
        super(app, code, sets);
        this.code = code;
        this.app = app;
        this.sets = sets;

        logger = new Logger(app, this.getClass());

        // Настройка ССК
        System.setProperty("service_data_product_code", app.ctx().getShortProductCode());
        System.setProperty("service_init_greenfield", app.ctx().getProperty("sk_greenfield", "false"));
        if (!app.ctx().getLogEnv().isEmpty()) {
            System.setProperty("app_log_env", app.ctx().getLogEnv());
        }
        String turboBoost = app.ctx().getProperty("turbo_boost", "");
        if (!turboBoost.isEmpty()) {
            System.setProperty("turbo_boost", turboBoost);
        }
        String serviceInstanceCode = app.ctx().getProperty("service_instance_code", "");
        if (!serviceInstanceCode.isEmpty()) {
            System.setProperty("service_instance_code", serviceInstanceCode);
        }

        // Инициализация статических переменных
        AppContext context = app.ctx();
        BaseTransformation.initStaticEnv(context);
    }

    protected BaseTransformation(App app, Class clazz, String... sets) {
        this(app, clazz.getSimpleName(), sets);
    }

    public static void runApp(String[] args, TransformationProvider provider) {
        App app = App.create(null, args)
                .terminator(false)
                .build();

        AppContext context = app.ctx();
        BaseTransformation.initStaticEnv(context);

        // Создание трансформации
        BaseTransformation transformation = provider.transformation(app);

        // Созданные трансформации необходимо добавить в app
        app.addTransformation(transformation);

        //  Старт работы приложения.
        //  При этом исполняются все определенные в рамках данного приложения трансформации (объекты
        //  Transformation) в порядке с учётом зависимостей
        //
        try {
            app.run();
        } catch (AppException e) {
            app.log(e.getLogMessage());
            throw e;
        }
    }

    @FunctionalInterface
    protected interface TransformationProvider {
        BaseTransformation transformation(App app);
    }

    public Dataset<Row> selectFrom(String schema, String table) {
        return app.getSparkSession().sql(format("select * from %s.%s", schema, table));
    }

    public Dataset<Row> selectSQL(String sql) {
        return app.getSparkSession().sql(sql);
    }

    public static void initStaticEnv(AppContext context) {
        String logEnvPrefix = !context.getLogEnv().isEmpty() ? context.getLogEnv() + "_" : "";
        SHORT_PRODUCT_CODE = context.getShortProductCode();
        STGTMP_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stgtmp";
        STG_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stg";
        SNP_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_snp";
        DIFF_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_diff";
    }

    /**
     * Техническая запись (только *_stg/*_stgtmp — контроль PUB_013) штатным методом фреймворка:
     * external parquet по локации схемы, дроп+пересоздание таблицы по схеме набора, REFRESH,
     * salting-ребаланс. saveMode игнорируется: фреймворковый метод всегда пишет с перезаписью.
     */
    protected void saveTable(Dataset<Row> entityDataset, String targetSchema, String targetTable, SaveMode saveMode) {
        if (entityDataset != null) {
            saveToExternalTemporaryTable(entityDataset, targetSchema, targetTable);
            app.getSparkSession().sql(format("ANALYZE TABLE %s.%s COMPUTE STATISTICS", targetSchema, targetTable));
            logger.logInfo(format("Таблица %s.%s сохранена", targetSchema, targetTable));
        }
    }

    protected Dataset<Row> readTable(String targetSchema, String targetTable) {
        return app.getSparkSession().sql(format("select * from %s.%s", targetSchema, targetTable));
    }

    /**
     * Признак инкрементального режима текущей загрузки.
     */
    protected boolean isIncrement() {
        return app.ctx().getLoadMode() == App.LoadMode.INCREMENT;
    }

    /**
     * Помечает строки набора флагом {@link #NEW_KEY_FLAG}: ключ отсутствует в текущем SNP
     * (новый ключ бизнес-истории). Используется для воспроизведения ESSA-правила дат:
     * для новых ключей start_dt = 1900-01-01, для существующих — ctl_validfrom - 1.
     */
    protected Dataset<Row> flagNewKeys(Dataset<Row> df, String snpTable, String pkColumn) {
        Dataset<Row> existingKeys = readTable(SNP_SCHEMA, snpTable)
                .select(col(pkColumn).as("_etl_pk"))
                .distinct();
        return df.join(existingKeys, df.col(pkColumn).equalTo(existingKeys.col("_etl_pk")), "left")
                .withColumn(NEW_KEY_FLAG,
                        when(existingKeys.col("_etl_pk").isNotNull(), lit(false)).otherwise(lit(true)))
                .drop(existingKeys.col("_etl_pk"));
    }

    protected Dataset<Row> addSk(Dataset<Row> mappedDfwc,
                                 String tableName,
                                 String attributeCode) {
        return addSk(mappedDfwc, tableName, Collections.singletonList(attributeCode), emptyMap(), SaveMode.Overwrite);
    }

    protected Dataset<Row> addSk(Dataset<Row> mappedDfwc,
                                 String tableName,
                                 List<String> attributeCode) {
        return addSk(mappedDfwc, tableName, attributeCode, emptyMap(), SaveMode.Overwrite);
    }

    protected Dataset<Row> addSk(Dataset<Row> mappedDfwc,
                                 String tableName,
                                 Map<String, String> skColumnCodeEntityMap) {
        return addSk(mappedDfwc, tableName, emptyList(), skColumnCodeEntityMap, SaveMode.Overwrite);
    }

    protected Dataset<Row> addSk(Dataset<Row> mappedDfwc,
                                 String tableName,
                                 List<String> attributeCodes,
                                 Map<String, String> skColumnCodeEntityMap,
                                 SaveMode saveMode) {
        try {
            Dataset<Row> fixedToNewSkDf = fixDfStructure(mappedDfwc);

            List<String> attributeCodesResolved = attributeCodes.isEmpty() ? getAttributeCodes(
                    Arrays.stream(fixedToNewSkDf.columns()).filter(cn -> cn.endsWith(SOURCE_SK_SUFFIX)).collect(toList()),
                    skColumnCodeEntityMap) : attributeCodes;

            List<String> columnsFromAttributes = getColumnsFromAttributes(attributeCodesResolved);

            Dataset<Row> castedSids = castSidsToString(fixedToNewSkDf, columnsFromAttributes);

            // Поскольку одновременно могут быть запущены несколько трансформаций, работающих с одной и той же
            // таблицей, необходимо предотвратить потенциальные конфликты.
            String uniqueSuffixPerTransformation = Integer.toHexString(this.hashCode());
            String mappedTableName = tableName + "_mapped_" + uniqueSuffixPerTransformation;
            saveTable(castedSids, STGTMP_SCHEMA, mappedTableName, saveMode);
            Dataset<Row> readDf = app.getSparkSession().sql(format("select * from %1$s.%2$s", STGTMP_SCHEMA, mappedTableName)).cache();

            // Заполняем датасет сгенерированными суррогатными ключами
            Dataset<Row> responseDf = runSkGen(
                    !"true".equals(ctx().getProperty(ApplicationParameters.DEBUG_SK_OFF.getParamName())),
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
        List<String> skColListRename = Arrays.stream(df.columns()).filter(cn -> cn.endsWith(TARGET_SK_SUFFIX)).collect(toList());
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
                        if (cn.indexOf(entityTp) > 0) {
                            attrCode = format("%s@%s", cn.substring(0, cn.indexOf(entityTp) - 1), entityTp);
                        }
                    }
                    return attrCode;
                }).collect(toList());
    }

    /**
     * Обращение к сервису СК для генерации суррогатных ключей
     *
     * @param skEnabled SK включен
     * @param tableName физическое название таблицы, для которой запрашивается генерация
     * @param df        набор данных
     * @return набор данных с заполненными суррогатными ключами
     */
    public Dataset<Row> runSkGen(boolean skEnabled,
                                 String tableName,
                                 Dataset<Row> df,
                                 List<String> attributeCodes) throws SkClientException {

        Dataset<Row> resDataSet = df;

        // Пропускаем генерацию SK, если включен параметр debug_sk_off.
        // В этом случае все _sk поля будут иметь тип данных Long
        if (skEnabled) {
            // Обращаемся к сервису, если датасет не пустой
            if (!resDataSet.isEmpty()) {

                // Воспроизведение протокола ESSA SkClientWrapper.initSk: при инициализирующей загрузке
                // сущность сначала онбордится в ССК (bulk load натуральных ключей), затем мапится.
                // Для исторических ESSA-лоадеров (AddrH/DivH/DivType вызывали incrementSk безусловно)
                // онбординг отключается переопределением skInitOnLoad().
                if (!isIncrement() && skInitOnLoad()) {
                    try {
                        Map<Integer, Dataset<Row>> initResult = getSkClient().init(tableName, resDataSet);
                        resDataSet = initResult.get(0);
                    } catch (SkCommonException e) {
                        throw new SkClientException(e.getMessage(), e);
                    }
                }

                resDataSet = getSkClient().map(resDataSet, attributeCodes)
                        .drop(getColumnsFromAttributes(attributeCodes).toArray(new String[0]));

            } else {
                logger.logInfo("EmptyTable - " + tableName);
                // mapping _sid back to _sk
                resDataSet = remapSkToSid(resDataSet, getColumnsFromAttributes(attributeCodes));
            }
        } else {
            logger.logInfo("sk disabled - {}" + tableName);
            // mapping _sid back to _sk
            resDataSet = remapSkToSid(resDataSet, getColumnsFromAttributes(attributeCodes));
        }

        return resDataSet;
    }

    /**
     * Онбординг сущности в ССК при инициализирующей загрузке (протокол ESSA SkClientWrapper.initSk:
     * bulk load натуральных ключей через skClient.init перед map).
     * Исторические ESSA-лоадеры (AddrH/DivH/DivType) вызывали incrementSk даже в INIT-режиме —
     * соответствующие трансформации возвращают false.
     */
    protected boolean skInitOnLoad() {
        return true;
    }

    public List<String> getColumnsFromAttributes(List<String> attributeCodes) {
        return attributeCodes.stream().map(code -> {
            String replacement = code.startsWith("@") ? "" : "_";
            return format("%s%s", code.replace("@", replacement), SOURCE_SK_SUFFIX);
        }).collect(Collectors.toList());
    }

    Dataset<Row> remapSkToSid(Dataset<Row> df, List<String> skColList) {
        Dataset<Row> resDataSet = df;
        for (String name : skColList) {
            resDataSet = resDataSet.withColumnRenamed(name, format("%ssk", name.substring(0, name.length() - 3)));
        }
        return resDataSet;
    }

    private SkClient getSkClient() {
        if (skClient == null) {
            skClient = SkClient.getInstance();
        }
        return skClient;
    }
}