package ru.sberbank.bigdata.dffess.vspc.common;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatHooks;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;
import ru.sberbank.bigdata.dffess.vspc.compat.guard.PublishPreconditions;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.core.Transformation;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfo;
import ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters;
import ru.sberbank.bigdata.sl.sk.SkCommonException;
import ru.sberbank.bigdata.sl.sk.client.SkClient;
import ru.sberbank.bigdata.sl.sk.client.SkClientException;

import java.util.*;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.getDatabaseLocationPath;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.scalaSeq;

public abstract class BaseTransformation extends Transformation {
    public static final String SID = "sid";
    protected static String SHORT_PRODUCT_CODE;
    public static String STGTMP_SCHEMA;
    public static String STG_SCHEMA;
    public static String SNP_SCHEMA;
    public static String DIFF_SCHEMA;
    public static final String SOURCE_SK_SUFFIX = "_sid";
    public static final String TARGET_SK_SUFFIX = "_sk";
    protected App app;
    protected String code;
    protected Logger logger;
    private SkClient skClient;
    /** debug_sk_off=Y/true: ключи считаются локально (3.0 CtlAdapterImpl.java:142-157; D/ON_GF/OFF_TB CTL 3.16 не пропускает). */
    private final boolean skOff;

    protected BaseTransformation(App app, String code, String... sets) {
        super(app, code, sets);
        this.code = code;
        this.app = app;

        logger = new Logger(app, this.getClass());

        String debugSkOff = app.ctx().getProperty(ApplicationParameters.DEBUG_SK_OFF.getParamName(), "");
        skOff = "Y".equalsIgnoreCase(debugSkOff) || "true".equalsIgnoreCase(debugSkOff);

        // 3.0 SkClientWrapper.java:44-54: turbo_boost по умолчанию "true"; код экземпляра — из имени потока CTL
        System.setProperty("service_data_product_code", app.ctx().getShortProductCode());
        System.setProperty("service_init_greenfield", app.ctx().getProperty("sk_greenfield", "false"));
        if (!app.ctx().getLogEnv().isEmpty()) {
            System.setProperty("app_log_env", app.ctx().getLogEnv());
        }
        String turboBoost = app.ctx().getProperty("turbo_boost", "");
        System.setProperty("turbo_boost", turboBoost.isEmpty() ? "true" : turboBoost);
        String serviceInstanceCode = app.ctx().getProperty("service_instance_code", "");
        if (serviceInstanceCode.trim().isEmpty()) {
            serviceInstanceCode = app.ctx().getInstanceCode();
        }
        if (serviceInstanceCode != null && !serviceInstanceCode.trim().isEmpty()) {
            System.setProperty("service_instance_code", serviceInstanceCode.toLowerCase());
        }

        AppContext context = app.ctx();
        BaseTransformation.initStaticEnv(context);
    }

    public static void initStaticEnv(AppContext context) {
        String logEnvPrefix = !context.getLogEnv().isEmpty() ? context.getLogEnv() + "_" : "";
        SHORT_PRODUCT_CODE = context.getShortProductCode();
        STGTMP_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stgtmp";
        STG_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stg";
        SNP_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_snp";
        DIFF_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_diff";
    }

    /** Публикация витрины: вендорский merge-путь с поправками compat.guard.PublishPreconditions. */
    protected final void publishChecked(Dataset<Row> result, String mask) {
        CompatTables.Table table = CompatTables.byMask(mask);
        SparkSession spark = app.getSparkSession();
        // вендор читает вход дважды (isEmpty и запись в stgtmp)
        Dataset<Row> ds = CompatHooks.emptyIncrMarker(PublishPreconditions.alignTypes(result, SNP_SCHEMA + "." + table.code(), spark).persist(),
                table, app.ctx(), spark, SNP_SCHEMA + "." + table.code());
        try {
            PublishPreconditions.keepLegs(DIFF_SCHEMA, table.code(), STG_SCHEMA, app.ctx(), spark, () -> publish(ds)
                    .calculationMode(PublisherMode.sparkRebalance())
                    .recordsetMasks(mask)
                    .launch());
        } finally {
            ds.unpersist();
            VspcLoadDate.reset();
        }
    }

    /** DV-45, DV-49: черновик тем же DAO-режимом, что публикация, без persist+count и ANALYZE; сторож схемы — из вендорского saveToExternalTemporaryTable HEAD (V/PublisherImpl.java:474-478). */
    protected void saveTable(Dataset<Row> entityDataset, String targetSchema, String targetTable) {
        if (entityDataset != null) {
            SparkSession spark = app.getSparkSession();
            TableInfo ti = new TableInfo(targetSchema, targetTable, getDatabaseLocationPath(spark, targetSchema));
            if (!ti.isTempSchema()) {
                throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                        "Таблица " + ti.getQualifiedTableName() + " не в схеме технических данных"));
            }
            new PublisherDao(spark, PublisherMode.sparkRebalance()).writeTable(ti, scalaSeq(Collections.<String>emptyList()), entityDataset);
            logger.logInfo(format("Таблица %s.%s сохранена", targetSchema, targetTable));
        }
    }

    protected Dataset<Row> readTable(String targetSchema, String targetTable) {
        return app.getSparkSession().sql(format("select * from %s.%s", targetSchema, targetTable));
    }

    public String getCode() {
        return code;
    }

    public App getApp() {
        return app;
    }

    protected Dataset<Row> addSk(Dataset<Row> mappedDfwc, String tableName, List<String> attributeCodes) {
        List<String> skColumns = getColumnsFromAttributes(attributeCodes);
        try {
            String normalizedTable = tableName.trim().toLowerCase();
            // 3.0 SkClientWrapper.java:99-100, 215-227: OFF — xxhash64(натуральный ключ) без ССК
            if (skOff) {
                Dataset<Row> local = mappedDfwc;
                for (String cn : skColumns) {
                    String skCol = cn.replace(SOURCE_SK_SUFFIX, TARGET_SK_SUFFIX);
                    local = local.withColumn(skCol, xxhash64(col(cn)).cast(DataTypes.LongType)).drop(cn);
                }
                logger.logInfo(format("СК для %s посчитаны локально (ССК отключён)", normalizedTable));
                return local;
            }

            // 3.0: натуральный ключ уходит в ССК как есть; черновик с постоянным именем
            Dataset<Row> frozen = readFrozenDraft(normalizedTable + "_mapped", mappedDfwc);
            // 3.0 SkClientWrapper.java:128-149: на INIT init красит свой ключ, map — только чужие
            Dataset<Row> resDataSet;
            if (!frozen.isEmpty()
                    && app.ctx().getLoadMode() != App.LoadMode.INCREMENT
                    && attributeCodes.contains("@" + normalizedTable)) {
                Dataset<Row> painted = initRegistry(frozen, normalizedTable);
                List<String> fkAttributeCodes = attributeCodes.stream()
                        .filter(code -> !("@" + normalizedTable).equals(code))
                        .collect(toList());
                resDataSet = runSkGen(normalizedTable, painted, fkAttributeCodes, attributeCodes);
            } else {
                resDataSet = runSkGen(normalizedTable, frozen, attributeCodes, attributeCodes);
            }
            for (String cn : skColumns) {
                String skCol = cn.replace(SOURCE_SK_SUFFIX, TARGET_SK_SUFFIX);
                resDataSet = resDataSet.withColumn(skCol, col(skCol).cast(DataTypes.LongType));
            }
            return resDataSet;
        } catch (SkClientException e) {
            logger.logError("Провал интеграции с ССК: " + e.getMessage());
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, "Провал интеграции с ССК"));
        }
    }

    private Dataset<Row> initRegistry(Dataset<Row> frozenFrame, String normalizedTable) throws SkClientException {
        try {
            return getSkClient().init(normalizedTable, frozenFrame).get(0);
        } catch (SkCommonException e) {
            throw new SkClientException(e.getMessage(), e);
        }
    }

    private Dataset<Row> readFrozenDraft(String draftTable, Dataset<Row> dataset) {
        saveTable(dataset, STGTMP_SCHEMA, draftTable);
        return app.getSparkSession().sql(format("select * from %1$s.%2$s", STGTMP_SCHEMA, draftTable)).cache();
    }

    private Dataset<Row> runSkGen(String tableName,
                                  Dataset<Row> df,
                                  List<String> mapAttributeCodes,
                                  List<String> attributeCodes) throws SkClientException {
        if (df.isEmpty()) {
            logger.logInfo("EmptyTable - " + tableName);
            for (String cn : getColumnsFromAttributes(attributeCodes)) {
                df = df.withColumnRenamed(cn, format("%ssk", cn.substring(0, cn.length() - 3)));
            }
            return df;
        }
        return getSkClient().map(df, mapAttributeCodes)
                .drop(getColumnsFromAttributes(attributeCodes).toArray(new String[0]));
    }

    private List<String> getColumnsFromAttributes(List<String> attributeCodes) {
        return attributeCodes.stream().map(code -> {
            String replacement = code.startsWith("@") ? "" : "_";
            return format("%s%s", code.replace("@", replacement), SOURCE_SK_SUFFIX);
        }).collect(toList());
    }

    private SkClient getSkClient() {
        if (skClient == null) {
            skClient = SkClient.getInstance();
        }
        return skClient;
    }
}
