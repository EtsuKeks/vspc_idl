package ru.sberbank.bigdata.dffess.vspc.common;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.dffess.vspc.common.publish.VspcPublisher;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.core.Transformation;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.sk.SkCommonException;
import ru.sberbank.bigdata.sl.sk.client.SkClient;
import ru.sberbank.bigdata.sl.sk.client.SkClientException;

import java.util.*;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;

public abstract class BaseTransformation extends Transformation {
    public static final String SID = "sid";
    protected static String SHORT_PRODUCT_CODE;
    public static String STGTMP_SCHEMA;
    public static String STG_SCHEMA;
    public static String SNP_SCHEMA;
    public static final String SOURCE_SK_SUFFIX = "_sid";
    public static final String TARGET_SK_SUFFIX = "_sk";
    protected App app;
    protected String code;
    protected Logger logger;
    private SkClient skClient;

    protected BaseTransformation(App app, String code) {
        super(app, code);
        this.code = code;
        this.app = app;

        logger = new Logger(app, this.getClass());

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

        AppContext context = app.ctx();
        BaseTransformation.initStaticEnv(context);
    }

    public static void initStaticEnv(AppContext context) {
        String logEnvPrefix = !context.getLogEnv().isEmpty() ? context.getLogEnv() + "_" : "";
        SHORT_PRODUCT_CODE = context.getShortProductCode();
        STGTMP_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stgtmp";
        STG_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stg";
        SNP_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_snp";
    }

    protected void saveTable(Dataset<Row> entityDataset, String targetSchema, String targetTable) {
        if (entityDataset != null) {
            saveToExternalTemporaryTable(entityDataset, targetSchema, targetTable);
            app.getSparkSession().sql(format("ANALYZE TABLE %s.%s COMPUTE STATISTICS", targetSchema, targetTable));
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

    protected void publishVspc(Dataset<Row> result) {
        VspcPublisher.instance().publish(this, result);
    }

    protected Dataset<Row> addSk(Dataset<Row> mappedDfwc, String tableName, List<String> attributeCodes) {
        List<String> skColumns = getColumnsFromAttributes(attributeCodes);
        try {
            String normalizedTable = tableName.trim().toLowerCase();
            Dataset<Row> frozen = mappedDfwc;
            for (String cn : skColumns) {
                frozen = frozen.withColumn(cn, regexp_replace(col(cn).cast(DataTypes.StringType), "\\.0+$", ""));
            }
            frozen = readFrozenDraft(normalizedTable + "_mapped_" + Integer.toHexString(this.hashCode()), frozen);

            if (!frozen.isEmpty()
                    && app.ctx().getLoadMode() != App.LoadMode.INCREMENT
                    && attributeCodes.contains("@" + normalizedTable)) {
                initRegistry(frozen, normalizedTable);
            }

            Dataset<Row> resDataSet = runSkGen(normalizedTable, frozen, attributeCodes);
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

    private void initRegistry(Dataset<Row> frozenFrame, String normalizedTable) throws SkClientException {
        Dataset<Row> ownProjection = frozenFrame.select(normalizedTable + SOURCE_SK_SUFFIX);
        Dataset<Row> initDraft = readFrozenDraft(
                normalizedTable + "_init_" + Integer.toHexString(this.hashCode()), ownProjection);
        try {
            getSkClient().init(normalizedTable, initDraft).get(0);
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
                                  List<String> attributeCodes) throws SkClientException {
        if (df.isEmpty()) {
            logger.logInfo("EmptyTable - " + tableName);
            for (String cn : getColumnsFromAttributes(attributeCodes)) {
                df = df.withColumnRenamed(cn, format("%ssk", cn.substring(0, cn.length() - 3)));
            }
            return df;
        }
        return getSkClient().map(df, attributeCodes)
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
