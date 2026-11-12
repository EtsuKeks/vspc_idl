package ru.sberbank.bigdata.dffess.vspc.common;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatHooks;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;
import ru.sberbank.bigdata.dffess.vspc.compat.guard.PublishPreconditions;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.core.Recordset;
import ru.sberbank.bigdata.sl.rt.core.Transformation;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.publisher.CalculationMode;
import ru.sberbank.bigdata.sl.rt.publisher.CalculationModes;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfo;
import ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters;
import ru.sberbank.bigdata.sl.sk.AppEnv;
import ru.sberbank.bigdata.sl.sk.SkCommonException;
import ru.sberbank.bigdata.sl.sk.client.ClientEnv;
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
    private SkClient skClient;

    // debug_sk_off флаг: считаются ли ключи локально.
    private final boolean skOff;

    // Маски наборов из конструктора: по ним RT-FW строит граф и учёт в CTL, publishChecked публикует в тот же набор.
    private final String[] publishMasks;

    protected BaseTransformation(App app, String code, String... sets) {
        super(app, code, sets);
        this.publishMasks = sets;
        this.code = code;
        this.app = app;

        skOff = Boolean.parseBoolean(ctlParam(ApplicationParameters.DEBUG_SK_OFF.getParamName(), "false", "true", "false"));

        // Клиент ССК берёт настройки из системных свойств — ставим их, как ESSA-версия витрины. По сути это поведение самого
        // клиента: те же допустимые значения и те же умолчания, что он подставил бы сам, если свойство не задано. Кроме turbo_boost.
        setSkProperty(AppEnv.SERVICE_DATA_PRODUCT_CODE, app.ctx().getShortProductCode());
        setSkProperty(ClientEnv.SERVICE_INIT_GREENFIELD, ctlParam("sk_greenfield", "false", "true", "false"));
        // turbo_boost: у клиента по умолчанию internal, у ESSA-версии — true: уже выданные ключи и своих, и чужих продуктов
        // ищутся прямо в HBase, а не через сервис ССК. Новые ключи в любом случае выдаёт сервис.
        setSkProperty(ClientEnv.TURBO_BOOST, ctlParam("turbo_boost", "true", "true", "false", "internal"));
        setSkProperty(AppEnv.LOG_ENV, app.ctx().getLogEnv());
        setSkProperty(AppEnv.SERVICE_INSTANCE_CODE, ctlParam("service_instance_code", app.ctx().getInstanceCode()));

        // Схемы продукта — так же, как их строит публикатор RT-FW: usl_[<ЛО>_]vspc_idl_<слой>.
        AppContext context = app.ctx();
        SHORT_PRODUCT_CODE = context.getShortProductCode();
        String logEnvPrefix = context.getLogEnv().isEmpty() ? "" : context.getLogEnv() + "_";
        STGTMP_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stgtmp";
        STG_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stg";
        SNP_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_snp";
        DIFF_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_diff";
    }

    // Системное свойство для клиента ССК; пустое значение не ставим.
    private static void setSkProperty(String name, String value) {
        if (value != null && !value.trim().isEmpty()) {
            System.setProperty(name, value.trim());
        }
    }

    // Параметр CTL: пусто — умолчание; иначе в нижнем регистре и, если задан список, одно из допустимых — иначе отказ.
    private String ctlParam(String name, String byDefault, String... allowed) {
        String value = app.ctx().getProperty(name, "").trim().toLowerCase();
        if (value.isEmpty()) {
            return byDefault;
        }
        if (allowed.length > 0 && !Arrays.asList(allowed).contains(value)) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                    format("Параметр CTL %s = '%s': допустимо %s или пусто", name, value, Arrays.asList(allowed))));
        }
        return value;
    }

    // Публикация витрины в набор из конструктора: вендорский merge-путь с поправками compat.guard.PublishPreconditions. */
    protected final void publishChecked(Dataset<Row> result) {
        if (publishMasks.length != 1) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, format(
                    "Трансформация %s должна объявить в конструкторе ровно один набор", code, publishMasks.length)));
        }
        String mask = publishMasks[0];
        CompatTables.Table table = CompatTables.register(app, mask);
        SparkSession spark = app.getSparkSession();
        // вендор читает вход дважды (isEmpty и запись в stgtmp)
        Dataset<Row> ds = CompatHooks.emptyIncrMarker(PublishPreconditions.alignTypes(result, table.snpTable(), spark).persist(),
                table, app.ctx(), spark);
        try {
            PublishPreconditions.keepLegs(DIFF_SCHEMA, table.code(), STG_SCHEMA, app.ctx(), spark, () -> publish(ds)
                    .calculationMode(sparkRebalance())
                    .recordsetMasks(mask)
                    .launch());
        } finally {
            ds.unpersist();
        }

        // RT-FW ведёт в CTL статистики не только по продукту, но и по каждому набору — в его собственной сущности; при первом
        // запуске на RT-FW эти сущности он создаёт сам. Статистику 2 на продукт он публикует, только если хотя бы один набор
        // помечен изменённым, — поэтому проще всего пометить набор, как того требует RT-FW. Для единообразия помечаем все наборы.
        registerChangedSets(Recordset.recordsets(app, mask));
    }

    protected Dataset<Row> readTable(String targetSchema, String targetTable) {
        return ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.readTable(app.getSparkSession(), targetSchema, targetTable);
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
                app.log(new LogMessage("", LogType.TECH_INFO, format("СК для %s посчитаны локально (ССК отключён)", normalizedTable)));
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
            app.log(new LogMessage("", LogType.TECH_ERROR, "Провал интеграции с ССК: " + e.getMessage()));
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

    /** Черновик для ССК в stgtmp (в 3.0 — forsk_<t>): таблицы в DDL нет, поэтому пишем писателем RT-FW (создаёт таблицу) тем же режимом, что публикация, и читаем обратно. */
    private Dataset<Row> readFrozenDraft(String draftTable, Dataset<Row> dataset) {
        SparkSession spark = app.getSparkSession();
        TableInfo ti = new TableInfo(STGTMP_SCHEMA, draftTable, getDatabaseLocationPath(spark, STGTMP_SCHEMA));
        PublisherDao dao = new PublisherDao(spark, sparkRebalance());
        dao.writeTable(ti, scalaSeq(Collections.<String>emptyList()), dataset);
        return dao.readTable(ti).cache();
    }

    private Dataset<Row> runSkGen(String tableName,
                                  Dataset<Row> df,
                                  List<String> mapAttributeCodes,
                                  List<String> attributeCodes) throws SkClientException {
        if (df.isEmpty()) {
            app.log(new LogMessage("", LogType.TECH_INFO, "EmptyTable - " + tableName));
            for (String cn : getColumnsFromAttributes(attributeCodes)) {
                df = df.withColumnRenamed(cn, cn.replace(SOURCE_SK_SUFFIX, TARGET_SK_SUFFIX));
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

    /**
     * Как раскладывать строки по файлам перед записью — на данные не влияет. По умолчанию RT-FW делает лишний проход по данным
     * (persist + count) и собирает статистику по всем таблицам плана; подсказка rebalance отдаёт это Spark (AQE), без лишних проходов.
     * Работает, только если в сессии включены spark.sql.adaptive.enabled и spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled
     * (заданы в vars_all.yml), иначе RT-FW молча вернётся к своему способу. Новый объект на вызов: withApproach меняет сам объект.
     */
    private static CalculationMode sparkRebalance() {
        return new CalculationMode(CalculationModes.HARD).withApproach(CalculationMode.Approach.SPARK_REBALANCE);
    }

    private SkClient getSkClient() {
        if (skClient == null) {
            skClient = SkClient.getInstance();
        }
        return skClient;
    }
}
