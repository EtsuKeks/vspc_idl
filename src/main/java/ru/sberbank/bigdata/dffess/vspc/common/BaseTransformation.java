package ru.sberbank.bigdata.dffess.vspc.common;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;
import ru.sberbank.bigdata.dffess.vspc.compat.guard.PublishPreconditions;
import ru.sberbank.bigdata.dffess.vspc.constant.EssaLoaders;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.core.Recordset;
import ru.sberbank.bigdata.sl.rt.core.Transformation;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.publisher.CalculationMode;
import ru.sberbank.bigdata.sl.rt.publisher.CalculationModes;
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

public abstract class BaseTransformation extends Transformation {
    public static final String SID = "sid";
    protected static String SHORT_PRODUCT_CODE;
    public static String STGTMP_SCHEMA;
    public static String STG_SCHEMA;
    public static final String SOURCE_SK_SUFFIX = "_sid";
    public static final String TARGET_SK_SUFFIX = "_sk";
    protected App app;
    protected String code;
    private SkClient skClient;

    // Служебная колонка проверки ключа версии в publishChecked: номер строки внутри ключа.
    private static final String KEY_RN = "_vspc_key_rn";

    // debug_sk_off флаг: считаются ли ключи локально
    private final boolean skOff;

    // Маски наборов из конструктора: по ним RT-FW строит граф и учёт в CTL, publishChecked публикует в тот же набор
    private final String[] publishMasks;

    protected BaseTransformation(App app, String code, String... sets) {
        super(app, code, sets);
        this.publishMasks = sets;
        this.code = code;
        this.app = app;

        skOff = Boolean.parseBoolean(ctlParam(ApplicationParameters.DEBUG_SK_OFF.getParamName(), "false", "true", "false"));

        // Клиент ССК берёт настройки из системных свойств — ставим их, как ESSA-версия витрины. По сути это поведение самого
        // клиента: те же допустимые значения и те же умолчания, что он подставил бы сам, если свойство не задано.
        // Кроме turbo_boost - о нем ниже.
        setSkProperty(AppEnv.SERVICE_DATA_PRODUCT_CODE, app.ctx().getShortProductCode());
        setSkProperty(ClientEnv.SERVICE_INIT_GREENFIELD, ctlParam("sk_greenfield", "false", "true", "false"));

        // turbo_boost: у клиента по умолчанию internal, у ESSA-версии — true: уже выданные ключи и своих, и чужих 
        // продуктов ищутся прямо в HBase, а не через сервис ССК. Новые ключи в любом случае выдаёт сервис.
        setSkProperty(ClientEnv.TURBO_BOOST, ctlParam("turbo_boost", "true", "true", "false", "internal"));
        setSkProperty(AppEnv.LOG_ENV, app.ctx().getLogEnv());
        setSkProperty(AppEnv.SERVICE_INSTANCE_CODE, ctlParam("service_instance_code", app.ctx().getInstanceCode()));

        // Схемы продукта — так же, как их строит публикатор RT-FW: usl_[<ЛО>_]vspc_idl_<слой>.
        AppContext context = app.ctx();
        SHORT_PRODUCT_CODE = context.getShortProductCode();
        String logEnvPrefix = context.getLogEnv().isEmpty() ? "" : context.getLogEnv() + "_";
        STGTMP_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stgtmp";
        STG_SCHEMA = "usl_" + logEnvPrefix + SHORT_PRODUCT_CODE + "_stg";
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

    protected final void publishChecked(Dataset<Row> result) {
        String mask = publishMask();

        // Описание таблицы для поправок ESSA кладём в общий реестр по имени таблицы: внутри публикации RT-FW передаёт
        // нашим вставкам только имя таблицы, и они достают описание оттуда.
        CompatTables.Table table = compatTable();
        SparkSession spark = app.getSparkSession();

        // Ключ версии во входе RT-FW должен быть заполнен и уникален. Строки с пустой колонкой ключа отбрасываем, из
        // строк с одним ключом оставляем первую: порядок внутри ключа — по хэшу всей строки,
        // чтобы выбор не зависел от порядка строк во входе. persist — чтобы проверка на пустоту в RT-FW и сама
        // публикация не пересчитывали трансформацию дважды.
        Dataset<Row> aligned = PublishPreconditions.alignTypes(result, table.snpTable(), spark);
        Column[] key = table.versionKey().stream().map(functions::col).toArray(Column[]::new);
        Column[] vals = Arrays.stream(aligned.columns())
                              .map(c -> coalesce(col(c).cast("string"), lit(""))).toArray(Column[]::new);
        WindowSpec w = Window.partitionBy(key).orderBy(xxhash64(vals));
        Column nullKey = Arrays.stream(key).map(Column::isNull).reduce(Column::or).get();
        Dataset<Row> marked = aligned.withColumn(KEY_RN, row_number().over(w)).persist();
        Dataset<Row> ds = marked.filter(not(nullKey).and(col(KEY_RN).equalTo(1))).drop(KEY_RN);

        try {
            // Тут внутри идет штатная публикация RT-FW с нашими ESSA-поправками к расчёту изменений и слиянию (теневой ProcessingStage).
            // calculationMode — как RT-FW раскладывает строки по файлам перед каждой своей записью (stgtmp, snp, diff); на данные
            // не влияет, только на время и размер файлов. HARD — единственный режим RT-FW, выбор только в способе (Approach):
            // по умолчанию SALTING — перед записью persist + count и статистика плана, т.е. лишний полный проход по данным;
            // SPARK_REBALANCE — просто hint("rebalance"), раскладывает сам Spark при записи. Срабатывает, только если включены
            // spark.sql.adaptive.enabled и spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled (vars_all.yml),
            // иначе RT-FW молча вернётся к SALTING.
            publish(ds).calculationMode(new CalculationMode(CalculationModes.HARD)
                                            .withApproach(CalculationMode.Approach.SPARK_REBALANCE)
            ).recordsetMasks(mask).launch();
        } finally {
            marked.unpersist();
        }

        // RT-FW ведёт в CTL статистики не только по продукту, но и по каждому набору — в его собственной сущности; при первом
        // запуске на RT-FW эти сущности он создаёт сам. Статистику 2 на продукт он публикует, только если хотя бы один набор
        // помечен изменённым, — поэтому проще всего пометить набор, как того требует RT-FW. Для единообразия помечаем все наборы.
        registerChangedSets(Recordset.recordsets(app, mask));
    }

    // Единственный набор из конструктора: в него публикует publishChecked.
    private String publishMask() {
        if (publishMasks.length != 1) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, 
                format("Трансформация %s должна объявить в конструкторе ровно один набор", code)));
        }
        return publishMasks[0];
    }

    // Описание публикуемой таблицы для наших ESSA-поправок: имя snp, ключ версии, партиции.
    protected final CompatTables.Table compatTable() {
        return CompatTables.register(app, publishMask(), EssaLoaders.BY_TRANSFORMATION.get(code));
    }

    protected Dataset<Row> readTable(String targetSchema, String targetTable) {
        return app.getSparkSession().table(targetSchema + "." + targetTable);
    }

    public App getApp() {
        return app;
    }

    protected Dataset<Row> addSk(Dataset<Row> mappedDfwc, String tableName, List<String> attributeCodes) {
        // Ищем все ключевые колонки в таблице: @div -> div_sid; fact@addr -> fact_addr_sid.
        List<String> skColumns = attributeCodes.stream().map(code -> {
            String replacement = code.startsWith("@") ? "" : "_";
            return format("%s%s", code.replace("@", replacement), SOURCE_SK_SUFFIX);
        }).collect(toList());

        try {
            String normalizedTable = tableName.trim().toLowerCase();
            if (skOff) {
                Dataset<Row> local = mappedDfwc;
                for (String cn : skColumns) {
                    String skCol = cn.replace(SOURCE_SK_SUFFIX, TARGET_SK_SUFFIX);
                    local = local.withColumn(skCol, xxhash64(col(cn)).cast(DataTypes.LongType)).drop(cn);
                }
                app.log(new LogMessage(
                    "", LogType.TECH_INFO, format("СК для %s посчитаны локально (ССК отключён)", normalizedTable)));
                return local;
            }

            // ESSA: натуральный ключ уходит в ССК как есть; черновик с постоянным именем
            Dataset<Row> frozen = readFrozenDraft(normalizedTable + "_mapped", mappedDfwc);
            try {
                // В INIT режиме метод init красит свой ключ, map — только чужие
                Dataset<Row> resDataSet;
                if (!frozen.isEmpty()
                        && app.ctx().getLoadMode() != App.LoadMode.INCREMENT
                        && attributeCodes.contains("@" + normalizedTable)) {
                    Dataset<Row> painted = getSkClient().init(normalizedTable, frozen).get(0);
                    List<String> fkAttributeCodes = attributeCodes.stream()
                            .filter(code -> !("@" + normalizedTable).equals(code))
                            .collect(toList());
                    resDataSet = getSkClient().map(painted, fkAttributeCodes);
                } else {
                    // На пустом входе map в сервис не ходит: сам добавляет пустые колонки *_sk.
                    resDataSet = getSkClient().map(frozen, attributeCodes);
                }

                // map добавляет *_sk рядом с *_sid: исходные *_sid убираем, ключи приводим к long.
                for (String cn : skColumns) {
                    String skCol = cn.replace(SOURCE_SK_SUFFIX, TARGET_SK_SUFFIX);
                    resDataSet = resDataSet.withColumn(skCol, col(skCol).cast(DataTypes.LongType)).drop(cn);
                }
                return resDataSet;
            } finally {
                // init и map свои проходы по черновику уже сделали (или упали) — освобождаем память.
                frozen.unpersist();
            }
        } catch (SkClientException | SkCommonException e) {
            // RT-FW пишет сообщение исключения в журнал и в CTL, но без причины — поэтому причина в тексте.
            throw new TransformationException(new LogMessage(
                "", LogType.TECH_ERROR, "Провал интеграции с ССК: " + e.getMessage()), e);
        }
    }

    // Черновик для ССК в stgtmp: фиксируем вход, чтобы init/map читали одни и те же строки, а не
    // пересчитывали трансформацию. path делает таблицу внешней, hint("rebalance") — раскладка по файлам, как у RT-FW 
    // при публикации.
    private Dataset<Row> readFrozenDraft(String draftTable, Dataset<Row> dataset) {
        SparkSession spark = app.getSparkSession();
        String table = STGTMP_SCHEMA + "." + draftTable;
        dataset.hint("rebalance").write().mode(SaveMode.Overwrite)
                .option("path", getDatabaseLocationPath(spark, STGTMP_SCHEMA) + "/" + draftTable)
                .format("parquet")
                .saveAsTable(table);
        return spark.table(table).persist();
    }

    private SkClient getSkClient() {
        if (skClient == null) {
            skClient = SkClient.getInstance();
        }
        return skClient;
    }
}
