package ru.sberbank.bigdata.dffess.vspc.common;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.dffess.vspc.compat.period.LoaderCache;
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
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.PUBLISH_MODE;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.onePerKey;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.writeWorkTable;

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

    // debug_sk_off флаг: считаются ли ключи локально
    private final boolean skOff;

    // Маски наборов из конструктора: по ним RT-FW строит граф и учёт в CTL, publishChecked публикует в тот же набор
    private final String[] publishMasks;

    protected BaseTransformation(App app, String code, String... sets) {
        super(app, code, sets);
        this.publishMasks = sets;
        this.code = code;
        this.app = app;

        // ЛО и инстанс (RT-FW берёт их из имени потока) витрина не поддерживает — требуем их отсутствия до любых
        // действий.
        AppContext context = app.ctx();
        if (notBlank(context.getLogEnv()) || notBlank(context.getInstanceCode())) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                format("ЛО '%s', инстанс '%s': витрина работает только без ЛО и без инстанса",
                    context.getLogEnv(), context.getInstanceCode())));
        }

        skOff = Boolean.parseBoolean(ctlParam(ApplicationParameters.DEBUG_SK_OFF.getParamName(), "false", "true", "false"));

        // Клиент ССК берёт настройки из системных свойств — ставим их, как ESSA-версия витрины. По сути это поведение
        // самого клиента: те же допустимые значения и те же умолчания, что он подставил бы сам, если свойство не
        // задано. Кроме turbo_boost - о нем ниже.
        setSkProperty(AppEnv.SERVICE_DATA_PRODUCT_CODE, app.ctx().getShortProductCode());
        setSkProperty(ClientEnv.SERVICE_INIT_GREENFIELD, ctlParam("sk_greenfield", "false", "true", "false"));

        // turbo_boost: у клиента по умолчанию internal, у ESSA-версии — true: уже выданные ключи и своих, и чужих 
        // продуктов ищутся прямо в HBase, а не через сервис ССК. Новые ключи в любом случае выдаёт сервис.
        setSkProperty(ClientEnv.TURBO_BOOST, ctlParam("turbo_boost", "true", "true", "false", "internal"));

        // Схемы продукта — так же, как их строит публикатор RT-FW без ЛО: usl_vspc_idl_<слой>.
        SHORT_PRODUCT_CODE = context.getShortProductCode();
        STGTMP_SCHEMA = "usl_" + SHORT_PRODUCT_CODE + "_stgtmp";
        STG_SCHEMA = "usl_" + SHORT_PRODUCT_CODE + "_stg";
    }

    private static boolean notBlank(String s) {
        return s != null && !s.trim().isEmpty();
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

        // Приводим типы, потому что RT-FW за нас этого делать не будет - он сверит колонки с meta и тихо запишет NULL
        // если найдет расхождения.
        Dataset<Row> aligned = PublishPreconditions.alignTypes(result, table.snpTable());
        Column[] key = table.versionKey().stream().map(functions::col).toArray(Column[]::new);

        // RT-FW нужен заполненный и уникальный ключ версии: строки с пустым ключом отбрасываем, из дублей оставляем по
        // одной строке на версию. Группируем по ключу версии (с start_dt — иначе слиплись бы версии истории), а
        // выбираем строку по хэшу без start_dt: он у строк версии один, но меняется каждый день (дата загрузки − 1), и
        // с ним победитель одних и тех же дублей менялся бы от загрузки к загрузке.
        Column nullKey = Arrays.stream(key).map(Column::isNull).reduce(Column::or).get();
        Dataset<Row> ds = onePerKey(aligned.filter(not(nullKey)), table.versionKey(),
            Collections.singletonList(START_DT_FIELD_NAME));
        if (app.ctx().getLoadMode() == App.LoadMode.INCREMENT) {
            // Только INCR: RT-FW читает вход дважды — проверка на пустоту в самом начале публикации и запись в stgtmp
            // (на INIT проверки нет). Поэтому записываем в stg (не кэшируем: кэш может быть вытеснен или исполнитель
            // может потеряться и тогда придется идти в ССК повторно)
            ds = writeWorkTable(STGTMP_SCHEMA, table.code() + "_input", ds);
        }

        // Тут внутри идет штатная публикация RT-FW с нашими ESSA-поправками к расчёту изменений и слиянию (теневой
        // ProcessingStage). calculationMode — как RT-FW раскладывает строки по файлам перед каждой своей записью
        // (stgtmp, snp, diff); на данные не влияет, только на время и размер файлов. HARD — единственный режим
        // RT-FW, выбор только в способе (Approach): по умолчанию SALTING — перед записью persist + count и
        // статистика плана, т.е. лишний полный проход по данным; SPARK_REBALANCE — просто hint("rebalance"),
        // раскладывает сам Spark при записи. Срабатывает, только если включены spark.sql.adaptive.enabled и spark.
        // sql.adaptive.optimizeSkewsInRebalancePartitions.enabled (vars_all.yml), иначе RT-FW молча вернётся к
        // SALTING. Тем же режимом пишем и свои staging таблицы.
        publish(ds).calculationMode(PUBLISH_MODE).recordsetMasks(mask).launch();

        // Только для таблиц с загрузчиком ESSA.EssaLoaders == HistSnp, у остальных ничего не делает. Кэш закрытий:
        // для ключей, пропавших из источника, — дата их закрытия; когда ключ снова появляется, его новая версия
        // открывается днём позже этой даты. Новый кэш калькулятор изменений собрал во время публикации в черновике
        // в stgtmp; публикация прошла (иначе launch бросил бы исключение), поэтому переносим черновик в stg — как
        // и мувер в ESSA-версии который переносил его вместе со снимком. На INIT кэш очищается.
        LoaderCache.commit(table, app.ctx().getLoadMode() != App.LoadMode.INCREMENT);

        // RT-FW ведёт в CTL статистики не только по продукту, но и по каждому набору — в его собственной сущности; при
        // первом запуске на RT-FW эти сущности он создаёт сам. Статистику 2 на продукт он публикует, только если хотя
        // бы один набор помечен изменённым, — поэтому проще всего пометить набор, как того требует RT-FW. Для
        // единообразия помечаем все наборы.
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
        return SparkSession.active().table(targetSchema + "." + targetTable);
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
                app.log(new LogMessage("", LogType.TECH_INFO,
                    format("СК для %s посчитаны локально (ССК отключён)", normalizedTable)));
                return local;
            }

            // Фиксируем вход, чтобы init/map читали одни и те же строки, а не пересчитывали трансформацию. Пишем как
            // все рабочие таблицы — способом RT-FW.
            Dataset<Row> frozen = writeWorkTable(STGTMP_SCHEMA, normalizedTable + "_mapped", mappedDfwc);

            // В INIT режиме метод init красит свой ключ, map — только чужие
            Dataset<Row> resDataSet;
            if (!frozen.isEmpty()
                    && app.ctx().getLoadMode() != App.LoadMode.INCREMENT
                    && attributeCodes.contains("@" + normalizedTable)) {
                Dataset<Row> painted = getSkClient().init(normalizedTable, frozen).get(0);
                List<String> fkAttributeCodes = attributeCodes.stream()
                    .filter(code -> !("@" + normalizedTable).equals(code)).collect(toList());
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
        } catch (SkClientException | SkCommonException e) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                "Провал интеграции с ССК: " + e.getMessage()), e);
        }
    }

    private SkClient getSkClient() {
        if (skClient == null) {
            skClient = SkClient.getInstance();
        }
        return skClient;
    }
}
