package ru.sberbank.bigdata.dffess.vspc.common;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.core.Transformation;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.sk.SkCommonException;
import ru.sberbank.bigdata.sl.sk.client.SkClient;
import ru.sberbank.bigdata.sl.sk.client.SkClientException;
import ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters;

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
    /**
     * Суффикс колонки с натуральным ключом в DataFrame трансформации ({@code <entity>_sid}).
     *
     * <p>Контракт RT-FW 3.16: натуральный ключ приходит в датасете под именем {@code *_sid},
     * а публикуется в целевой таблице под именем {@code *_sk} — тем же, что в DDL витрины
     * ({@code src/main/resources/sql/ddl/ddl.usl.vspc_idl.v021.model.9.47.0_fw_3.9.6.sql}).
     * Переименование выполняет {@link #addSk} перед передачей датасета издателю.
     */
    public static final String SOURCE_SK_SUFFIX = "_sid";
    /** Суффикс колонки с ключом ССК в целевой таблице ({@code <entity>_sk}) — см. {@link #SOURCE_SK_SUFFIX}. */
    public static final String TARGET_SK_SUFFIX = "_sk";
    protected App app;
    protected String code;
    protected Logger logger;
    private SkClient skClient;

    /**
     * Режимы ССК.
     *
     * <p>В 3.0 их было пять ({@code CommonConst.SkMode}, ESSA 3.0, {@code CommonConst.java:43-49}):
     * ON, OFF, DISABLE, ON_GF, OFF_TB. В 4.0 оставлены два — те, что достижимы через
     * конфигурацию RT-FW 3.16: {@link SkMode#ON} и {@link SkMode#OFF}.
     *
     * <p>Почему остальные исчезли: CTL-валидатор RT-FW 3.16 допускает для параметра
     * {@code debug_sk_off} только {@code true}/{@code false}/пусто
     * ({@code PossibleValuesValidator} с типом {@code TERNARY_FLAG_VALUES} в
     * {@code rt-fw-core-3.16.0.jar}, {@code ApplicationParameters.DEBUG_SK_OFF}), а ESSA 3.0
     * разбирала ещё {@code D}/{@code ON_GF}/{@code OFF_TB}
     * ({@code CtlAdapterImpl.configureSkMode}, ESSA 3.0, {@code CtlAdapterImpl.java:142-157}).
     * Значения {@code D}/{@code ON_GF}/{@code OFF_TB} ⇒ CTL 4.0 не примет конфигурацию ⇒ эти режимы
     * недостижимы и в коде 4.0 не нужны.
     *
     * <p>Что от этого НЕ потерялось:
     * <ul>
     *   <li>{@code sk_greenfield} (greenfield-режим) — в 4.0 отдельный CTL-параметр, читается
     *       независимо от режима; карточка ОС-8 реестра отклонений;</li>
     *   <li>{@code turbo_boost} — в 4.0 тоже отдельный CTL-параметр (пусто ⇒ {@code "true"},
     *       как в 3.0 вне {@code OFF_TB}).</li>
     * </ul>
     */
    private enum SkMode { ON, OFF }

    private final SkMode skMode;

    /**
     * Разбор значения параметра {@code debug_sk_off} в режим ССК — дословный перенос разбора 3.0
     * ({@code CtlAdapterImpl.configureSkMode}, ESSA 3.0, {@code CtlAdapterImpl.java:142-157})
     * для значений, которые принимает CTL RT-FW 3.16:
     * <ul>
     *   <li>{@code Y}/{@code true} → {@link SkMode#OFF} — сервисы ССК не вызываются, ключи считаются
     *       локально (в 3.0 ровно так же);</li>
     *   <li>всё остальное, включая пустое значение и {@code false} → {@link SkMode#ON} — сервисы ССК
     *       вызываются (в 3.0 ровно так же).</li>
     * </ul>
     * Пустое значение и {@code false} — это разные вещи, и так же в 3.0: пусто ⇒ ССК включены,
     * {@code true}/{@code Y} ⇒ выключены. Значения {@code D}/{@code ON_GF}/{@code OFF_TB} CTL 3.16
     * не принимает; если они всё же пришли — трактуются как {@link SkMode#ON} с предупреждением
     * в лог (терять данные хуже, чем считать ключи через сервис).
     */
    private SkMode resolveSkMode(String value) {
        if ("Y".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value)) {
            return SkMode.OFF;
        }
        if (value != null && !value.isEmpty()
                && !"false".equalsIgnoreCase(value)) {
            // Значение, которого нет в списке CTL-валидатора RT-FW 3.16 (D/ON_GF/OFF_TB и пр.).
            logger.log(new LogMessage("", LogType.TECH_WARN, format(
                    "Параметр %s=%s не поддерживается CTL RT-FW 3.16 (допустимы true/false/пусто) — "
                            + "сервисы ССК используются как при пустом значении",
                    ApplicationParameters.DEBUG_SK_OFF.getParamName(), value)));
        }
        return SkMode.ON;
    }

    protected BaseTransformation(App app, String code, String... sets) {
        super(app, code, sets);
        this.code = code;
        this.app = app;

        logger = new Logger(app, this.getClass());

        skMode = resolveSkMode(app.ctx().getProperty(ApplicationParameters.DEBUG_SK_OFF.getParamName(), ""));
        boolean skDisabledLocally = skMode == SkMode.OFF;
        if (skDisabledLocally) {
            logger.log(new LogMessage("", LogType.TECH_WARN, format(
                    "ССК отключён параметром %s=%s: ключи считаются локально. Режим только для отладки/стендов",
                    ApplicationParameters.DEBUG_SK_OFF.getParamName(), skMode)));
        }

        // System-property для клиента ССК 1.30.1 — состав и порядок как в ESSA 3.0
        // (SkClientWrapper.java:44-51): service_data_product_code — код продукта,
        // service_init_greenfield — признак стартовой загрузки реестра, turbo_boost — признак
        // ускоренного расчёта ключей, app_log_env — логическое окружение.
        // Два отличия 4.0 от 3.0 (только в передаче значения, набор свойств тот же):
        //   sk_greenfield — в 3.0 вне режима ON_GF всегда "false" (SkClientWrapper.java:45), в 4.0
        //                   вне ON_GF читается CTL-параметр sk_greenfield; расхождение зафиксировано
        //                   карточкой ОС-8 реестра отклонений (CLOSED_DEVIATIONS.md);
        //   turbo_boost   — в 3.0 вне режима OFF_TB всегда "true" (SkClientWrapper.java:46-48), в 4.0
        //                   вне OFF_TB читается CTL-параметр turbo_boost (пусто ⇒ "true").
        System.setProperty("service_data_product_code", app.ctx().getShortProductCode());
        // Режим ON_GF в 4.0 недостижим (CTL-валидатор 3.16 не принимает это значение), поэтому
        // признак стартовой загрузки реестра берётся ТОЛЬКО из CTL-параметра sk_greenfield —
        // как и до упразднения режима (вне ON_GF в 3.0 он всегда был "false"; расхождение
        // зафиксировано карточкой ОС-8 реестра отклонений).
        System.setProperty("service_init_greenfield", app.ctx().getProperty("sk_greenfield", "false"));
        if (!app.ctx().getLogEnv().isEmpty()) {
            System.setProperty("app_log_env", app.ctx().getLogEnv());
        }
        // Режим OFF_TB в 4.0 недостижим ⇒ turbo_boost всегда выставляется как в 3.0 вне OFF_TB:
        // пусто ⇒ "true", иначе значение параметра.
        String turboBoost = app.ctx().getProperty("turbo_boost", "");
        System.setProperty("turbo_boost", turboBoost.isEmpty() ? "true" : turboBoost);
        // ESSA 3.0 (SkClientWrapper.java:52-54): код экземпляра сервиса выводился из CTL-имени потока
        // (CtlAdapterImpl.configureInstanceCode — часть между "__"), приводился к нижнему регистру и
        // выставлялся в system-property ТОЛЬКО при непустом значении (StringUtils.isNotBlank).
        // 4.0: два канала, оба дают одно и то же значение в ключ реестра ССК:
        //   (1) явный CTL-параметр "service_instance_code" — приоритет (в CTL объявлен в обоих потоках);
        //   (2) app.ctx().getInstanceCode() — RT-FW 3.16 сам разбирает CTL-имя потока
        //       (rt-fw-coordinator 3.16.0, WorkflowNameParser.parseInstance: имя режется с "_INIT",
        //       затем split("__"); берётся вторая часть, но только если частей ровно две, иначе — пусто;
        //       результат приводится к нижнему регистру) — это тот же источник, что в 3.0.
        //       Разбор не идентичен 3.0: там бралась часть между первым и последним "__", здесь при
        //       нескольких "__" значение окажется пустым, а суффикс "_INIT" у 3.0 в код экземпляра входил.
        String serviceInstanceCode = app.ctx().getProperty("service_instance_code", "");
        String instanceCodeSource = "ctl-параметр";
        if (serviceInstanceCode == null || serviceInstanceCode.trim().isEmpty()) {
            serviceInstanceCode = app.ctx().getInstanceCode();
            instanceCodeSource = "имя потока CTL";
        }
        if (serviceInstanceCode != null && !serviceInstanceCode.trim().isEmpty()) {
            // значение входит в ключ реестра ССК, от него зависят выданные *_sk
            String effectiveInstanceCode = serviceInstanceCode.toLowerCase();
            System.setProperty("service_instance_code", effectiveInstanceCode);
            logger.logInfo(format("Код экземпляра ССК: %s (источник: %s)", effectiveInstanceCode, instanceCodeSource));
        } else {
            logger.logInfo("Код экземпляра ССК пуст — system-property service_instance_code не выставляется "
                    + "(ESSA 3.0: пустой код экземпляра, SkClientWrapper.java:52-54)");
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
            // vspc-4.0 perf (аудит №3): NOSCAN вместо полного COMPUTE STATISTICS.
            // Разница ровно в одном: NOSCAN берёт только табличные статистики (размер в байтах) по
            // листингу файлов, БЕЗ чтения данных; полный COMPUTE STATISTICS читает данные (для parquet —
            // футеры файлов, для hive-serde — проход по данным). Поколоночные min/max/NDV не считаются
            // ни в одном из режимов: для них нужен FOR ALL COLUMNS. Теряется ровно numRows ⇒
            // кардинальность соединений планировщик оценивает по размеру; выигрывается отсутствие
            // скана на каждой сохранённой таблице. Колоночная статистика здесь и не нужна: черновик и
            // stg.chg_div_h читаются обратно без фильтров (readFrozenDraft / readTable: select * ...).
            // ПАРИТЕТ С 3.0: в 3.0 ANALYZE не выполнялся вовсе — 0 совпадений по всему дереву витрины 3.0
            // и 0 совпадений в исходниках ESSA 3.0 (essa-common-lib). NOSCAN — выбор 4.0, а не паритет с 3.0.
            // Издатель поступает так же для служебных таблиц: PublisherDao.java:274 — NOSCAN,
            // PublisherDao.java:609 — "ANALYZE TABLE COMPUTE STATISTICS не требуется, т.к. в целевой
            // EXTERNAL-таблице не добавлялись партиции"; по ЦЕЛЕВЫМ EXTERNAL-таблицам издатель, наоборот,
            // считает ПОЛНЫЕ статистики (PublisherDao.java:789 и :811 — "ANALYZE TABLE ... COMPUTE STATISTICS").
            app.getSparkSession().sql(format("ANALYZE TABLE %s.%s COMPUTE STATISTICS NOSCAN", targetSchema, targetTable));
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
            Dataset<Row> frozen = mappedDfwc;
            // Режим DISABLE в 3.0 (SkClientWrapper.generateSk:97-98 — датасет возвращался КАК ЕСТЬ,
            // без нормализации ключа в строку) в 4.0 недостижим: CTL-валидатор RT-FW 3.16 не принимает
            // значение "D" параметра debug_sk_off ⇒ отдельной ветки для него не нужно.
            // Для режимов с обращением к реестру ССК (ON) — как в 3.0: натуральный ключ уходит
            // в ССК КАК ЕСТЬ — ни приведения к строке, ни срезания хвоста ".0" обёртка 3.0 не делает, клиент
            // ССК 1.30.1 значения тоже не нормализует (в нём replace* относятся к именам колонок).
            // Нормализация "\.0+$" (BaseTransformation, версия 4.0 до правки) отсутствует в 3.0 ⇒ снята.

            if (skMode == SkMode.OFF) {
                // ESSA 3.0 (SkClientWrapper.generateSk:99-100 → localSK:215-227): OFF — локальная подмена
                // xxhash64(натуральный ключ), ССК не вызывается вовсе.
                Dataset<Row> local = frozen;
                for (String cn : skColumns) {
                    String skCol = cn.replace(SOURCE_SK_SUFFIX, TARGET_SK_SUFFIX);
                    local = local.withColumn(skCol, xxhash64(col(cn)).cast(DataTypes.LongType)).drop(cn);
                }
                logger.logInfo(format("СК для %s посчитаны локально (ССК отключён: %s)", normalizedTable, skMode));
                return local;
            }

            // Имя черновика — постоянное, по имени сущности. На каждую сущность создаётся ровно
            // один экземпляр трансформации (MainClass.java:35-43), а трансформации идут параллельно,
            // но каждая пишет в свою таблицу — поэтому хэш объекта в имени не нужен.
            // Раньше было "<table>_mapped_<hashCode()>" ⇒ на каждый запуск появлялась новая таблица
            // в STGTMP (8 таблиц за прогон, накапливались на ПРОМе). В 3.0 имя тоже было постоянным
            // ("tmp_" + имя stg-таблицы, SnpDataLoader.java:148-149) — черновик перезаписывался.
            String draftTable = normalizedTable + "_mapped";
            frozen = readFrozenDraft(draftTable, frozen);

            // ESSA 3.0 (SkClientWrapper.initSk:128-149): на INIT владелец сущности сначала зовёт init — он
            // заводит натуральные ключи в реестре ССК и САМ красит собственный ключ <entity>_sk (join-back по
            // <entity>_sid), — а map затем красит только ссылки на чужие сущности (код своей сущности из
            // списка атрибутов снят). В 4.0 ответ init отбрасывался, а свой ключ красил map по полному
            // списку атрибутов: реестр тот же, но канал доставки номера другой — и для пустого собственного
            // натурала init даёт NULL, а map уводит пустоту на сервер как обычный ключ.
            // На INCR 3.0 шёл в map по полному списку атрибутов (SkClientWrapper.incrSk:151-167) — так же
            // и в 4.0 в ветке else.
            boolean ownEntity = attributeCodes.contains("@" + normalizedTable);
            Dataset<Row> resDataSet;
            if (!frozen.isEmpty()
                    && app.ctx().getLoadMode() != App.LoadMode.INCREMENT
                    && ownEntity) {
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

    /**
     * ESSA 3.0 (SkClientWrapper.initSk): init получает полный черновик кадра и сам красит собственный ключ
     * сущности (&lt;entity&gt;_sk) значением из реестра ССК; возвращённый кадр идёт дальше в map.
     */
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
            // Пустой датасет: к реестру ССК не ходим — строк нет, а имена колонок-ключей всё равно должны
            // совпадать с непустым случаем, поэтому *_sid переименовываются в *_sk. Типы ключей приводит
            // к LongType addSk — уже после возврата отсюда.
            logger.logInfo("EmptyTable - " + tableName);
            for (String cn : getColumnsFromAttributes(attributeCodes)) {
                df = df.withColumnRenamed(cn, cn.substring(0, cn.length() - SOURCE_SK_SUFFIX.length()) + TARGET_SK_SUFFIX);
            }
            return df;
        }
        return getSkClient().map(df, mapAttributeCodes)
                .drop(getColumnsFromAttributes(attributeCodes).toArray(new String[0]));
    }

    /**
     * Имена колонок с натуральными ключами по списку атрибутов.
     *
     * <p>Формы кодов атрибутов (см. {@code AddrTransformation.java:246-248}) и получаемые имена колонок:
     * {@code "@addr"} (свой атрибут) → {@code addr_sid}; {@code "@rus_fed_subj"} (ссылка на чужую
     * сущность) → {@code rus_fed_subj_sid}; {@code "fias_code@addr"} (атрибут своей сущности)
     * → {@code fias_code_addr_sid}.
     */
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
