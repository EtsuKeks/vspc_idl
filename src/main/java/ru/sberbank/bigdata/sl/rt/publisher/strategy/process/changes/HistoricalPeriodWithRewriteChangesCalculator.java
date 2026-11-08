package ru.sberbank.bigdata.sl.rt.publisher.strategy.process.changes;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import ru.sberbank.bigdata.dffess.vspc.common.VspcLoadDate;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Partition;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SectionBy;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SnapshotChanges;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfoFactory;
import ru.sberbank.bigdata.sl.rt.util.RtfwLogger;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.when;
// vspc-3.0 parity (D2/VANISH): формулы закрытия периодов как в ESSA 3.0 —
// обоснование: ~/Desktop/ess/temp/audit_default_publisher_fixmap.md (F2/F3/F4)
import static org.apache.spark.sql.functions.date_sub;
// vspc-3.0 parity (VANISH, FX2.6): 3.0 закрывает исчезнувший ключ на current_date()-1 в TZ сессии Spark
// (DataLoaderHistSnp.java:106) — берём тот же якорь, а не JVM-дату загрузки.
import static org.apache.spark.sql.functions.current_date;
// vspc-3.0 parity (EQUALS-retention/старт-даты): xxhash64 — канон SnpDataLoader.getStringHash
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.xxhash64;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION_DELETE;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION_INSERT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION_UPDATE;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO_REDO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO_UNDO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.MINUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.metadata.domain.Method.BUSINESS_HISTORY;
import static ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName.SNP;
import static ru.sberbank.bigdata.sl.rt.publisher.logger.PublisherLogMessageBuilder.PUB_114;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.INCR_ALIAS;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.INCR_PREFIX;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.INCR_PREFIX_ALL;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.INNER;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.LEFT;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.OLD_ALIAS;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.OLD_PREFIX;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.OLD_PREFIX_ALL;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.TECH_FIELDS;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.castDataset;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.getColumnTimeUnit;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.getStructFieldByName;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.joinByArray;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.scalaSeq;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.sectionBy;

/**
 * Shadow-копия vendor-класса rt-fw-publisher-2.16.1 (тот же пакет; замещение в fatjar —
 * shade-exclude в pom.xml, механика та же, что у PublicationStage/ProcessingStage/
 * PreparationStage/NonHistoricalChangesCalculator). Из пяти shadow-классов переписан сильнее всех:
 * считает изменения для HIST-таблиц с перезаписью версий (div_type, addr_h, div_h, div_div_relation).
 *
 * <p>Отличия от vendor (все — паритет содержания с ESSA 3.0; каждое помечено в теле метода):
 * <ul>
 *   <li>{@code getSnpGeqFilters} — порог зоны перезаписи = 1-е число month(D−1) (окно 3.0,
 *       HistMovingGenerator.java:31) вместо vendor'овского startingBusinessDate−1
 *       (AbstractHistoricalChangesCalculator.java:85-87);</li>
 *   <li>{@code getMinBusinessDateUpdateFilter} — возвращает lit(true): снап читается целиком,
 *       как в 3.0 (vendor фильтрует oldSnp по бизнес-дате на чтении);</li>
 *   <li>{@code calculateImpl} — правки закрытия версий: якорь даты загрузки VspcLoadDate (:124)
 *       вместо current_date(); закрытие исчезнувших ключей (VANISH) для div_type; правило
 *       возвращенца start_dt = max(closed_end)+1 для div_type (кеш закрытых версий есть только
 *       у div_type — DataLoaderHistSnp.java:79-116);</li>
 *   <li>{@code calculateDiff2Datasets} — переопределение: хэш xxhash64(concat(...)) БЕЗ разделителя,
 *       канон 3.0 (SnpDataLoader.java:116-133), вместо vendor md5(concat_ws("|", …));</li>
 *   <li>удалена мёртвая ветка схлопывания периодов (collapsePeriods всегда false — :159-162);</li>
 *   <li>маркер SILENT_DROP_COL (:134) — версии снапа, которые 3.0 отбрасывает молча
 *       (DataLoaderHistSnp.java:117-122); вычищается в shadow-ProcessingStage (calculateDiff).</li>
 * </ul>
 *
 * <p>Остальные тела методов — байт-в-байт от vendor
 * (vendor/rt-fw-publisher-2.16.1-src/ru/sberbank/bigdata/sl/rt/publisher/strategy/process/changes/HistoricalPeriodWithRewriteChangesCalculator.java).
 */
public class HistoricalPeriodWithRewriteChangesCalculator extends AbstractHistoricalWithRewriteChangesCalculator {
    private final String secondDateColumn;
    private final String sectioningDMin;
    private final String sectioningDMax;

    // vspc-3.0 parity (D2/VANISH): анкер даты загрузки — ОДНО значение на прогон, посчитанное на драйвере
    // при первом обращении к VspcLoadDate (статика). В 3.0 это был wall-clock старта приложения
    // (VspcConstant:19-21, getCtlValidFromStr() = LocalDateTime.now(), JVM-TZ). Spark-функция current_date()
    // вместо константы пере-вычисляется на каждой задаче (RuntimeReplaceable: Instant.now() +
    // spark.sql.session.timeZone), поэтому на переходе через полночь и при рассинхроне session TZ vs JVM
    // end_dt становился неоднородным внутри одной загрузки (сдвиг до ±1 дня, вплоть до перекрытия версий).
    // Тот же анкер используют трансформеры для start_dt (DivH/AddrH/DivType/DivDivRelation) — одна дата
    // на обе половины стыка версий.
    // Обоснование: ~/Desktop/ess/temp/audit_v2/14_d2_anchor.md, приоритет 2; фикс F8.
    // ИСКЛЮЧЕНИЕ (FX2.6): VANISH-закрытие исчезнувшего ключа в 3.0 — current_date()-1 в TZ сессии Spark
    // (DataLoaderHistSnp.java:106), поэтому там якорь другой (см. ветку CLOSE_VANISHED_TABLE ниже);
    // D2-якорь закрытия версии остаётся на loadDate-2.
    private final Date loadDate = VspcLoadDate.sqlDate();

    private static final String HASH_COL_NAME = "tech_cv_md5";
    private static final String TMP_UPD_FLG = "_tmp_upd_flg";
    private static final String TMP_UPD_PREFIX = "_tmp_upd_";

    // vspc-3.0 parity (В-4, out-of-order): маркер версии снапа, которую 3.0 отбрасывает МОЛЧА.
    // Публичный: колонка-маркер снимается в shadow-ProcessingStage (calculateDiff/readChangesFromStg) —
    // он же вычищает такие ноги из diff-таблицы (в 3.0 их там нет) и возвращает версию в набор
    // удалений для merge (из снапа версия обязана исчезнуть).
    public static final String SILENT_DROP_COL = "_vspc_silent_drop";

    // vspc-3.0 parity (EQUALS-retention/старт-даты, arbiter_verdict.md В1/В2/В3): служебные колонки,
    // живут только внутри calculateImpl и всегда дропаются до выхода (см. блок !fullSlice ниже)
    private static final String RETAIN_HASH_COL = "_vt_retain_hash";
    private static final String RETAIN_HASH_SRC_COL = "_vt_retain_hash_snp";
    private static final String RETAIN_FLG_COL = "_vt_retain_flg";
    private static final String LAST_CLOSED_END_COL = "_vt_last_closed_end";
    private static final String OPEN_FLG_COL = "_vt_open_flg";

    // vspc-3.0 parity (VANISH): в 3.0 закрытие ключей, исчезнувших из источника (end_dt = дата загрузки − 1),
    // выполнял только DataLoaderHistSnp (closeOld=true) и только для div_type; HistDiff/HistMix держали
    // OLD-записи открытыми. Штатный RT-FW INCR теряет vanish-закрытия: старый снап отсечён INNER-join'ом
    // с min-датами инкремента. Гейт по имени таблицы (EntityInfo.tableName == tableCode метаданных,
    // для div.sat=type это "div_type") — расширяется одной константой.
    // Обоснование: ~/Desktop/ess/temp/audit_default_publisher_fixmap.md, histframeworkbase_mechanics.md
    private static final String CLOSE_VANISHED_TABLE = "div_type";

    // vspc-3.0 parity (A2-D2-03): «правило возвращенца» (start_dt = max(closed_end)+1) в 3.0 есть только
    // у div_type — там ведётся кеш закрытых версий (DataLoaderHistSnp.java:79-116, LoaderCache:
    // cache.dump(toCache) / cache.load(cachePrefix)); старт возвращенца = cached_end+1
    // (DataLoaderHistSnp.java:93-96). Для addr_h/div_h/div_div_relation кеша нет (HistDiff/HistMix/
    // LinkHistDiff), и там ключ без открытой версии получает MIN_DATE (1900-01-01).
    private static final String RETURNER_START_DATE_TABLE = "div_type";

    // Параметр collapsePeriods оставлен в сигнатуре: он входит в бинарный контракт с vendor-классом
    // CalculatorFactory.createChangesCalculator (vendor/.../strategy/CalculatorFactory.java:57-65),
    // вызываемым из неизменяемого jar rt-fw-publisher-2.16.1. В дереве 4.0 значение всегда false
    // (PublisherImpl.java:103,150-151) ⇒ ветка схлопывания удалена как мёртвая (см. calculateImpl).
    public HistoricalPeriodWithRewriteChangesCalculator(PublisherDao dao,
                                                        List<String> pkName,
                                                        boolean fullSlice,
                                                        boolean reInit,
                                                        boolean collapsePeriods,
                                                        int loadingId,
                                                        TableInfoFactory tableInfoFactory,
                                                        String startDtColumn,
                                                        String endDtColumn,
                                                        String startingBusinessDate,
                                                        Map<String, Partition> derivedPartitioningFields,
                                                        String sectioningDMin,
                                                        String sectioningDMax) {
        super(dao, pkName, fullSlice, reInit, loadingId, tableInfoFactory,  startDtColumn, startingBusinessDate, derivedPartitioningFields);
        this.secondDateColumn = endDtColumn;
        this.sectioningDMin = sectioningDMin;
        this.sectioningDMax = sectioningDMax;
    }

    @Override
    public Map<String, String> getMinBusinessDateUpdateFilters() {
        return getSnpGeqFilters(firstDateColumn, secondDateColumn);
    }

    // vspc-3.0 parity (FX9.1): порог зоны перезаписи = month(D-1) — независимо от max_increment_depth_days
    // (MIDD) и дня месяца.
    //
    // Эталон 3.0: HistMovingGenerator.java:31 — cutPoint = LocalDate.now().minusMonths(1) (формат yyyyMM),
    //   условие AFTER_OR_EQUALS_CUT_POINT ⇒ перезаписываются партиции p1month ∈ {month(D-1), month(D)}
    //   (плюс modifiable) ⇒ ровно 2 месяца.
    // Штатный RT-FW: порог зоны = startingBusinessDate - 1 день, где
    //   startingBusinessDate = max(CTL starting_business_date, today - MIDD) (PublisherImpl.java:573-582),
    //   а сам порог выводится из startingBusinessDate в AbstractHistoricalChangesCalculator.java:85-87.
    //   При MIDD=40 порог = month(today-41); при D-41 < 1-е число month(D-1) это month(D-2)
    //   ⇒ зона перезаписи {month(D-2), month(D-1), month(D)} — лишняя третья партиция.
    // Здесь порог считается напрямую от D: LocalDate.now().minusMonths(1).withDayOfMonth(1).
    //
    // Ветвление по firstDateColumn == secondDateColumn не воспроизводится: у всех HIST-таблиц витрины
    // start_dt != end_dt (secondDateColumn = end_dt), а порог BUSINESS_HISTORY-секции берётся по end_dt.
    //
    // REINIT-путь не затрагивается: calculateImpl (:276-283) режет срез по полю startingBusinessDate,
    //   приходящему из CTL-параметра init_diff_depth_days (PublisherImpl.java:266-269) — это поле
    //   здесь не меняется. Карта порогов в REINIT не используется для выбора перезаписываемых партиций:
    //   clearTargetByMovingToBackup() в REINIT не вызывается (PublisherImpl.runReinit не вызывает его),
    //   а moveToTargetTable() для snp использует PublicationStage, созданный ДО calculateDiff
    //   (PublisherImpl.java:642-644), т.е. с пустой картой порогов.
    @Override
    protected Map<String, String> getSnpGeqFilters(String firstDateColumn, String secondDateColumn) {
        if (startingBusinessDate == null || startingBusinessDate.isEmpty() ||
                startingBusinessDate.equals(MINUS_INFINITY_DT) || (
                !Objects.equals(firstDateColumn, secondDateColumn) &&
                derivedPartitioningFields.entrySet().stream()
                        .anyMatch(e -> e.getValue().getMethod() == BUSINESS_HISTORY &&
                                e.getValue().getOriginColumnCode().equals(firstDateColumn)))
        ) {
            return Collections.emptyMap(); // нет фильтра на бизнес-историю
        }

        // 3.0: месячное окно перезаписи — текущий и предыдущий месяц ⇒ порог = 1-е число month(D-1)
        String minChangedBusinessDate = LocalDate.now().minusMonths(1).withDayOfMonth(1).toString();

        Map<String, String> filters = new HashMap<>();
        for (Map.Entry<String, Partition> row : derivedPartitioningFields.entrySet()) {
            Partition partition = row.getValue();
            if (partition.getMethod() == BUSINESS_HISTORY) {
                String originalColName = partition.getOriginColumnCode();
                if (Objects.equals(originalColName, secondDateColumn)) {
                    StructType schema = new StructType().add(originalColName + "_str", DataTypes.StringType, false);
                    Dataset<Row> startingBusinessDateDataset = dao.createDataFrame(
                                    Collections.singletonList(RowFactory.create(minChangedBusinessDate)), schema)
                            .withColumn(originalColName, col(originalColName + "_str").cast(DataTypes.DateType));
                    Dataset<Row> dataset = SectionBy.valueOf(partition.getUnit().name())
                            .compute(new SectionBy.SectionData(startingBusinessDateDataset, row.getKey(), partition));
                    filters.put(row.getKey(), dataset.first().getAs(row.getKey()));
                }
            }
        }

        return filters;
    }

    // vspc-3.0 parity (окно чтения снапа): в 3.0 все HIST-лоадеры (DataLoaderHistSnp/HistDiff/HistMix,
    // DataLoaderLinkHistDiff) читали целевой снап ЦЕЛИКОМ через getDataset(tgtSchemaSnp, tgtTable) —
    // никакого отсечения по бизнес-дате на чтении не было; окно 40 дней работало только в коде витрины
    // (VspcConstant.DEPTH_DELTA) и в выборе переписываемых партиций.
    // Штатный паблишер 3.16.1 применяет фильтр к oldSnp на чтении (vendor/rt-fw-publisher-2.16.1-src/…/ProcessingStage.java:128), из-за
    // чего: (1) ключ, вернувшийся после паузы длиннее max_increment_depth_days+1 день, не видел своих
    // закрытых версий и получал start_dt = 1900 вместо cached_end+1; (2) исчезнувшие ключи старше окна
    // вообще не попадали в oldSnp и не закрывались. Возвращаем чтение целиком.
    // Карта getMinBusinessDateUpdateFilters() (та же формула) остаётся нетронутой: её использует
    // PublicationStage.clearTargetByMovingToBackup (vendor/rt-fw-publisher-2.16.1-src/…/PublicationStage.java:87;
    // в shadow-копии — :108-126, использование карты :124) для выбора переписываемых партиций — это и есть
    // аналог месячного окна 3.0 (p1month(current) ∪ p1month(current-1) ∪ modifiable).
    @Override
    public Column getMinBusinessDateUpdateFilter() {
        RtfwLogger.log(new LogMessage("", LogType.TECH_INFO,
                format("Снап %s читается целиком (parity с ESSA 3.0): фильтр по бизнес-дате к oldSnp не применяется",
                        tableInfoFactory.createTableInfo(SNP, "").getQualifiedTableName())));
        return lit(true);
    }

    @Override
    protected SnapshotChanges calculateImpl(Dataset<Row> inc, Dataset<Row> snp, List<Map<String, String>> partitionSpecs) {
        // Валидация типа колонки даты: getColumnTimeUnit бросает TransformationException на неподдерживаемом
        // типе (PublisherUtils.java:312-323). Значение timeUnit использовалось только в удалённой ветке
        // collapsePeriods (timeUnitSub(...)) ⇒ сам вызов оставлен как проверка, результат не используется.
        getColumnTimeUnit(inc, firstDateColumn);
        final StructType tgtSchema = snp.schema();

        // Исключаем из инкремента записи, влияющие на бизнес-историю глубже настроенной минимальной
        // реализация требования publisher−08.3
        StructField firstDateStruct = getStructFieldByName(inc,firstDateColumn).orElseThrow(() ->
                new TransformationException(PUB_114(format("Поле '%s' не найдено в датасете", firstDateColumn))));
        Column minDateCol = lit(startingBusinessDate).cast(firstDateStruct.dataType());

        if (fullSlice && reInit) {
            Dataset<Row> incrSlice = (startingBusinessDate == null || startingBusinessDate.isEmpty()) ? inc :
                    inc.filter(col(firstDateColumn).$greater$eq(minDateCol));
            Dataset<Row> targetSlice = (startingBusinessDate == null || startingBusinessDate.isEmpty()) ? snp :
                    snp.filter(col(firstDateColumn).$greater$eq(minDateCol));
            return calculateReinitDiff(incrSlice, targetSlice, partitionSpecs);
        }

        // Если удаления физические, то работаем как с логическими
        if (!Arrays.asList(tgtSchema.fieldNames()).contains(DELETED_FLAG)) {
            inc = inc.withColumn(DELETED_FLAG, lit(false).cast(BooleanType));
            snp = snp.withColumn(DELETED_FLAG, lit(false).cast(BooleanType));
        }

        // vspc-3.0 parity (FX2.5): фильтра дельты по MIDD в 3.0 НЕТ — инкремент обрабатывается целиком
        // (DataLoaderHistMix.java:31-94: ни фильтра по end_dt, ни клампа start_dt; 40-дневное окно 3.0
        // относилось к чтению снапа и переписываемым партициям — getIncrCutOffSrcDate()/DEPTH_DELTA).
        // В 4.0 фильтр был no-op для addr_h/div_type/ddr (end_dt инкремента = 9999-12-31), а для div_h
        // (end_dt приходит из источника — DivHTransformation.java:47-48) в INIT отсекал версии цепочки
        // с end_dt < MIDD и клампом start_dt схлопывал оставшиеся в MIDD — дубли PK [sk,start_dt] против
        // 3.0, где INIT грузит цепочку как есть (DataLoaderHistMix.java:95-117, start_dt источника).
        // Фильтр не был несущим: MIN_DT нужен только crossing-логике (TMP_UPD_FLG/TMP_UPD_PREFIX+end_dt)
        // и part-1-таргету (start_dt >= MIN_DT); в INCR эти величины равны loadDate-1 и без фильтра.
        Dataset<Row> incrMinDt = inc;

        // vspc-3.0 parity: список ключей источника ДО EQUALS-retention — используется VANISH-веткой ниже
        // (retained-ключи физически присутствуют в источнике и исчезнувшими не считаются, иначе были бы
        // ложно закрыты vanish-логикой div_type)
        Dataset<Row> incrMinDtPreRetention = incrMinDt;

        // ═══ vspc-3.0 parity: EQUALS-retention + старт-даты новичков/возвратцев (arbiter_verdict.md В1/В2/В3) ═══
        // Паритет ESSA 3.0 (SnpDataLoader.unionSnpAndDiff + DataLoaderHistSnp.java:82-96,
        // HistDiff/HistMix/LinkHistDiff `.or(EQUALS ∧ rn=N2)`):
        //  1) EQUALS-retention: строка, чей бизнес-хэш равен хэшу ОТКРЫТОЙ (end_dt=+∞) версии её ключа,
        //     покидает инкремент — ключ не закрывается, не переоткрывается, diff-ног не даёт (FLG=0-эквивалент:
        //     ключ просто выпадает из changes). В 3.0 это статус EQUALS: активной оставалась СТАРАЯ версия.
        //  2) старт-даты вставок: новичок (в окне снапа нет ни одной версии ключа) → 1900-01-01; возвратец
        //     (есть только закрытые) → max(closed_end)+1; изменённый (есть открытая) — прежний штамп today-1.
        // Хэш — 3.0-канон: xxhash64(concat(бизнес-колонки, NULL→'', БЕЗ разделителя)) —
        // SnpDataLoader.getStringHash/getHashColumn; набор колонок = динамически: incr-колонки ∩ snp-колонки
        // минус bk минус start_dt/end_dt минус TECH_FIELDS минус производные поля секционирования.
        // MERGE-механика проверена по x_pub_2161_src (GeneralMergeCalculator.buildMergedSnpCommonImpl):
        // снап собирается ПОСТРОЧНО по PK [<sk>, start_dt] — замена только версий, совпавших с redo-ногами,
        // вставки union'ятся, остальные строки проходят как snpPartWithoutUpdates ⇒ простое исключение
        // неизменённых ключей из инкремента НЕ стирает их строки снапа; вариант «no-op-redo» не потребовался.
        // Граница честности: опора — окно чтения снапа (открытые строки в окне всегда: p1month='999912';
        // закрытые — только свежие: отсутствие дольше окна → возвратец ведёт себя как новичок, 3.0-близко).
        if (!fullSlice) {
            // финальные ссылки для лямбд (snp/incrMinDt переназначаются выше по методу)
            final Dataset<Row> vtSnpBase = snp;
            final Dataset<Row> vtIncrBase = incrMinDt;
            Dataset<Row> vtSnpOpen = vtSnpBase.filter(col(secondDateColumn)
                    .equalTo(lit(PLUS_INFINITY_DT).cast(firstDateStruct.dataType())));
            Column[] vtBkCols = bkName.stream().map(functions::col).toArray(Column[]::new);

            String[] vtRetainAttr = Arrays.stream(vtIncrBase.columns())
                    .filter(c -> !bkName.contains(c) && !c.equals(firstDateColumn) && !c.equals(secondDateColumn)
                            && !TECH_FIELDS.contains(c) && !derivedPartitioningFields.containsKey(c)
                            && Arrays.asList(vtSnpBase.columns()).contains(c))
                    .toArray(String[]::new);
            if (vtRetainAttr.length > 0) {
                Column vtIncrHash = xxhash64(functions.concat(Arrays.stream(vtRetainAttr)
                        .map(attr -> when(col(attr).isNull(), lit("")).otherwise(col(attr).cast(DataTypes.StringType)))
                        .toArray(Column[]::new)));
                Column vtSnpOpenHash = xxhash64(functions.concat(Arrays.stream(vtRetainAttr)
                        .map(attr -> when(col(attr).isNull(), lit("")).otherwise(col(attr).cast(DataTypes.StringType)))
                        .toArray(Column[]::new)));
                Column[] vtMarkCols = Stream.concat(
                        bkName.stream().map(c -> col(INCR_PREFIX + c).as(c)),
                        Stream.of(col(INCR_PREFIX + RETAIN_HASH_COL).as(RETAIN_HASH_SRC_COL)))
                        .toArray(Column[]::new);
                Dataset<Row> vtRetainedMarks = incrMinDt
                        .withColumn(RETAIN_HASH_COL, vtIncrHash).as(INCR_ALIAS)
                        .join(vtSnpOpen.withColumn(RETAIN_HASH_COL, vtSnpOpenHash).as(OLD_ALIAS),
                                joinByArray(INCR_ALIAS, OLD_ALIAS, bkName)
                                        .and(col(INCR_PREFIX + RETAIN_HASH_COL)
                                                .equalTo(col(OLD_PREFIX + RETAIN_HASH_COL))),
                                INNER)
                        .select(vtMarkCols)
                        .withColumn(RETAIN_FLG_COL, lit(1))
                        .distinct();
                // построчный отсев: у ключа может быть несколько входных строк — убираем только те,
                // чей хэш совпал с открытой версией; прочие строки этого же ключа (реальные изменения) остаются
                incrMinDt = incrMinDt
                        .withColumn(RETAIN_HASH_COL, vtIncrHash)
                        .join(vtRetainedMarks, scalaSeq(bkName), LEFT)
                        .filter(col(RETAIN_FLG_COL).isNull()
                                .or(col(RETAIN_HASH_COL).notEqual(col(RETAIN_HASH_SRC_COL))))
                        .drop(RETAIN_HASH_COL, RETAIN_HASH_SRC_COL, RETAIN_FLG_COL);
            }

            // --- старт-даты вставок: новичок → 1900-01-01, возвратец → max(closed_end)+1,
            //     изменённый ключ (есть открытая версия) — прежний штамп today-1 без изменений ---
            // Канон 3.0: DataLoaderHistSnp.java:93-96 — when(IS_CACHED, cached_end+1).when(NEW, MIN_DATE).
            // Даты ставим ДО агрегата incSnpStgMinDt — MIN_DT и все ноги считаются уже от исправленных дат.
            Dataset<Row> vtOpenKeys = vtSnpOpen.select(vtBkCols).distinct();
            // vspc-3.0 parity (A2-D2-03): правило стартовой даты «возвращенца» (start_dt = max(closed_end)+1)
            // в 3.0 действует ТОЛЬКО для div_type: там ведётся кеш закрытых версий, и вернувшийся ключ
            // получает start_dt = cached_end+1 (DataLoaderHistSnp.java:93-96 — IS_CACHED ⇒ date_add(cachedEndDt, N1)).
            // Для addr_h/div_h/div_div_relation кеша закрытых версий в 3.0 нет: старт задаётся только
            // новому ключу (DataLoaderHistDiff.java:83, DataLoaderHistMix.java:80,
            // DataLoaderLinkHistDiff.java:85 — when(NEW_CONDITION, MIN_DATE)).
            // NEW_CONDITION в ESSA 3.0 = «у ключа НЕТ ОТКРЫТОЙ версии в цели» — проверено по исходникам
            // (/Users/22304126/Desktop/ess/essa/sources/essa_common, essa-common-lib 2.2.6; байткод
            // essa-common-lib-2.2.6-SNAPSHOT.jar сверен):
            //   - NEW_CONDITION = col("record_status") = "NEW" — SnpDataLoader.java:34;
            //   - статус "NEW" получают строки инкремента, "OLD" — строки снапа — SnpDataLoader.java:161,166;
            //     статус переопределяется на EQUALS/NOTEQUALS, если у ключа есть строка в OLD-стороне
            //     merge (сравнение хэшей) — SnpDataLoader.java:182-190,202-204;
            //   - OLD-сторона merge = ТОЛЬКО открытые версии (end_dt = MAX_DATE ⇒ партиция "999912"):
            //     DataLoaderHistMix.java:66, DataLoaderLinkHistDiff.java:71, DataLoaderHistDiff.java:69;
            //     колонка партиционирования — END_DT (VspcMetadataKeeper.java:19,21,25);
            //   ⇒ ключ с закрытыми версиями, но БЕЗ открытой, в OLD-сторону merge не попадает ⇒ остаётся
            //     "NEW" ⇒ start_dt = MIN_DATE = 1900-01-01 (CommonConst.java:17) — так же, как в 4.0.
            // Поэтому «правило возвращенца» применяем только к div_type; для прочих таблиц ключ без
            // открытой версии получает 1900-01-01 (как новый).
            boolean returnerStartDateRule = RETURNER_START_DATE_TABLE.equals(
                    tableInfoFactory.createTableInfo(SNP, "").getTableName());
            // vspc-4.0 perf (аудит №3, P1-Н5): LAST_CLOSED_END_COL читается ТОЛЬКО в ветке
            // when(noOpenVersion.and(hasClosedVersion).and(lit(returnerStartDateRule)), date_add(..., 1)).
            // Для addr_h/div_h/div_div_relation returnerStartDateRule = false ⇒ эта ветка недостижима,
            // и значение колонки больше нигде не используется. Значит для этих трёх таблиц мёртвая работа:
            // скан ВСЕЙ закрытой истории + groupBy-шаркл по bkName + join по bkName.
            // Считаем агрегат только когда он реально нужен — для div_type.
            Column noOpenVersion = col(OPEN_FLG_COL).isNull();
            if (returnerStartDateRule) {
                Dataset<Row> vtLastClosed = snp.filter(col(secondDateColumn)
                                .notEqual(lit(PLUS_INFINITY_DT).cast(firstDateStruct.dataType())))
                        .groupBy(vtBkCols)
                        .agg(max(col(secondDateColumn)).as(LAST_CLOSED_END_COL));
                Column hasClosedVersion = col(LAST_CLOSED_END_COL).isNotNull();
                incrMinDt = incrMinDt
                        .join(vtOpenKeys.withColumn(OPEN_FLG_COL, lit(1)), scalaSeq(bkName), LEFT)
                        .join(vtLastClosed, scalaSeq(bkName), LEFT)
                        .withColumn(firstDateColumn,
                                when(noOpenVersion.and(hasClosedVersion),
                                        date_add(col(LAST_CLOSED_END_COL), 1))
                                        .when(noOpenVersion,
                                                lit(MINUS_INFINITY_DT).cast(firstDateStruct.dataType()))
                                        .otherwise(col(firstDateColumn)))
                        .drop(OPEN_FLG_COL, LAST_CLOSED_END_COL);
            } else {
                // Для прочих таблиц «кеша закрытых версий» в 3.0 нет — ветка с date_add(closed_end, 1)
                // в 3.0 недостижима, и в 4.0 она тоже не применяется. Старт без открытой версии —
                // как новый (1900-01-01), ровно как в 3.0.
                incrMinDt = incrMinDt
                        .join(vtOpenKeys.withColumn(OPEN_FLG_COL, lit(1)), scalaSeq(bkName), LEFT)
                        .withColumn(firstDateColumn,
                                when(noOpenVersion,
                                        lit(MINUS_INFINITY_DT).cast(firstDateStruct.dataType()))
                                        .otherwise(col(firstDateColumn)))
                        .drop(OPEN_FLG_COL);
            }
        }

        // Подтягиваем к снепшоту минимальные даты инкремента в рамках экземпляра снепшота, пересекающиеся в рамках экземпляра с минимальными датами инкремента
        Dataset<Row> incSnpStgMinDt = incrMinDt
                .groupBy(bkName.stream().map(functions::col).toArray(Column[]::new))
                .agg(min(col(firstDateColumn)).as(MIN_DT))
                .as(INCR_ALIAS);
        incSnpStgMinDt = landStgTmpDataset(incSnpStgMinDt, partitionSpecs, "_incr_min_dt");

        Dataset<Row> oldSnpMinDt = snp.as(OLD_ALIAS)
                .join(incSnpStgMinDt.as(INCR_ALIAS), joinByArray(INCR_ALIAS, OLD_ALIAS, bkName), INNER);

        // Если передан актуальный снимок, то вычисляем и добавляем к инкременту логические удаления
        if (fullSlice) {
            Column sliceDate = lit(calculateSliceDate(inc)).cast(firstDateStruct.dataType());
            Dataset<Row> sliceLogicalDeletes = snp.as(OLD_ALIAS)
                    .filter(col(OLD_PREFIX + firstDateColumn).$less$eq(sliceDate)
                            .and(col(OLD_PREFIX + secondDateColumn).$greater$eq(sliceDate)))
                    .join(incSnpStgMinDt.as(INCR_ALIAS), joinByArray(INCR_ALIAS, OLD_ALIAS, bkName), LEFT)
                    .filter(col(INCR_PREFIX + bkName.get(0)).isNull())
                    .select(OLD_PREFIX_ALL);
            Dataset<Row> entityDeletes = castDataset(sliceLogicalDeletes
                    .withColumn(DELETED_FLAG, lit(true))
                    .withColumn(firstDateColumn, sliceDate)
                    .withColumn(secondDateColumn, lit(PLUS_INFINITY_DT).cast(firstDateStruct.dataType())), inc.schema());
            entityDeletes = calcBusinessHistoryDerivedPartFields(entityDeletes);
            incrMinDt = landStgTmpDataset(incrMinDt.unionByName(entityDeletes),
                    partitionSpecs, "_pk_incr_with_del_log");
            incSnpStgMinDt = incrMinDt
                    .groupBy(bkName.stream().map(functions::col).toArray(Column[]::new))
                    .agg(min(col(firstDateColumn)).as(MIN_DT))
                    .as(INCR_ALIAS);
            incSnpStgMinDt = landStgTmpDataset(incSnpStgMinDt, partitionSpecs, "_incr_min_dt");
            oldSnpMinDt = snp.as(OLD_ALIAS)
                    .join(incSnpStgMinDt.as(INCR_ALIAS), joinByArray(INCR_ALIAS, OLD_ALIAS, bkName), LEFT)
                    .withColumn(MIN_DT, coalesce(col(MIN_DT), sliceDate));
        }

        // vspc-3.0 parity (D2): закрывающая дата версии = another_ctl_validfrom - 2, где another_ctl_validfrom —
        // момент загрузки (per-load now) в 3.0; новая версия при этом стартует с validfrom-1 ⇒ стык без разрыва.
        // Первоисточники: DataLoaderHistSnp.java:106 (END_DT = date_sub(col(ANOTHER_CTL_VALIDFROM), N2)),
        // DataLoaderHistMix.java; эквивалентность штатной min_dt-1 при 3.0-раскладке входа: agent_histmix_parity.md:69.
        // Штатно здесь: timeUnitSub(timeUnit, col(MIN_DT), 1). Воспроизводим 3.0-якорь «день загрузки» дословно.
        // vspc-3.0 parity (A2-CC2-01): закрываемость версии — закрывать можно ТОЛЬКО открытую версию
        // (end_dt = 9999-12-31). 3.0: в merge со снапом участвуют только строки снапа с
        // p1month='999912' (end_dt = MAX_DATE) — DataLoaderHistSnp.java:69-70, DataLoaderHistMix.java:65-66,
        // DataLoaderLinkHistDiff.java:70-71, DataLoaderHistDiff.java:68-69; версия с уже проставленной
        // датой закрытия (бизнес-дата) в merge не попадает и дату закрытия не переписывает.
        // 4.0 было: start_dt < MIN_DT ∧ end_dt ≥ MIN_DT для ЛЮБОЙ версии ⇒ версия с бизнес-датой закрытия
        // перезакрывалась на loadDate-2 (расходились end_dt/p1month версии и end_dt её redo-ноги в diff).
        Column openVersionCond = col(secondDateColumn)
                .equalTo(lit(PLUS_INFINITY_DT).cast(firstDateStruct.dataType()));
        Column crossingCond = col(firstDateColumn).$less(col(MIN_DT))
                .and(col(secondDateColumn).$greater$eq(col(MIN_DT)))
                .and(openVersionCond);
        oldSnpMinDt = oldSnpMinDt.select(OLD_PREFIX_ALL, MIN_DT)
                .withColumn(TMP_UPD_FLG, when(crossingCond, lit(1))
                        .otherwise(lit(0)))
                .withColumn(TMP_UPD_PREFIX + secondDateColumn, when(crossingCond, date_sub(lit(loadDate), 2)) // vspc-3.0 parity (D2): end := loadDate-2 (≡ another_ctl_validfrom-2 в 3.0)
                        .otherwise(col(secondDateColumn)));

        // vspc-4.0 cleanup (решение владельца 02.11.2026, карточка D-10 / A2-D5-04): удалена ветка
        // «схлопывание последовательных записей снепшот-инкремента с одинаковыми значениями атрибутов»
        // (if (collapsePeriods) {...}) с двумя приземлениями _inc_collapsed / _snp_collapsed — она
        // никогда не выполнялась: признак collapsePeriods в PublisherImpl по умолчанию false
        // (vendor/rt-fw-publisher-2.16.1-src/.../PublisherImpl.java:103), а сеттер Builder.collapsePeriods()
        // (там же :150-151) в дереве 4.0 не вызывается ⇒ значение всегда false, тело ветки недостижимо.
        // Расчёт изменений не меняется: ветка не создавала ни строк, ни промежуточных приземлений.
        // Параметр конструктора collapsePeriods сохранён — он входит в бинарный контракт с vendor-классом
        // CalculatorFactory.createChangesCalculator (vendor/.../strategy/CalculatorFactory.java:57-65),
        // вызываемым из неизменяемого jar rt-fw-publisher-2.16.1.

        oldSnpMinDt = landStgTmpDataset(oldSnpMinDt, partitionSpecs, "_snp_upd");

        SnapshotChanges snpCh = calculateDiff2Datasets(incrMinDt, oldSnpMinDt
                        .filter(col(firstDateColumn).$greater$eq(col(MIN_DT)))
                        .drop(MIN_DT, TMP_UPD_PREFIX + secondDateColumn, TMP_UPD_FLG),
                true, true, partitionSpecs);

        Dataset<Row> inserts = snpCh.getInserts();
        Dataset<Row> updatesRedo = snpCh.getUpdates().filter(col(CTL_DO).equalTo(lit(CTL_DO_REDO)));
        Dataset<Row> updatesUndo = snpCh.getUpdates().filter(col(CTL_DO).equalTo(lit(CTL_DO_UNDO)));
        Dataset<Row> deletes = snpCh.getDeletes();
        // Если удаления физические, то формируем их из логических
        if (!Arrays.asList(tgtSchema.fieldNames()).contains(DELETED_FLAG)) {
            inserts = castDataset(inserts.filter(col(DELETED_FLAG).equalTo(false)), tgtSchema)
                    .withColumn(CTL_DO, lit(CTL_DO_REDO))
                    .withColumn(CTL_ACTION, lit(CTL_ACTION_INSERT));
            updatesRedo = castDataset(snpCh.getUpdates().filter(col(CTL_DO).equalTo(lit(CTL_DO_REDO))), tgtSchema)
                    .withColumn(CTL_DO, lit(CTL_DO_REDO))
                    .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE));
            updatesUndo = castDataset(snpCh.getUpdates().filter(col(CTL_DO).equalTo(lit(CTL_DO_UNDO))), tgtSchema)
                    .withColumn(CTL_DO, lit(CTL_DO_UNDO))
                    .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE));
            deletes = castDataset(snpCh.getDeletes(), tgtSchema)
                    .withColumn(CTL_DO, lit(CTL_DO_REDO))
                    .withColumn(CTL_ACTION, lit(CTL_ACTION_DELETE));
        }

        // vspc-3.0 parity (В-4, out-of-order): 3.0 закрываемую версию отбрасывает МОЛЧА — фильтром
        // END_DT >= START_DT (DataLoaderHistSnp.java:117-122; DataLoaderHistMix.java:88-93;
        // DataLoaderLinkHistDiff.java:93-98): закрывающая дата версии = another_ctl_validfrom-2, и если
        // она меньше start_dt версии, версия не публикуется НИ в снап, НИ в diff ⇒ значение более новой
        // версии теряется, ног — 0 (у этой версии в 3.0 ног не остаётся вообще).
        // Штатный 4.0 в том же сценарии (версия снапа новее инкремента: её start_dt > MIN_DT) версию не
        // закрывал, а отдавал физическим удалением ⇒ в diff уходила пара R/D + U/I (4 ноги вместо 2), и
        // версия оставалась в снапе: merge берёт inserts целиком (GeneralMergeCalculator.java:75-76, без
        // фильтра по ctl_do), а undo-нога удаления (U/I) лежит в inserts ⇒ строка возвращалась в снап.
        // Помечаем такие удаления маркером SILENT_DROP_COL: shadow-ProcessingStage вычищает их ноги из
        // diff-таблицы (в 3.0 следов нет) и возвращает redo-ногу в набор удалений для merge — версия
        // обязана исчезнуть из снапа (как в 3.0). В обычном INCR этот набор пуст (версии снапа не новее
        // MIN_DT), т.е. правка — no-op вне сценария out-of-order.
        deletes = deletes.withColumn(SILENT_DROP_COL, lit(true));

        // vspc-3.0 parity (consec-day corner, f06 §9.1, отчёты W1-W3 2026-10-28): часть-1 образует
        // update-пару при совпадении ПОЛНОГО PK [sk,start_dt]. Штамп трансформеров start = loadDate-1
        // делает такое совпадение достижимым только при повторной загрузке ТОГО ЖЕ календарного дня
        // (старая открытая версия тоже со штампом loadDate-1 == MIN_DT; TMP_UPD_FLG=0 ⇒ crossing-ног
        // нет) и для div_h сразу после INIT (бизнес-датированный хвост цепочки chg_div_h со
        // start == D-1) — иного источника update-пар части-1 на PERIOD-таблицах нет.
        // В 3.0 окно SnpDataLoader самопарило NOTEQ-пару, закрывающую ногу убивал DROPBAD
        // (end = another_vf-2 < start = vf-1: DataLoaderHistSnp.java:118-119 «второе обновление за
        // день… похерить»), старая версия стиралась молча, журнал = пара I/R + U/D новой версии
        // (undo — recolor той же строки, D2 §1.2). Воспроизводим форму 3.0: ОБЕ ноги несут payload
        // НОВОЙ версии (undo перекрашивается из redo), метки (R,I) и (U,D); прежняя undo-нога
        // (payload старой версии) отбрасывается — в 3.0 её не было. Снап не меняется: GeneralMerge
        // берёт из updates только redo-ноги и замещает по PK (replace-on-place ≡ «стирание+вставка»
        // 3.0), ctl_action отбрасывается castDataset'ом к схеме снапа. Пустые наборы — no-op.
        // Метку redo-ноги (R,I) ставим в точке сборки `_snp_upd_redo` ниже: TECH_FIELDS (включая
        // ctl_action) strip'аются в calcBusinessHistoryDerivedPartFields.
        // vspc-3.0 parity (A2-E3-N2): p1month пересчитывается по end_dt для КАЖДОЙ ноги diff —
        // 3.0 считает p1month по end_dt в самом конце, для всего targetDiff сразу
        // (DataLoaderHistSnp.java:141-154, DataLoaderHistDiff.java:115-127, DataLoaderHistMix.java:128-140,
        // DataLoaderLinkHistDiff.java:116-128 — addPartitionMonthColumn по всему набору ног).
        // 4.0 пересчитывал p1month только в трёх ветвях (redo части-1, redo пересечения, redo vanish),
        // остальные ноги наследовали значение из строки снапа/инкремента ⇒ инвариант
        // «p1month ноги = yyyyMM(end_dt) ноги» не был конструктивен.
        // Здесь пересчитываем p1month для undo-ноги части-1 (redo-нога уже пересчитана выше).
        updatesUndo = recalcDiffP1Month(updatesRedo)
                .withColumn(CTL_DO, lit(CTL_DO_UNDO))
                .withColumn(CTL_ACTION, lit(CTL_ACTION_DELETE));

        // vspc-3.0 parity (DROPBAD): отбраковка «второго обновления за день» — закрывающая нога с
        // end_dt < start_dt не публикуется; канон 3.0: where(ANOTHER_DEL.or(END_DT >= START_DT)),
        // DataLoaderHistSnp.java:119 / DataLoaderHistMix.java. Часть 1 ног (calculateDiff2Datasets) не тронута.
        // vspc-3.0 parity (consec-day corner): calcBusinessHistoryDerivedPartFields strip'ает
        // TECH_FIELDS (в т.ч. ctl_do/ctl_action), поэтому метки части-1 возвращаем сразу после
        // лендинга; ctl_action здесь = перекраска в 3.0-форму (R,I) из блока выше. Crossing-ноги
        // (обычный соседне-дневный churn, end := loadDate-2) штампуются отдельно в прежней форме (R,U).
        Dataset<Row> updateDiffRedo = landStgTmpDataset(
                castDataset(calcBusinessHistoryDerivedPartFields(updatesRedo), tgtSchema),
                partitionSpecs, "_snp_upd_redo")
                .withColumn(CTL_DO, lit(CTL_DO_REDO))
                .withColumn(CTL_ACTION, lit(CTL_ACTION_INSERT));
        updateDiffRedo = updateDiffRedo.unionByName(castDataset(calcBusinessHistoryDerivedPartFields(oldSnpMinDt
                .filter(col(TMP_UPD_FLG).equalTo(lit(1)))
                .filter(col(TMP_UPD_PREFIX + secondDateColumn).$greater$eq(col(firstDateColumn))) // vspc-3.0 parity (DROPBAD): end >= start
                .withColumn(secondDateColumn, col(TMP_UPD_PREFIX + secondDateColumn))), tgtSchema)
                .withColumn(CTL_DO, lit(CTL_DO_REDO))
                .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE)));

        Dataset<Row> updateDiffUndo = landStgTmpDataset(updatesUndo, partitionSpecs, "_snp_upd_undo");
        updateDiffUndo = updateDiffUndo.unionByName(castDataset((oldSnpMinDt
                .filter(col(TMP_UPD_FLG).equalTo(lit(1)))
                .filter(col(TMP_UPD_PREFIX + secondDateColumn).$greater$eq(col(firstDateColumn)))), tgtSchema) // vspc-3.0 parity (DROPBAD): end >= start
                .withColumn(CTL_DO, lit(CTL_DO_UNDO))
                .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE)));

        // vspc-3.0 parity (VANISH): ключи, исчезнувшие из источника, штатный INCR не закрывает — старый снап
        // отсечён INNER-join'ом с min-датами инкремента выше. В 3.0 такие OLD-записи закрывались
        // end_dt = дата загрузки − 1 ТОЛЬКО у DataLoaderHistSnp (div_type, closeOld=true); HistDiff/HistMix
        // держали их открытыми. Гейт: имя таблицы == CLOSE_VANISHED_TABLE И только INCR (fullSlice=false) —
        // в fullSlice-режимах vanish уже обработан штатно (sliceLogicalDeletes / calculateReinitDiff).
        // Redo = закрытая строка (end := current_date()−1, TZ сессии Spark — как в 3.0), undo = исходная
        // открытая строка (откат вектора обновления).
        if (!fullSlice && CLOSE_VANISHED_TABLE.equals(tableInfoFactory.createTableInfo(SNP, "").getTableName())) {
            // vspc-3.0 parity: кандидаты в vanish — ключи, ОТСУТСТВУЮЩИЕ в исходном инкременте (до
            // EQUALS-retention): retained-ключи приходят из источника и исчезнувшими не являются.
            Dataset<Row> vanishedOpenRows = snp.as(OLD_ALIAS)
                    .join(incrMinDtPreRetention.select(bkName.stream().map(functions::col).toArray(Column[]::new))
                                    .distinct().as(INCR_ALIAS),
                            joinByArray(INCR_ALIAS, OLD_ALIAS, bkName), LEFT)
                    .filter(col(INCR_PREFIX + bkName.get(0)).isNull())
                    // vspc-3.0 parity (VANISH, arbiter В3 правка-1): закрываем ТОЛЬКО открытые (end_dt=+∞)
                    // версии — закрытие происходит ровно один раз, при первом исчезновении ключа из источника;
                    // без фильтра стек закрытых версий отсутствующего ключа перевыполнялся бы ежедневно
                    // (слайд end_dt, ежедневные холостые R/U+U/U-ноги, схлопывание стека версий при churn).
                    .filter(col(OLD_PREFIX + secondDateColumn)
                            .equalTo(lit(PLUS_INFINITY_DT).cast(firstDateStruct.dataType())))
                    .select(OLD_PREFIX_ALL);
            updateDiffRedo = updateDiffRedo.unionByName(castDataset(calcBusinessHistoryDerivedPartFields(
                            vanishedOpenRows.withColumn(secondDateColumn, date_sub(current_date(), 1))), tgtSchema) // vspc-3.0 parity (VANISH): end := current_date()-1 (DataLoaderHistSnp.java:106)
                    .withColumn(CTL_DO, lit(CTL_DO_REDO))
                    .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE)));
            updateDiffUndo = updateDiffUndo.unionByName(castDataset(vanishedOpenRows, tgtSchema)
                    .withColumn(CTL_DO, lit(CTL_DO_UNDO))
                    .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE)));
        }

        // vspc-3.0 parity (A2-E3-N2): p1month пересчитывается по end_dt для КАЖДОЙ ноги diff.
        // 3.0 пересчитывает его по всему собранному набору ног одной операцией —
        // addPartitionMonthColumn на targetDiff (DataLoaderHistSnp.java:154, DataLoaderHistDiff.java:127,
        // DataLoaderHistMix.java:140, DataLoaderLinkHistDiff.java:128). В 4.0 пересчёт шёл только в трёх
        // ветвях (redo части-1, redo пересечения, redo vanish) — остальные ноги наследовали p1month
        // из строки снапа/инкремента ⇒ инвариант «p1month ноги = yyyyMM(end_dt) ноги» не был конструктивен.
        // Здесь пересчёт доведён до конца: ноги с уже пересчитанным p1month сохраняют значение
        // (пересчёт идемпотентен), остальные получают p1month = yyyyMM(end_dt) своей ноги.
        return new SnapshotChanges(
                recalcDiffP1Month(inserts),
                recalcDiffP1Month(updateDiffRedo.unionByName(updateDiffUndo)),
                recalcDiffP1Month(deletes));
    }

    // vspc-3.0 parity (FX2b.1): часть 1 расчёта ног — переопределение вендорного метода
    // AbstractChangesCalculator.calculateDiff2Datasets (AbstractChangesCalculator.java:110-212).
    // Вендор определяет признак «атрибуты изменились» (_tmp_upd_flg_ch) по хэшу
    // md5(concat_ws("|", attrCols)) (AbstractChangesCalculator.java:138-139) — с разделителем "|".
    // Канон ESSA 3.0 — xxhash64(concat(...)) БЕЗ разделителя (SnpDataLoader.getHashColumn,
    // SnpDataLoader.java:116-117), набор и порядок полей — SnpDataLoader.getStringHash
    // (SnpDataLoader.java:120-133). Разделитель различает «ab»+«c» и «a»+«bc», тогда как 3.0
    // (склейка без разделителя) считает их равными ⇒ 4.0 трактует часть изменений как NOTEQUALS
    // вместо EQUALS ⇒ разный состав ног в diff-таблице и разный состав строк в снапе.
    // Метод в вендоре protected, не final, не static ⇒ переопределение перехватывает вызов
    // из calculateImpl (:505) и из calculateReinitDiff (ветка fullSlice && reInit, :278-283);
    // jar вендора (rt-fw-publisher-2.16.1.jar) не изменяется — правка только в shadow-классе.
    @Override
    protected SnapshotChanges calculateDiff2Datasets(Dataset<Row> incr,
                                                     Dataset<Row> target,
                                                     boolean calculateDeletes,
                                                     boolean physicalDeletion,
                                                     List<Map<String, String>> partitionSpecs) {
        final StructType tgtSchema = target.schema();
        final String HASH_COL = HASH_COL_NAME; // "tech_cv_md5" — служебный столбец хэша строки
        final String UPD_FLG = "_tmp_upd_flg_ch";
        final String UPD_ALIAS = "_tmp_upd";

        // 3 варианта: не вычислять удаления; вычислять физические удаления; вычислять логические удаления
        assert (calculateDeletes || !physicalDeletion);
        // если вычисляем логические удаления, то в схеме должно быть поле deleted_flag
        assert (!calculateDeletes || physicalDeletion || Arrays.asList(tgtSchema.fieldNames()).contains(DELETED_FLAG));

        // vspc-3.0 parity (FX2b.1): набор и порядок полей хэша — канон ESSA 3.0
        // (SnpDataLoader.getStringHash, SnpDataLoader.java:120-133): все поля, кроме "ctl*",
        // start_dt/end_dt (excludeHashColumns витрины — VspcMetadataKeeper.java:19-25) и
        // производных полей секционирования (p1month). Порядок — порядок колонок инкремента
        // (= порядок схемы целевого снапа: castToTargetStructure, ProcessingStage.java:137).
        // Поля, отсутствующие в снапе, из хэша исключаются: выражение хэша применяется к обоим наборам.
        List<String> tgtColumns = Arrays.asList(target.columns());
        String[] attrToHash = Arrays.stream(incr.columns())
                .filter(c -> !c.startsWith("ctl") && !c.equals(firstDateColumn) && !c.equals(secondDateColumn)
                        && !derivedPartitioningFields.containsKey(c) && tgtColumns.contains(c))
                .toArray(String[]::new);
        Column[] attrCols = Arrays.stream(attrToHash)
                .map(attr -> when(col(attr).isNull(), lit("")).otherwise(col(attr).cast(DataTypes.StringType)))
                .toArray(Column[]::new);
        // vspc-3.0 parity (FX2b.1): xxhash64(concat(...)) БЕЗ разделителя — канон 3.0
        // (SnpDataLoader.getHashColumn, SnpDataLoader.java:116-117)
        Column hashCol = attrCols.length == 0
                ? xxhash64(lit(""))
                : xxhash64(functions.concat(attrCols));

        Column doNotInsertLogicalDeletes = !physicalDeletion &&
                Arrays.asList(tgtSchema.fieldNames()).contains(DELETED_FLAG) ?
                actualRow(col(INCR_PREFIX + DELETED_FLAG)) :
                lit(true).cast(DataTypes.BooleanType);

        Dataset<Row> upd = target.withColumn(HASH_COL, hashCol).as(OLD_ALIAS)
                .join(incr.withColumn(HASH_COL, hashCol).as(INCR_ALIAS),
                        joinByArray(OLD_ALIAS, INCR_ALIAS, pkName), INNER)
                .withColumn(UPD_FLG, when(col(INCR_PREFIX + HASH_COL)
                        .notEqual(col(OLD_PREFIX + HASH_COL)), lit(1))
                        .otherwise(lit(0)))
                .select(scalaSeq(Stream.concat(pkName.stream().map(c -> col(OLD_PREFIX + c)),
                        Stream.of(col(UPD_FLG))).collect(Collectors.toList())));
        upd = landStgTmpDataset(upd, partitionSpecs, "_pk_upd");

        // Формирование операций INSERT
        Dataset<Row> insertMapped = castDataset(incr.as(INCR_ALIAS)
                .join(upd.as(UPD_ALIAS), joinByArray(UPD_ALIAS, INCR_ALIAS, pkName), LEFT)
                .filter(col(UPD_ALIAS + "." + pkName.get(0)).isNull().and(doNotInsertLogicalDeletes))
                .select(INCR_PREFIX_ALL), tgtSchema)
                .withColumn(CTL_DO, lit(CTL_DO_REDO))
                .withColumn(CTL_ACTION, lit(CTL_ACTION_INSERT));
        insertMapped  = landStgTmpDataset(insertMapped, partitionSpecs, "_pk_ins");

        // Формирование операций UPDATE
        Dataset<Row> updateMappedRU = castDataset(
                upd.as(UPD_ALIAS).filter(col(UPD_FLG).equalTo(lit(1)))
                        .join(incr.as(INCR_ALIAS), joinByArray(UPD_ALIAS, INCR_ALIAS, pkName), INNER)
                        .select(INCR_PREFIX_ALL), tgtSchema)
                .withColumn(CTL_DO, lit(CTL_DO_REDO))
                .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE))
                .unionByName( // операции UNDO
                        castDataset(
                                upd.as(UPD_ALIAS).filter(col(UPD_FLG).equalTo(lit(1)))
                                        .join(target.as(OLD_ALIAS), joinByArray(UPD_ALIAS, OLD_ALIAS, pkName), INNER)
                                        .select(OLD_PREFIX_ALL), tgtSchema)
                                .withColumn(CTL_DO, lit(CTL_DO_UNDO))
                                .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE)));
        updateMappedRU = landStgTmpDataset(updateMappedRU, partitionSpecs, "_pk_upd_ru");

        Dataset<Row> emptyDeleteMapped = dao.createDataFrame(new ArrayList<Row>(), insertMapped.schema());

        // Если не вычисляем удаления
        if (!calculateDeletes) {
            return new SnapshotChanges(insertMapped, updateMappedRU, emptyDeleteMapped);
        }

        // Если вычисляем логическия удаления (апдейты deleted_flag = true)
        if (!physicalDeletion) { // если не физическое удаление - то логическое
            Dataset<Row> logicalDeleteR = castDataset(target.as(OLD_ALIAS)
                    .join(upd.as(UPD_ALIAS), joinByArray(UPD_ALIAS, OLD_ALIAS, pkName), LEFT)
                    .filter(col(UPD_ALIAS + "." + pkName.get(0)).isNull())
                    .filter(actualRow(col(OLD_PREFIX + DELETED_FLAG)))
                    .select(OLD_PREFIX_ALL), tgtSchema)
                    .withColumn(DELETED_FLAG, lit(true))
                    .withColumn(CTL_DO, lit(CTL_DO_REDO))
                    .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE));
            logicalDeleteR = landStgTmpDataset(logicalDeleteR, partitionSpecs, "_pk_del_log");
            Dataset<Row> logicalDeleteRU = logicalDeleteR.unionByName(logicalDeleteR
                    .withColumn(DELETED_FLAG, lit(false))
                    .withColumn(CTL_DO, lit(CTL_DO_UNDO))
                    .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE)));

            return new SnapshotChanges(
                    insertMapped,
                    updateMappedRU.unionByName(logicalDeleteRU),
                    emptyDeleteMapped);
        }

        // Иначе вычисляем физические удаления (ctl_action=D)
        Dataset<Row> deleteMapped = castDataset(target.as(OLD_ALIAS)
                .join(upd.as(UPD_ALIAS), joinByArray(UPD_ALIAS, OLD_ALIAS, pkName), LEFT)
                .filter(col(UPD_ALIAS + "." + pkName.get(0)).isNull())
                .select(OLD_PREFIX_ALL), tgtSchema)
                .withColumn(CTL_DO, lit(CTL_DO_REDO))
                .withColumn(CTL_ACTION, lit(CTL_ACTION_DELETE));
        deleteMapped = landStgTmpDataset(deleteMapped, partitionSpecs, "_pk_del_phy");

        return new SnapshotChanges(insertMapped, updateMappedRU, deleteMapped);
    }

    protected Dataset<Row> calcBusinessHistoryDerivedPartFields(Dataset<Row> dataset) {
        // Поскольку меняются временные отрезки, то должны быть заново пересчитаны значения полей секционирования,
        // производных от полей дат технической истории
        Map<String, Partition> endDtBasedDerivedPartitioningFields = derivedPartitioningFields.entrySet().stream()
                .filter(e -> secondDateColumn.equals(e.getValue().getOriginColumnCode()) ||
                        firstDateColumn.equals(e.getValue().getOriginColumnCode()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return sectionBy(dataset.drop(scalaSeq(TECH_FIELDS)), endDtBasedDerivedPartitioningFields,
                sectioningDMin, sectioningDMax);
    }

    /**
     * vspc-3.0 parity (A2-E3-N2): пересчёт поля секционирования бизнес-истории (p1month) по end_dt
     * БЕЗ снятия tech-полей.
     *
     * <p>{@link #calcBusinessHistoryDerivedPartFields} применим только к набору, у которого ctl_* уже
     * сняты (его вызов стоит ДО простановки меток redo/undo). Для ног, у которых метки (ctl_do,
     * ctl_action) и маркер SILENT_DROP_COL уже проставлены, нужен отдельный вариант: sectionBy
     * перезаписывает ТОЛЬКО колонку секционирования (PublisherUtils.sectionBy — вызов
     * dataset.withColumn(partitionName, …)) и сохраняет все прочие колонки набора.
     *
     * <p>Формула совпадает с 3.0: yyyyMM(end_dt) с подстановкой MIN_DATE при NULL
     * (Expression.java:42-44,65-72; DataLoaderHistSnp.java:154). Клампинг границ секционирования
     * (SectionBy.applySectioningRange) даёт те же значения на достижимых данных: end_dt = 9999-12-31
     * ⇒ «999912», end_dt = 1900-01-01 и NULL ⇒ «190001».
     */
    protected Dataset<Row> recalcDiffP1Month(Dataset<Row> dataset) {
        Map<String, Partition> endDtBasedDerivedPartitioningFields = derivedPartitioningFields.entrySet().stream()
                .filter(e -> secondDateColumn.equals(e.getValue().getOriginColumnCode()) ||
                        firstDateColumn.equals(e.getValue().getOriginColumnCode()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (endDtBasedDerivedPartitioningFields.isEmpty()) {
            return dataset; // секция бизнес-истории не задана — пересчитывать нечего
        }
        return sectionBy(dataset, endDtBasedDerivedPartitioningFields, sectioningDMin, sectioningDMax);
    }
}
