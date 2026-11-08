package ru.sberbank.bigdata.sl.rt.publisher.strategy.process;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Partition;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SnapshotChanges;
import ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfo;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfoFactory;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.changes.ChangesCalculator;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.merge.MergeCalculator;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.publish.PublicationStage;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.publish.PublicationStep;
import ru.sberbank.bigdata.sl.rt.publisher.validation.EntityInfo;
import scala.collection.Seq;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION_DELETE;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO_REDO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_LOADING;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_VALIDFROM;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.SYSTEM_EXTERNAL_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.SYSTEM_HUB_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.TIMESTAMP_PATTERN;
import static ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName.STG;
import static ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName.STGTMP;
import static ru.sberbank.bigdata.sl.rt.publisher.strategy.process.changes.HistoricalPeriodWithRewriteChangesCalculator.SILENT_DROP_COL;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.TECH_FIELDS;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.buildFilterByPartitions;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.buildFilterByPartitionsExcludingIgnoredFields;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.scalaSeq;

/**
 * Shadow-копия vendor-класса rt-fw-publisher-2.16.1 (тот же пакет; замещение в fatjar —
 * shade-exclude в pom.xml, механика та же, что у PublicationStage/ChangesCalculators).
 * Создаётся shadow-PreparationStage'ом (readyForProcessing) и работает в паре с ним.
 *
 * Единственное отличие от vendor (контракт ctl_validfrom, temp/ctl_validfrom_contract/):
 * enrichTechFieldsForInit() стал fill-if-absent по ctl_validfrom — принесённое производителем
 * значение сохраняется; если колонки в наборе нет или она NULL, ставится прежний безусловный
 * NOW-штамп (формат/таймзона литерала — ровно как в vendor). Дополнительно — паритет 3.0
 * (DataLoaderHistMix.java:109, ctl_validfrom := start_dt): при NULL start_dt метка не ставится,
 * ctl_validfrom остаётся NULL. ctl_loading остаётся БЕЗУСЛОВНЫМ
 * штампом загрузки (идентичность загрузки — владение транспорта). Остальное тело класса
 * байт-в-байт от vendor; создаваемый здесь PublicationStage — тоже shadow-копия.
 */
public class ProcessingStage implements ProcessingStep {
    private final TableInfoFactory tableInfoFactory;
    private final List<Map<String, String>> partitionSpecs;
    private final Map<String, Partition> derivedPartitioningFields;
    private final PublisherDao dao;
    private Dataset<Row> dataset;
    private Dataset<Row> oldSnp;
    private SnapshotChanges changes;
    // vspc-3.0 parity (В-4, out-of-order): версии снапа, которые 3.0 отбрасывает МОЛЧА (значение теряется,
    // ног в diff нет) — храним их между calculateDiff и readChangesFromStg, чтобы (1) вычистить их ноги
    // из diff-таблицы и (2) вернуть их в набор удалений для merge: версия обязана исчезнуть из снапа.
    // Состояние живёт только в рамках одной загрузки (поле экземпляра), новых файлов/состояний нет.
    private Dataset<Row> silentDroppedOldVersions;
    // Инициализация непустой картой: значение подставляется только в calculateDiff (:185), а в PublicationStage
    // (в PublicationStage: 6-аргументный конструктор :73-86; поле читается в moveToTargetTable :155-166) уходит при INIT/REINIT,
    // где calculateDiff не вызывается ⇒ раньше туда утекал null.
    // Нормализация null → emptyMap в PublicationStage (:83-85) остаётся (идемпотентна к этой инициализации).
    Map<String, String> readSnpPartitionGeqFilters = Collections.emptyMap();

    public ProcessingStage(PublisherDao dao,
                           Dataset<Row> dataset,
                           TableInfoFactory tableInfoFactory,
                           List<Map<String, String>> partitionSpecs,
                           Map<String, Partition> derivedPartitioningFields) {
        this.dao = dao;
        this.tableInfoFactory = tableInfoFactory;
        this.partitionSpecs = partitionSpecs;
        this.derivedPartitioningFields = derivedPartitioningFields;

        // Значения производных поля секционирования для записей входного набора ещё не рассчитаны
        // (это делается в calculateDerivedPartitioningFields) => игнорировать производные поля при построении фильтра
        org.apache.spark.sql.Column filterExpr = buildFilterByPartitionsExcludingIgnoredFields(
                partitionSpecs, derivedPartitioningFields);
        this.dataset = dataset.filter(filterExpr);
    }

    @Override
    public ProcessingStep enrichTechFieldsForInit(int loadingId) {
        // Контракт ctl_validfrom: fill-if-absent — принесённое производителем значение уважается;
        // нет колонки/значения — прежний безусловный NOW-штамп. ctl_loading — безусловный штамп.
        Column ctlValidfrom = lit(DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN)
                .format(LocalDateTime.now())).cast(DataTypes.TimestampType);
        if (Arrays.asList(dataset.columns()).contains(CTL_VALIDFROM)) {
            ctlValidfrom = when(col(CTL_VALIDFROM).isNull(), ctlValidfrom).otherwise(col(CTL_VALIDFROM));
        }
        // vspc-3.0 parity: ctl_validfrom := start_dt (3.0 DataLoaderHistMix.java:109) ⇒ при NULL start_dt
        // метка остаётся NULL, NOW не подставляется. Правило затрагивает только таблицы, у которых в цели
        // есть start_dt: div_h (единственная, где start_dt бывает NULL — DivHTransformation.java:122 несёт
        // ctl_validfrom = start_dt, NULL сохраняется). У addr_h/div_type/div_div_relation start_dt непустой
        // (1900-01-01 на INIT / loadDate-1 на INCR) ⇒ NOW остаётся; у хабов (addr/div_/terr_div_type/
        // time_zone_rf) колонки start_dt в цели нет вовсе ⇒ NOW остаётся — как в 3.0
        // (DataLoaderHistDiff.java:99, DataLoaderHistSnp.java:126, DataLoaderLinkHistDiff.java:101,
        // DataLoaderHubDiff.java:43).
        if (Arrays.asList(dataset.columns()).contains(START_DT_FIELD_NAME)) {
            ctlValidfrom = when(col(START_DT_FIELD_NAME).isNull(), lit(null).cast(DataTypes.TimestampType))
                    .otherwise(ctlValidfrom);
        }
        dataset = dataset
                .withColumn(CTL_VALIDFROM, ctlValidfrom)
                .withColumn(CTL_LOADING, lit(loadingId).cast(DataTypes.LongType));
        return this;
    }

    @Override
    public ProcessingStep collapsePeriods(boolean collapseEnabled, EntityInfo entityInfo) {
        if (!collapseEnabled) {
            return this; // игнорируем шаг схлопывания
        }

        // Схлопываем последовательные версии с одинаковым значением атрибутов
        String[] attrToHash = Arrays.stream(this.dataset.columns()).filter(c ->
                        !entityInfo.getPkFields().contains(c) && !entityInfo.getDerivedPartitioningFields().containsKey(c) &&
                                !c.equals(entityInfo.getStartDtField()) && !c.equals(entityInfo.getEndDtField()) &&
                                !TECH_FIELDS.contains(c))
                .toArray(String[]::new);
        this.dataset = PublisherUtils.collapsePeriods(this.dataset,
                entityInfo.getPkFields(), Arrays.asList(attrToHash),
                entityInfo.getStartDtField(), entityInfo.getEndDtField());
        return this;
    }

    @Override
    public PublicationStep saveToStgRepartitioned(SchemaName targetSchemaName, String tableNameSuffix, String preSuffix) {
        // Здесь ожидается, что целевая таблица уже существует
        TableInfo tgtTableInfo = tableInfoFactory.createTableInfo(targetSchemaName);
        this.dataset = PublisherUtils.castToTargetStructure(dataset, tgtTableInfo.enrichedWithCatalogInfo(dao).getCatalogInfo().getSchema());

        TableInfo stgTableInfo = tableInfoFactory.createTmpTableInfo(STG, tableNameSuffix, partitionSpecs);
        Seq<String> partitionColumnNames = tableInfoFactory.createTableInfo(targetSchemaName).
                enrichedWithCatalogInfo(dao).
                getCatalogInfo().
                getPartitionColumnNames();

        TableInfo preTableInfo = tableInfoFactory.createTmpTableInfo(STGTMP, tableNameSuffix + preSuffix, partitionSpecs);
        dao.writeTable(stgTableInfo, partitionColumnNames, dataset, preTableInfo);
        return new PublicationStage(dao, tableInfoFactory, partitionSpecs, targetSchemaName, tableNameSuffix, readSnpPartitionGeqFilters);
    }

    @Override
    public ProcessingStep calculateDerivedPartitioningFields(String dMin, String dMax) {
        this.dataset = PublisherUtils.sectionBy(this.dataset, this.derivedPartitioningFields, dMin, dMax)
                // Данные поля входного набора являются базой для расчёта производных полей секционирования
                // результирующего набора данных, при этом сами не должны попадать в целевую область.
                .drop(scalaSeq(SYSTEM_HUB_DT, SYSTEM_EXTERNAL_DT));
        return this;
    }

    @Override
    public ProcessingStep calculateDiff(ChangesCalculator changesCalculator, SchemaName schemaName) {
        TableInfo snpTableInfo = tableInfoFactory.createTableInfo(schemaName);

        // publisher−08.2 -	чтение только тех секций таблиц-приемников, данные в которых должны быть изменены:
        Column filterExpr = buildFilterByPartitions(partitionSpecs);

        // publisher−08.3 -	для историчных сущностей вычитывать не весь snapshot, а начиная с min_date
        Column filterBusinessHistory = changesCalculator.getMinBusinessDateUpdateFilter();

        this.oldSnp = dao.readTable(snpTableInfo).filter(filterExpr).filter(filterBusinessHistory);
        // валидация обязательных полей в таргете (для отладки), см. https://confluence.sberbank.ru/display/USL/Technical+columns
        assert(Arrays.asList(this.oldSnp.columns()).contains(CTL_VALIDFROM));
        assert(Arrays.asList(this.oldSnp.columns()).contains(CTL_LOADING));

        this.readSnpPartitionGeqFilters = changesCalculator.getMinBusinessDateUpdateFilters();
        this.changes = changesCalculator.calculate(this.dataset, this.oldSnp, partitionSpecs);
        // vspc-3.0 parity (В-4, out-of-order): 3.0 закрываемую версию (версию снапа, чей start_dt новее
        // инкремента) отбрасывает МОЛЧА — фильтром END_DT >= START_DT (DataLoaderHistSnp.java:117-122):
        // ни строки в снапе, ни ног в diff ⇒ значение более новой версии теряется, ног 0. Штатный 4.0
        // отдавал такую версию физическим удалением ⇒ лишняя пара R/D + U/I в diff (4 ноги вместо 2) и
        // версия оставалась в снапе (undo-нога удаления U/I лежит в inserts, merge берёт inserts целиком).
        // Маркер SILENT_DROP_COL ставит shadow-калькулятор (см. HistoricalPeriodWithRewriteChangesCalculator):
        // помеченные удаления убираем из diff-таблицы (в 3.0 их там нет) и запоминаем, чтобы на шаге
        // readChangesFromStg вернуть их в набор удалений для merge — версия обязана исчезнуть из снапа.
        // Для не-исторических таблиц и для REINIT-ветки маркера нет ⇒ набор пуст, поведение не меняется.
        this.silentDroppedOldVersions = null;
        Dataset<Row> deletes = this.changes.getDeletes();
        if (deletes != null && Arrays.asList(deletes.columns()).contains(SILENT_DROP_COL)) {
            this.silentDroppedOldVersions = deletes
                    .filter(col(SILENT_DROP_COL).equalTo(lit(true)))
                    .drop(SILENT_DROP_COL);
            Dataset<Row> publishedDeletes = deletes
                    .filter(col(SILENT_DROP_COL).isNull().or(col(SILENT_DROP_COL).equalTo(lit(false))))
                    .drop(SILENT_DROP_COL);
            this.changes = new SnapshotChanges(this.changes.getInserts(), this.changes.getUpdates(), publishedDeletes);
        }
        this.dataset = this.changes.getDiff();
        return this;
    }

    @Override
    public ProcessingStep calculateMergedSnp(SchemaName targetSchemaName, MergeCalculator mergeCalculator) {
        this.dataset = mergeCalculator.calculate(oldSnp, changes, tableInfoFactory, dao, targetSchemaName, partitionSpecs);
        // FX26 (A2-D5-03): ограничение объёма записи в stg зоной перезаписи.
        this.dataset = restrictToRewriteScope(this.dataset);
        return this;
    }

    /**
     * FX26 (A2-D5-03, кампания 2026-11-02) — ограничение объёма записи в stg зоной перезаписи.
     *
     * <p>Эталон 3.0: во временную таблицу, которая затем переносится в целевую, кладются ТОЛЬКО
     * затронутые строки и строки переписываемых месяцев —
     * {@code targetSnp = actualRecords.union(outdatedRecords).union(rewritableData)}
     * (DataLoaderHistSnp.java:141-150; rewritableData = строки цели за текущий и предыдущий месяц,
     * :75-77), и только они переносятся в целевую таблицу
     * (MovingTableService.java:25-31, 33-47 — truncate целевых партиций + moveAll).
     *
     * <p>4.0 до правки: merge считает весь снап (oldSnp читается целиком — фильтр бизнес-истории
     * возвращает lit(true), HistoricalPeriodWithRewriteChangesCalculator.java:257-262) и целиком
     * пишется в stg (PublisherImpl.java:596-597), тогда как ПЕРЕНОС уже ограничен зоной перезаписи
     * (PublicationStage.restrictSnpSpecs:181-247 — по значениям партиций stg с порогом из
     * readSnpPartitionGeqFilters). Строки вне зоны перезаписи всё равно не переносились ⇒ запись
     * полного снапа в stg была чистыми накладными расходами.
     *
     * <p>Инвариант: отбор строк здесь и отбор партиций при переносе (PublicationStage:216-226)
     * используют ОДИН И ТОТ ЖЕ предикат по ОДНОЙ И ТОЙ ЖЕ колонке (секция бизнес-истории,
     * строковое сравнение &gt;= как в PublisherDao.matchHfdsLocation). Поэтому отсечённые строки —
     * ровно те, что и до правки не переносились в целевую таблицу ⇒ состав целевых таблиц
     * не меняется; меняется только объём записи.
     *
     * <p>Зона перезаписи = {секция &gt;= порога} ∪ {секция '999912' (открытые версии)};
     * порог считает shadow-калькулятор изменений (getSnpGeqFilters:209-242) по month(D-1) —
     * это месячное окно 3.0 (HistMovingGenerator.java:31).
     *
     * <p>Гейты (любой не выполнен ⇒ набор не ограничивается, поведение как до правки):
     * карта порогов непуста (её заполняет только hist-with-rewrite путь INCR — тот же гейт Г2,
     * что в PublicationStage.restrictSnpSpecs:190-194); колонки секции физически присутствуют в
     * наборе (иначе отбор невыполним).
     */
    private Dataset<Row> restrictToRewriteScope(Dataset<Row> merged) {
        // Гейт Г2 (как в PublicationStage.restrictSnpSpecs): карта порогов непуста только на
        // hist-with-rewrite пути INCR; на INIT/REINIT/simple и для не-hist таблиц её нет —
        // ограничение не применяется, поведение прежнее.
        if (readSnpPartitionGeqFilters == null || readSnpPartitionGeqFilters.isEmpty()) {
            return merged;
        }
        // Секция должна быть материальной колонкой набора.
        if (!Arrays.asList(merged.columns()).containsAll(readSnpPartitionGeqFilters.keySet())) {
            return merged;
        }
        Column scope = null;
        for (Map.Entry<String, String> geq : readSnpPartitionGeqFilters.entrySet()) {
            Column cond = col(geq.getKey()).cast(DataTypes.StringType).$greater$eq(lit(geq.getValue()));
            scope = scope == null ? cond : scope.or(cond);
        }
        return merged.filter(scope);
    }

    @Override
    public ProcessingStep readChangesFromStg(String tableNameSuffix) {
        TableInfo stgTableInfo = tableInfoFactory.createTmpTableInfo(STG, tableNameSuffix, partitionSpecs);
        Dataset<Row> diff = dao.readTable(stgTableInfo);
        this.changes = SnapshotChanges.datasetToSnapshotChanges(diff);
        // vspc-3.0 parity (В-4, out-of-order): merge собирает снап по удалениям, прочитанным ИЗ diff-stg
        // (PublisherImpl.runIncr: readChangesFromStg → calculateMergedSnp), а версии, отброшенные 3.0
        // молча, в diff-таблицу не попадают (в 3.0 их там тоже нет) ⇒ возвращаем их в набор удалений:
        // без этого версия осталась бы в снапе, тогда как 3.0 её теряет.
        // Возвращаем только redo-ногу (CTL_DO=REDO, CTL_ACTION=DELETE): undo-нога (U/I) нужна лишь для
        // diff-таблицы, а её там быть не должно.
        if (silentDroppedOldVersions != null) {
            Dataset<Row> silentRedo = silentDroppedOldVersions
                    .filter(col(CTL_DO).equalTo(lit(CTL_DO_REDO)))
                    .filter(col(CTL_ACTION).equalTo(lit(CTL_ACTION_DELETE)));
            Dataset<Row> stashed = PublisherUtils.castDataset(silentRedo, this.changes.getDeletes().schema());
            this.changes = new SnapshotChanges(this.changes.getInserts(), this.changes.getUpdates(),
                    this.changes.getDeletes().unionByName(stashed));
        }
        return this;
    }
}