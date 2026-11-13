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
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_LOADING;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_VALIDFROM;
import static ru.sberbank.bigdata.sl.rt.core.Constants.SYSTEM_EXTERNAL_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.SYSTEM_HUB_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.TIMESTAMP_PATTERN;
import static ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName.STG;
import static ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName.STGTMP;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.TECH_FIELDS;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.buildFilterByPartitions;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.buildFilterByPartitionsExcludingIgnoredFields;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.scalaSeq;

public class ProcessingStage implements ProcessingStep {
    private final TableInfoFactory tableInfoFactory;
    private final List<Map<String, String>> partitionSpecs;
    private final Map<String, Partition> derivedPartitioningFields;
    private final PublisherDao dao;
    private Dataset<Row> dataset;
    private Dataset<Row> oldSnp;
    private SnapshotChanges changes;
    Map<String, String> readSnpPartitionGeqFilters;

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
        dataset = dataset
                .withColumn(CTL_VALIDFROM, lit(DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN)
                        .format(LocalDateTime.now())).cast(DataTypes.TimestampType))
                .withColumn(CTL_LOADING, lit(loadingId).cast(DataTypes.LongType));
        // VSPC-COMPAT begin: div_h ctl_validfrom := start_dt
        this.dataset = ru.sberbank.bigdata.dffess.vspc.compat.CompatHooks.enrichInit(this.dataset, tableInfoFactory);
        // VSPC-COMPAT end
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
        // VSPC-COMPAT begin: декоратор калькулятора, до фильтра зоны чтения
        changesCalculator = ru.sberbank.bigdata.dffess.vspc.compat.CompatHooks.wrapChanges(this, changesCalculator,
                dao, tableInfoFactory, derivedPartitioningFields);
        // VSPC-COMPAT end
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
        this.dataset = this.changes.getDiff();
        return this;
    }

    @Override
    public ProcessingStep calculateMergedSnp(SchemaName targetSchemaName, MergeCalculator mergeCalculator) {
        // VSPC-COMPAT begin: декоратор merge той же стадии
        mergeCalculator = ru.sberbank.bigdata.dffess.vspc.compat.CompatHooks.wrapMerge(this, mergeCalculator);
        // VSPC-COMPAT end
        this.dataset = mergeCalculator.calculate(oldSnp, changes, tableInfoFactory, dao, targetSchemaName, partitionSpecs);
        return this;
    }

    @Override
    public ProcessingStep readChangesFromStg(String tableNameSuffix) {
        TableInfo stgTableInfo = tableInfoFactory.createTmpTableInfo(STG, tableNameSuffix, partitionSpecs);
        Dataset<Row> diff = dao.readTable(stgTableInfo);
        this.changes = SnapshotChanges.datasetToSnapshotChanges(diff);
        return this;
    }
}