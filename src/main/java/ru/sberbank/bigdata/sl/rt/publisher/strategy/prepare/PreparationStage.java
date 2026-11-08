package ru.sberbank.bigdata.sl.rt.publisher.strategy.prepare;

import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Column;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Partition;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Table;
import ru.sberbank.bigdata.sl.rt.metadata.model.RecordsetCode;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.ColumnTypeDiscrepancy;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfo;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfoFactory;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.ProcessingStage;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.ProcessingStep;
import ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils;
import ru.sberbank.bigdata.sl.rt.publisher.validation.ComplexValidator;
import ru.sberbank.bigdata.sl.rt.publisher.validation.EntityInfo;
import ru.sberbank.bigdata.sl.rt.util.RtfwLogger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_CSN;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_LOADING;
import static ru.sberbank.bigdata.sl.rt.core.Constants.INSTANCE_CODE;
import static ru.sberbank.bigdata.sl.rt.core.Constants.P_WO;
import static ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName.STGTMP;
import static ru.sberbank.bigdata.sl.rt.publisher.logger.PublisherLogMessageBuilder.PUB_006;
import static ru.sberbank.bigdata.sl.rt.publisher.logger.PublisherLogMessageBuilder.PUB_010;
import static ru.sberbank.bigdata.sl.rt.publisher.logger.PublisherLogMessageBuilder.PUB_038;
import static ru.sberbank.bigdata.sl.rt.publisher.logger.PublisherLogMessageBuilder.PUB_039;
import static ru.sberbank.bigdata.sl.rt.publisher.logger.PublisherLogMessageBuilder.PUB_044;
import static ru.sberbank.bigdata.sl.rt.publisher.logger.PublisherLogMessageBuilder.PUB_107;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.buildPartialPartitionSpecs;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.nonEmpty;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.scalaSeq;

/**
 * Shadow-копия vendor-класса rt-fw-publisher-2.16.1 (тот же пакет; замещение в fatjar —
 * shade-exclude в pom.xml, механика та же, что у PublicationStage/ChangesCalculators).
 * Подхватывается обеими точками создания в PublisherImpl (MERGE и SIMPLE стартовые цепочки).
 *
 * Единственное отличие от vendor (контракт ctl_validfrom, temp/ctl_validfrom_contract/):
 * dropTechFields() больше НЕ стирает принесённое производителем ctl_validfrom — до штампа
 * fill-if-absent в ProcessingStage.enrichTechFieldsForInit значение доходит. Остальное тело
 * класса байт-в-байт от vendor; создаваемый здесь ProcessingStage — тоже shadow-копия.
 */
public class PreparationStage implements
        ValidationStep, PreprocessingStep {

    private Dataset<Row> dataset;
    private final PublisherDao dao;
    private final String instanceCode;
    private final List<RecordsetCode> recordsetCodes;
    private final String pWo; // если != null, установлен пользователем; иначе используется переданный во входном наборе.
    private final EntityInfo entityInfo;
    private final TableInfoFactory tableInfoFactory;
    private final List<Map<String, String>> partialPartitionSpecs;

    public PreparationStage(Dataset<Row> dataset,
                            PublisherDao dao,
                            String instanceCode,
                            List<RecordsetCode> recordsetCodes,
                            String pWo,
                            EntityInfo entityInfo,
                            Table table) {
        this.dataset = dataset;
        this.dao = dao;
        this.instanceCode = instanceCode;
        this.recordsetCodes = recordsetCodes;
        this.pWo = pWo;
        this.entityInfo = entityInfo;
        this.tableInfoFactory = new TableInfoFactory(entityInfo);
        this.partialPartitionSpecs = ImmutableList.copyOf(buildPartialPartitionSpecs(
                instanceCode, recordsetCodes, pWo, table));
    }

    @Override
    public ValidationStep validateTargetTableExists(SchemaName schemaName) {
        TableInfo targetTableInfo = tableInfoFactory.createTableInfo(schemaName);
        if (!dao.tableExists(targetTableInfo)) {
            throw new TransformationException(PUB_039(targetTableInfo.getQualifiedTableName()));
        }
        return this;
    }

    @Override
    public ValidationStep validateTableFieldsWithMismatchedTypes(SchemaName schemaName, Table table) {
        // Здесь ожидается, что целевая таблица уже существует - проверяется первым шагом в цепочке
        // - метод validateTargetTableExists.
        TableInfo targetTableInfo = tableInfoFactory.createTableInfo(schemaName).
                enrichedWithCatalogInfo(dao);
        /* publisher−02 -> 2 соответствие типов данных полей одноименным полям таблицы-приемника */

        List<Column> metadataFields = table.getColumns();
        StructField[] catalogFields = targetTableInfo.getCatalogInfo().getSchema().fields();

        List<ColumnTypeDiscrepancy> targetTableTypesDiscrepanciesList =
                compareTableFieldTypesWithMetadataBeanTableColumnTypes(metadataFields, catalogFields);

        if (!targetTableTypesDiscrepanciesList.isEmpty()) {
            targetTableTypesDiscrepanciesList.forEach(e ->
                    RtfwLogger.log(PUB_006(targetTableInfo.getQualifiedTableName(),
                            e.getOriginalColumnName(),
                            e.getActualColumnType(),
                            e.getOriginalColumnType()))
            );

            ColumnTypeDiscrepancy firstDiscrepancy = targetTableTypesDiscrepanciesList.get(0);
            throw new TransformationException(PUB_006(targetTableInfo.getQualifiedTableName(),
                    firstDiscrepancy.getOriginalColumnName(),
                    firstDiscrepancy.getActualColumnType(),
                    firstDiscrepancy.getOriginalColumnType())
            );
        }
        /* publisher−02 -> 2 соответствие типов данных полей одноименным полям таблицы-приемника */

        return this;
    }

    @Override
    public ValidationStep dropDatasetFieldsWithMismatchedTypes(SchemaName schemaName, Table table) {
        // Здесь ожидается, что целевая таблица уже существует - проверяется первым шагом в цепочке
        // - метод validateTargetTableExists.
        TableInfo targetTableInfo = tableInfoFactory.createTableInfo(schemaName);
        /* publisher−02 -> 2 соответствие типов данных полей одноименным полям таблицы-приемника */

        List<Column> metadataFields = table.getColumns();
        StructField[] datasetFields = this.dataset.schema().fields();
        List<ColumnTypeDiscrepancy> inputDatasetTypesDiscrepanciesList =
                compareTableFieldTypesWithMetadataBeanTableColumnTypes(metadataFields, datasetFields);

        for (ColumnTypeDiscrepancy e : inputDatasetTypesDiscrepanciesList) {
            RtfwLogger.log(PUB_038(targetTableInfo.getQualifiedTableName(),
                    e.getOriginalColumnName(),
                    e.getActualColumnType(),
                    e.getOriginalColumnType()));
            this.dataset = this.dataset.drop(e.getOriginalColumnName());
        }

        /* publisher−02 -> 2 соответствие типов данных полей одноименным полям таблицы-приемника */

        return this;
    }

    @Override
    public ValidationStep validate(SchemaName schemaName, Table table) {
        // Здесь ожидается, что целевая таблица уже существует - проверяется первым шагом в цепочке
        // - метод validateTargetTableExists.
        TableInfo targetTableInfo = tableInfoFactory.createTableInfo(schemaName).
                enrichedWithCatalogInfo(dao);

        validateInput(dataset, table, targetTableInfo, instanceCode, recordsetCodes, pWo);

        return this;
    }

    @Override
    public ValidationStep dropDerivedPartitioningFields(Map<String, Partition> derivedPartitioningFields) {
        List<String> prohibitedDerivedPartitioningFields = Arrays.stream(dataset.columns())
                .filter(derivedPartitioningFields::containsKey)
                .collect(Collectors.toList());
        if (!prohibitedDerivedPartitioningFields.isEmpty()) {
            this.dataset = dataset.drop(scalaSeq(prohibitedDerivedPartitioningFields));
            RtfwLogger.log(PUB_107(prohibitedDerivedPartitioningFields));
        }
        return this;
    }

    @Override
    public PreprocessingStep dropAndRefillInstanceCodeIfMultiInstance() {
        if (nonEmpty(instanceCode)) {
            this.dataset = dataset
                    .drop(scalaSeq(INSTANCE_CODE))
                    .withColumn(INSTANCE_CODE, lit(instanceCode));
        }
        return this;
    }

    @Override
    public PreprocessingStep applyUserDefinedPWoIfSupplied() {
        // Расчёт поля секционирования p_wo, если его значение было передано константой согласно SA (publisher−07.1, подпункт 3.a)
        //  см. https://jira.delta.sbrf.ru/browse/USL-4465
        if (pWo != null) {
            RtfwLogger.log(PUB_044(pWo));
            this.dataset = dataset.withColumn(P_WO, lit(pWo));
        }
        return this;
    }

    @Override
    public PreprocessingStep castToTargetStructure(SchemaName schemaName, String... fieldsToIgnore) {
        // Здесь ожидается, что целевая таблица уже существует.
        StructType targetTableSchema = tableInfoFactory.createTableInfo(schemaName).
                enrichedWithCatalogInfo(dao).
                getCatalogInfo().
                getSchema();

        this.dataset = PublisherUtils.castToTargetStructure(dataset, targetTableSchema, fieldsToIgnore);
        return this;
    }

    @Override
    public PreprocessingStep dropTechFields() {
        // Контракт ctl_validfrom: CTL_VALIDFROM исключён из drop — принесённое производителем значение
        // доходит до fill-if-absent штампа (ProcessingStage.enrichTechFieldsForInit). Остальные тех-поля
        // ctl_loading/ctl_csn/ctl_do стираются как в vendor.
        this.dataset = dataset.drop(scalaSeq(CTL_LOADING, CTL_CSN, CTL_DO));
        return this;
    }

    @Override
    public PreprocessingStep saveToStgtmpNonPartitioned(SchemaName targetSchemaName, String tableNameSuffix) {
        TableInfo tableInfo = tableInfoFactory.createTmpTableInfo(STGTMP, tableNameSuffix, partialPartitionSpecs);
        dao.writeTable(tableInfo, scalaSeq(Collections.emptyList()), dataset, null);
        this.dataset = dao.readTable(tableInfo);
        return this;
    }

    @Override
    public ProcessingStep readyForProcessing() {
        return new ProcessingStage(dao, dataset, tableInfoFactory, partialPartitionSpecs,
                entityInfo.getDerivedPartitioningFields());
    }

    private static void validateInput(Dataset<Row> ds, Table table, TableInfo tableInfo,
                                      String instanceCode, List<RecordsetCode> rsCodes, String pWo) {
        List<LogMessage> validationResult = new ComplexValidator()
                .validate(ds, table, tableInfo, instanceCode, rsCodes, pWo);

        validationResult.forEach(RtfwLogger::log);

        if (validationResult.isEmpty()) {
            RtfwLogger.log(PUB_010(tableInfo, table));
        } else {
            throw new TransformationException(validationResult.get(0));
        }
    }

    List<ColumnTypeDiscrepancy> compareTableFieldTypesWithMetadataBeanTableColumnTypes(List<Column> metadataFields, StructField[] tableFields) {
        List<ColumnTypeDiscrepancy> typesDiscrepanciesList = new ArrayList<>();
        for (Column metadataColumn : metadataFields) {
            String metadataColumnName = metadataColumn.getColumnCode();
            for (StructField tableField : tableFields) {
                String catalogFieldName = tableField.name();
                if (metadataColumnName.equals(catalogFieldName)) {
                    String metadataColumnType = metadataColumn.getDataType();
                    String catalogFieldType = tableField.dataType().simpleString();
                    if (!metadataColumnType.equalsIgnoreCase(catalogFieldType)) {
                        typesDiscrepanciesList.add(new ColumnTypeDiscrepancy(metadataColumnName, catalogFieldType, metadataColumnType));
                    }
                    break;
                }
            }
        }
        return typesDiscrepanciesList;
    }
}
