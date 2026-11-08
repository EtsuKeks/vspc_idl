package ru.sberbank.bigdata.sl.rt.publisher.strategy.process.changes;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Partition;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SnapshotChanges;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfoFactory;
import ru.sberbank.bigdata.sl.rt.util.RtfwLogger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.xxhash64;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION_DELETE;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION_INSERT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION_UPDATE;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO_REDO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO_UNDO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_LOADING;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_VALIDFROM;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.INCR_ALIAS;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.INCR_PREFIX;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.INCR_PREFIX_ALL;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.INNER;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.LEFT;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.OLD_ALIAS;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.OLD_PREFIX;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.OLD_PREFIX_ALL;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.castDataset;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.joinByArray;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.scalaSeq;

/**
 * Shadow-копия vendor-класса rt-fw-publisher-2.16.1 (тот же пакет; замещение в fatjar —
 * shade-exclude в pom.xml, механика та же, что у PublicationStage/ProcessingStage/
 * PreparationStage/HistoricalPeriodWithRewriteChangesCalculator). Переписан под паритет с ESSA 3.0
 * для неисторичных хабов витрины.
 *
 * <p>В 3.0 diff для неисторичных хабов собирал {@code DataLoaderHubDiff}: undo-ноги изменённых строк
 * переносили из снапа КАК ЕСТЬ, и {@code DataLoaderHubDiff.prepareTargetDiff} не перезаписывал у них
 * {@code ctl_validfrom} — в diff оставалась метка старой версии. Стоковый паблишер обогащает все ноги
 * разом ({@code enrichedWithTechnicalFields(loadingId)} → {@code ctl_validfrom = LocalDateTime.now()}),
 * из-за чего undo-ноги теряли метку версии, которую они откатывают.
 *
 * <p>Здесь обогащение расщеплено: redo-ноги и ноги вставок/удалений получают NOW загрузки
 * (как в 3.0), undo-ноги изменённых строк сохраняют {@code ctl_validfrom} старой версии и получают
 * {@code ctl_loading} текущей загрузки (тоже как в 3.0: prepareTargetDiff ставил loadingId на все ноги).
 *
 * <p>FX11: признак «атрибуты изменились» считается по хэшу канона ESSA 3.0
 * ({@code xxhash64(concat(...))} без разделителя, {@code SnpDataLoader.java:116-133}), а не по
 * вендорному {@code md5(concat_ws("|", ...))} ({@code AbstractChangesCalculator.java:126-139}).
 *
 * <p>FX16: для логически удалённой строки (deleted_flag = true в инкременте) redo-нога
 * (CTL_DO=R, CTL_ACTION=U) формируется из payload'а СТАРОГО снапа с deleted_flag = true —
 * как в 3.0 ({@code DataLoaderHubDiff.java:81-88}, {@code :93}, {@code :145-150}), а не из
 * payload'а инкремента (вендор {@code AbstractChangesCalculator.java:158-163}). От payload'а ноги
 * зависит и содержимое diff-таблицы, и содержимое снапа (merge применяет redo-ноги,
 * {@code GeneralMergeCalculator.java:45-50}, {@code :79-100}).
 */
public class NonHistoricalChangesCalculator extends AbstractChangesCalculator {
    protected final boolean physicalDeletion;
    protected final boolean fullSlice;

    /**
     * vspc-3.0 parity (FX11.1): служебные поля 3.0, не участвующие в хэше
     * ({@code SnpDataLoader.ADDITIONAL_FIELDS}, {@code SnpDataLoader.java:39}).
     */
    private static final List<String> ADDITIONAL_FIELDS_3_0 = Collections.unmodifiableList(Arrays.asList(
            "rn", "record_status", "hash", "string_hash",
            "another_del_flag", "another_ctl_loading", "another_ctl_validfrom"));

    /**
     * vspc-3.0 parity (FX11.2): {@code transformConfiguration.getExcludeHashColumns()} для хабов витрины
     * в 3.0 — ПУСТОЙ набор: {@code VspcMetadataKeeper.java:18,20,23,24} регистрирует
     * {@code ADDR}, {@code DIV}, {@code TERRDIVTYPE}, {@code TIMEZONE} (и промежуточный {@code CHG_DIVH},
     * {@code VspcMetadataKeeper.java:17}) через {@code addTable(table, primaryKey)} без excludeHashColumns
     * ⇒ {@code TableMetadata} подставляет {@code Collections.emptyList()}
     * ({@code TableMetadata.java:22-24}). Поэтому в 4.0 из хэша не исключается НИЧЕГО, кроме {@code ctl*}
     * и {@code ADDITIONAL_FIELDS} — в отличие от вендора, который дополнительно исключал pk,
     * производные поля секционирования и {@code system__*}.
     */
    private static final List<String> EXCLUDE_HASH_COLUMNS_3_0 = Collections.emptyList();

    public NonHistoricalChangesCalculator(PublisherDao dao,
                                          List<String> pkName,
                                          boolean fullSlice,
                                          boolean physicalDeletion,
                                          int loadingId,
                                          TableInfoFactory tableInfoFactory,
                                          Map<String, Partition> derivedPartitioningFields) {
        super(dao, pkName, loadingId, tableInfoFactory, derivedPartitioningFields);
        this.physicalDeletion = physicalDeletion;
        this.fullSlice = fullSlice;
    }

    @Override
    public Map<String, String> getMinBusinessDateUpdateFilters() {
        return Collections.emptyMap(); // нет фильтра на бизнес-историю
    }

    @Override
    protected SnapshotChanges calculateImpl(Dataset<Row> inc,
                                            Dataset<Row> snp,
                                            List<Map<String, String>> partitionSpecs) {
        return calculateDiff2Datasets(inc, snp, fullSlice, physicalDeletion, partitionSpecs);
    }

    @Override
    public SnapshotChanges calculate(Dataset<Row> incr,
                                     Dataset<Row> targetSnapshot,
                                     List<Map<String, String>> partitionSpecs) {
        SnapshotChanges changes = calculateImpl(incr, targetSnapshot, partitionSpecs).addInsertDeleteRedo();

        Dataset<Row> updates = changes.getUpdates();
        if (!Arrays.asList(updates.columns()).contains(CTL_VALIDFROM)) {
            return changes.enrichedWithTechnicalFields(loadingId, LocalDateTime.now());
        }

        LocalDateTime ctlValidfrom = LocalDateTime.now();
        Dataset<Row> updatesRedo = SnapshotChanges.enrichedWithTechnicalFields(
                updates.filter(col(CTL_DO).equalTo(lit(CTL_DO_REDO))), loadingId, ctlValidfrom);
        Dataset<Row> updatesUndo = updates
                .filter(col(CTL_DO).equalTo(lit(CTL_DO_UNDO)))
                .withColumn(CTL_LOADING, lit(loadingId).cast(DataTypes.LongType));

        RtfwLogger.log(new LogMessage("", LogType.TECH_INFO,
                "Undo-ноги изменённых строк сохраняют ctl_validfrom старой версии (parity с ESSA 3.0)"));

        return new SnapshotChanges(
                SnapshotChanges.enrichedWithTechnicalFields(changes.getInserts(), loadingId, ctlValidfrom),
                updatesRedo.unionByName(updatesUndo),
                SnapshotChanges.enrichedWithTechnicalFields(changes.getDeletes(), loadingId, ctlValidfrom));
    }

    /**
     * vspc-3.0 parity (FX11.1): переопределение вендорного метода
     * {@code AbstractChangesCalculator.calculateDiff2Datasets}
     * ({@code vendor/rt-fw-publisher-2.16.1-src/…/AbstractChangesCalculator.java:110-212}).
     *
     * <p>Метод в вендоре {@code protected}, не {@code final}, не {@code private}, не {@code static}
     * ⇒ переопределение допустимо; вызов из {@code calculateImpl} виртуальный (через {@code this}) ⇒
     * переопределение перехватывает и ветку INCR, и ветку INIT-среза.
     *
     * <p>Изменена только функция хэша: вендор считал {@code md5(concat_ws("|", attrCols))}
     * ({@code AbstractChangesCalculator.java:138-139}), канон ESSA 3.0 —
     * {@code xxhash64(concat(...))} БЕЗ разделителя, NULL → {@code ""}
     * ({@code SnpDataLoader.getHashColumn/getStringHash}, {@code SnpDataLoader.java:116-133}).
     * Разделитель {@code "|"} различает «ab»+«c» и «a»+«bc», тогда как 3.0 (склейка без разделителя)
     * считает их равными ⇒ 4.0 трактует часть изменений как NOTEQUALS вместо EQUALS ⇒ разный состав
     * ног в diff-таблице и разный состав строк в снапе.
     */
    @Override
    protected SnapshotChanges calculateDiff2Datasets(Dataset<Row> incr,
                                                     Dataset<Row> target,
                                                     boolean calculateDeletes,
                                                     boolean physicalDeletion,
                                                     List<Map<String, String>> partitionSpecs) {
        final StructType tgtSchema = target.schema();
        final String HASH_COL = "tech_cv_md5";
        final String UPD_FLG = "_tmp_upd_flg_ch";
        final String UPD_ALIAS = "_tmp_upd";

        // 3 варианта: не вычислять удаления; вычислять физические удаления; вычислять логические удаления
        assert (calculateDeletes || !physicalDeletion);
        // если вычисляем логические удаления, то в схеме должно быть поле deleted_flag
        assert (!calculateDeletes || physicalDeletion || Arrays.asList(tgtSchema.fieldNames()).contains(DELETED_FLAG));

        // vspc-3.0 parity (FX11.3): на INIT-загрузке 3.0 diff для хабов НЕ формируется.
        // DataLoaderHubDiff.fillTargetDatasets (DataLoaderHubDiff.java:103-111): на INIT берётся только
        // снап (= дифф источника без удалённых), targetDiff остаётся null ⇒ SnpDataLoader.loadData
        // (:53-56) пересоздаёт diff-таблицу пустой и (:63-67) не пишет в неё ни строки.
        // Признак ветки INIT у этого калькулятора — physicalDeletion=true: PublisherImpl:273-274
        // (ветка INITIAL) передаёт physicalDeletion=true, ветка INCR (:290-291) — false;
        // CalculatorFactory:44-46 (NONE_FULL_SLICE) прокидывает признак в конструктор.
        // При штатной конфигурации (init_diff_flag=false ⇒ diffClearedFlag=true ⇒ runInit,
        // PublisherImpl:261-262) калькулятор вообще не вызывается — 3.0-паритет достигается без правки.
        if (physicalDeletion) {
            Dataset<Row> emptyDiff = target.limit(0)
                    .withColumn(CTL_DO, lit(CTL_DO_REDO))
                    .withColumn(CTL_ACTION, lit(CTL_ACTION_INSERT));
            RtfwLogger.log(new LogMessage("", LogType.TECH_INFO,
                    "INIT: diff неисторичной таблицы пуст (parity с ESSA 3.0: DataLoaderHubDiff.java:103-111)"));
            return new SnapshotChanges(emptyDiff, emptyDiff, emptyDiff);
        }

        // vspc-3.0 parity (FX11.1/FX11.2): набор и порядок полей хэша — канон ESSA 3.0
        // (SnpDataLoader.getStringHash, SnpDataLoader.java:120-133): ВСЕ поля набора, кроме
        //   - начинающихся с "ctl" (правило field.startsWith("ctl"), :127);
        //   - transformConfiguration.getExcludeHashColumns() — для хабов витрины в 3.0 это ПУСТОЙ набор
        //     (VspcMetadataKeeper.java:17,18,20,23,24 → TableMetadata.java:22-24);
        //   - ADDITIONAL_FIELDS 3.0 (:39).
        // Порядок полей — порядок колонок инкремента (= порядок схемы целевого снапа: инкремент
        // приведён к структуре цели в PreparationStage:204-213 (вызов castToTargetStructure :211),
    // служебные ctl_* выкинуты в :216-222) ⇒
        // порядок совпадает с порядком полей 3.0 (matchSchemas с прототипом схемы снапа,
        // DataLoaderHubDiff.java:49-58). Поля, отсутствующие в снапе, из хэша исключаются: выражение
        // хэша применяется к обоим наборам.
        List<String> tgtColumns = Arrays.asList(target.columns());
        String[] attrToHash = Arrays.stream(incr.columns())
                .filter(c -> tgtColumns.contains(c)
                        && !c.startsWith("ctl")
                        && !ADDITIONAL_FIELDS_3_0.contains(c)
                        && !EXCLUDE_HASH_COLUMNS_3_0.contains(c))
                .toArray(String[]::new);
        Column[] attrCols = Arrays.stream(attrToHash)
                .map(attr -> when(col(attr).isNull(), lit("")).otherwise(col(attr).cast(DataTypes.StringType)))
                .toArray(Column[]::new);
        // vspc-3.0 parity (FX11.1): xxhash64(concat(...)) БЕЗ разделителя — канон 3.0
        // (SnpDataLoader.getHashColumn, SnpDataLoader.java:116-117); NULL → "" (:128)
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
        // vspc-3.0 parity (FX16): логически удалённая строка (deleted_flag = true в инкременте)
        // в 3.0 получает redo-ногу (CTL_DO=R, CTL_ACTION=U) со СТАРЫМ payload'ом снапа и
        // deleted_flag = true: DataLoaderHubDiff.java:81-88 (prevStateOfDel — строка СТАРОГО снапа,
        // RN=N2, another_del_flag = deleted_flag инкремента), :93 (снап получает старый payload
        // с deleted_flag = true), :145-150 (в diff эта же строка идёт redo-ногой).
        // Вендор 4.0 брал payload ИНКРЕМЕНТА (AbstractChangesCalculator.java:158-163), из-за чего
        // и diff-таблица, и снап (merge применяет только redo-ноги — GeneralMergeCalculator.java:45-50,
        // :79-100) содержали payload инкремента вместо старого.
        // Прочие redo-ноги (обычное изменение) — без изменений: payload инкремента.
        boolean hasDeletedFlag = Arrays.asList(tgtSchema.fieldNames()).contains(DELETED_FLAG);

        Dataset<Row> updChanged = upd.as(UPD_ALIAS).filter(col(UPD_FLG).equalTo(lit(1)));

        Dataset<Row> updateMappedRU = castDataset(
                updChanged
                        .join(incr.as(INCR_ALIAS), joinByArray(UPD_ALIAS, INCR_ALIAS, pkName), INNER)
                        .filter(hasDeletedFlag
                                ? actualRow(col(INCR_PREFIX + DELETED_FLAG))
                                : lit(true).cast(DataTypes.BooleanType))
                        .select(INCR_PREFIX_ALL), tgtSchema)
                .withColumn(CTL_DO, lit(CTL_DO_REDO))
                .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE))
                .unionByName( // операции UNDO
                        castDataset(
                                updChanged
                                        .join(target.as(OLD_ALIAS), joinByArray(UPD_ALIAS, OLD_ALIAS, pkName), INNER)
                                        .select(OLD_PREFIX_ALL), tgtSchema)
                                .withColumn(CTL_DO, lit(CTL_DO_UNDO))
                                .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE)));

        if (hasDeletedFlag) {
            // vspc-3.0 parity (FX16): redo-нога логического удаления — payload СТАРОГО снапа
            // с deleted_flag = true (3.0: DataLoaderHubDiff.java:81-88, :93, :145-150).
            Dataset<Row> logicalDeleteRedo = castDataset(
                    updChanged
                            .join(incr.as(INCR_ALIAS), joinByArray(UPD_ALIAS, INCR_ALIAS, pkName), INNER)
                            .filter(col(INCR_PREFIX + DELETED_FLAG).equalTo(lit(true)))
                            .join(target.as(OLD_ALIAS), joinByArray(UPD_ALIAS, OLD_ALIAS, pkName), INNER)
                            .select(OLD_PREFIX_ALL), tgtSchema)
                    .withColumn(DELETED_FLAG, lit(true))
                    .withColumn(CTL_DO, lit(CTL_DO_REDO))
                    .withColumn(CTL_ACTION, lit(CTL_ACTION_UPDATE));
            updateMappedRU = updateMappedRU.unionByName(logicalDeleteRedo);
        }
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
        // vspc-3.0 parity (FX11.3): в этом классе ветка недостижима — physicalDeletion=true означает
        // INIT-ветку (см. выше), где 3.0 diff не формирует вовсе, а на INCR calculateDeletes=false.
        Dataset<Row> deleteMapped = castDataset(target.as(OLD_ALIAS)
                .join(upd.as(UPD_ALIAS), joinByArray(UPD_ALIAS, OLD_ALIAS, pkName), LEFT)
                .filter(col(UPD_ALIAS + "." + pkName.get(0)).isNull())
                .select(OLD_PREFIX_ALL), tgtSchema)
                .withColumn(CTL_DO, lit(CTL_DO_REDO))
                .withColumn(CTL_ACTION, lit(CTL_ACTION_DELETE));
        deleteMapped = landStgTmpDataset(deleteMapped, partitionSpecs, "_pk_del_phy");

        return new SnapshotChanges(insertMapped, updateMappedRU, deleteMapped);
    }
}
