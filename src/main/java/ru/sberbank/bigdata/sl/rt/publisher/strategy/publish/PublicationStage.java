package ru.sberbank.bigdata.sl.rt.publisher.strategy.publish;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import ru.sberbank.bigdata.sl.rt.core.Recordset;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfo;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfoFactory;
import ru.sberbank.bigdata.sl.rt.util.RtfwLogger;
import scala.Option;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName.STG;
import static ru.sberbank.bigdata.sl.rt.publisher.dto.TablePairInfo.getPairForSnpSchema;
import static ru.sberbank.bigdata.sl.rt.publisher.dto.TablePairInfo.isSnpSchema;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.SUFFIX_SNPBAK;

/**
 * Shadow-копия vendor-класса rt-fw-publisher-2.16.1 (та же сигнатура конструкторов и то же
 * публичное поведение; замещается в fatjar через shade-exclude в pom.xml — механика та же,
 * что у работающих shadow-калькуляторов strategy/process/changes/*).
 *
 * ЕДИНСТВЕННОЕ отличие от vendor: в moveToTargetTable() для инкрементальной публикации
 * историчных таблиц перенос из stg в target ограничивается фактически затронутыми партициями
 * (вариант B исследования): скоуп = {значение секции >= порога из readSnpPartitionGeqFilters,
 * строковое сравнение как в PublisherDao.matchHfdsLocation} ∪ {секция '999912'}, спека строится
 * по ФАКТИЧЕСКИМ партициям stg-таблицы из metastore. Все прочие пути (INIT, REINIT-snp/diff,
 * INCR-diff, простые публикации, не-hist таблицы, непустая базовая спека, любой сбой
 * перечисления партиций) ведут себя байт-в-байт как vendor: пустая спека = переносятся все
 * партиции.
 * Обоснование/доказательства: ~/Desktop/ess/temp/audit_v2/23_rewrite_scope_fix_research.md
 */
public class PublicationStage implements
        PublicationStep, DropTmpTableStep, ReportUpdatedBusinessDatesStep, ReportChangedSetsStep {
    private final List<Map<String, String>> partitionSpecs;
    private final PublisherDao dao;
    private final TableInfo targetTableInfo;
    private final TableInfo stgTableInfo;
    private final TableInfo diffTableInfo;
    private Map<String, String> readSnpPartitionGeqFilters;

    // Максимальное значение производной секции бизнес-истории (p1month) — «бесконечная» партиция
    // открытых версий. Лексикографически больше любого месяца yyyyMM, поэтому всегда покрывается
    // бэкапом (matchHfdsLocation: значение >= порога); в скоуп переноса включается явно.
    private static final String MAX_DATE_SECTION_VALUE = "999912";

    public PublicationStage(PublisherDao dao,
                            TableInfoFactory tableInfoFactory,
                            List<Map<String, String>> partitionSpecs,
                            SchemaName targetSchemaName,
                            String stgTableNameSuffix) {
        this.dao = dao;
        this.partitionSpecs = partitionSpecs;

        this.stgTableInfo = tableInfoFactory.createTmpTableInfo(STG, stgTableNameSuffix, partitionSpecs);
        this.targetTableInfo = tableInfoFactory.createTableInfo(targetSchemaName);
        this.diffTableInfo = tableInfoFactory.createTableInfo(getPairForSnpSchema(targetSchemaName));
        this.readSnpPartitionGeqFilters = Collections.emptyMap();
    }

    public PublicationStage(PublisherDao dao,
                            TableInfoFactory tableInfoFactory,
                            List<Map<String, String>> partitionSpecs,
                            SchemaName targetSchemaName,
                            String stgTableNameSuffix,
                            Map<String, String> readSnpPartitionGeqFilters) {
        this(dao, tableInfoFactory, partitionSpecs, targetSchemaName, stgTableNameSuffix);
        // Нормализация входа: вызывающая сторона (ProcessingStage:158) передаёт поле, которое
        // заполняется только в calculateDiff (ProcessingStage:185; INIT/REINIT — null). null трактуется как пустая карта:
        // ограничения партиций не применяются (поведение vendor на этих путях).
        this.readSnpPartitionGeqFilters = readSnpPartitionGeqFilters == null
                ? Collections.emptyMap()
                : readSnpPartitionGeqFilters;
    }

    @Override
    public PublicationStep clearTargetTable() {
        //  publisher−18
        //  > Модуль должен обеспечить журналирование событий аудита С5 и С6 (при очистке снимка и
        //  > difference data согласно publisher−11) в соответствии с требованиями КАП.
        dao.clearTableWithLogAudit(targetTableInfo, partitionSpecs, !targetTableInfo.isManSchema());
        return this;
    }

    public PublicationStep clearDiffTable() {
        //  publisher−18
        //  > Модуль должен обеспечить журналирование событий аудита С5 и С6 (при очистке снимка и
        //  > difference data согласно publisher−11) в соответствии с требованиями КАП.

        assert(isSnpSchema(targetTableInfo.getSchemaName()));
        dao.clearTableWithLogAudit(diffTableInfo, partitionSpecs, !diffTableInfo.isManSchema());
        return this;
    }

    @Override
    public PublicationStep clearTargetByMovingToBackup() {
        // Очистка обновляемых партиций таргета за счет перемещения этих партиций по резервному пути,
        // вычисляемому как <путь_к_директории_stg_таблицы>_snpbak, например, /data/usl/rtfw_idl/stg/toptn_snpbak

        // Поскольку в случае неисторичных таблиц снимок замещается полностью, то и риск при возникновении ошибок
        // в работе = потеря всего снимка.
        // Поэтому перед замещением снимка в "старый" снимок перекладывается из целевой области в отдельный каталог
        // внутри stg.
        // Это довольно лёгкая операция, поскольку производится на уровне файловой системы HDFS. При этом остаётся
        // возможность выполнить "ручное" восстановление данных в случае сбоев.
        // Автоматического восстановления на данный момент не предусмотрено; полноценный механизм
        //  резервирования/восстановления будет прорабатываться в рамках https://jira.delta.sbrf.ru/browse/USL-2847

        assert(isSnpSchema(targetTableInfo.getSchemaName()));

        String backupPath = stgTableInfo.getAutoHdfsPath(dao) + SUFFIX_SNPBAK;
        dao.moveToBackupHdfsLocation(targetTableInfo.getAutoHdfsPath(dao), backupPath, partitionSpecs, readSnpPartitionGeqFilters);
        return this;
    }

    @Override
    public PublicationStep getUpdatedBusinessDates() {
        // TODO: USL-2715
        /* Todo: подумать, какие параметры стоит заранее заложить
            Если таблица партиционирована по p1day, то кажется все проще, а если
            нет - то, возможно, стоило бы подумать, как упростить алгоритм получения
            обновляемых дат */
        if (targetTableInfo.isManSchema()) {
            // Если man-каталог - пропускаем шаг
            return this;
        }
        return this;
    }

    @Override
    public PublicationStep reportUnavailabilityFlag() {
        // TODO: USL-2757
        //  publisher−12
        //  > Перед началом обновления снимка и difference data публикуемой таблицы-приемника, модуль должен инициировать регистрацию события недоступности в соответствии с описанием метода 2.4 на странице SA CAP process flow control service.
        if (targetTableInfo.isManSchema()) {
            //  > Это требование действует только при обновлении публикуемой области data-продуктов ЕСС (не требуется для man-каталогов).
            return this;
        }
        return this;
    }

    @Override
    public DropTmpTableStep moveToTargetTable() {
        // TODO [Post-MVP]: отправка сообщения о недоступности в КАП.МЕТА
        //  publisher−13 - https://jira.delta.sbrf.ru/browse/USL-2757
        //  > Перед началом обновления снимка и difference data публикуемой таблицы-приемника, модуль должен
        //  > инициировать регистрацию события недоступности в соответствии с описанием метода 2.4
        //  > на странице SA CAP process flow control service.
        // vspc-3.0 parity (окно переноса): единственное изменённое место относительно vendor —
        // вместо постоянного поля partitionSpecs передаётся restrictSnpSpecs(partitionSpecs),
        // которое для гейтов Г1-Г3 возвращает ограниченную спеку (вариант B), иначе — исходную.
        dao.moveToTarget(stgTableInfo, targetTableInfo, restrictSnpSpecs(partitionSpecs));
        return this;
    }

    /**
     * Ограничение скоупа переноса партиций stg -> target (вариант B исследования 23):
     * из фактических партиций stg-таблицы отбираются те, чьё значение секции удовлетворяет
     * geq-порогу калькулятора изменений (строковое сравнение >=, дословно как в
     * PublisherDao.matchHfdsLocation) либо равно максимальному значению секции ('999912').
     *
     * Гейты (невыполнение любого => возвращается base без изменений = поведение vendor):
     * Г0 — базовая спека уже непуста (спецированные пути не трогаем);
     * Г1 — публикация идёт в SNP-схему (DIFF/man — как в vendor);
     * Г2 — карта geq-порогов непуста (её заполняет только calculateDiff hist-калькулятора на INCR);
     * Г3 — ключи порогов являются реальными колонками секционирования целевой таблицы
     *      (структурный признак историчной таблицы).
     */
    private List<Map<String, String>> restrictSnpSpecs(List<Map<String, String>> base) {
        // Г0: спецированные пути (непустая базовая спека) — поведение байт-в-байт как в vendor.
        if (!base.isEmpty()) {
            return base;
        }
        // Г1: перенос ограничиваем только публикацией в SNP-схему.
        if (!isSnpSchema(targetTableInfo.getSchemaName())) {
            return base;
        }
        // Г2: только hist-with-rewrite путь INCR — пороги выставляет calculateDiff hist-калькулятора;
        // на INIT/REINIT-snp/simple путях карта пуста.
        if (readSnpPartitionGeqFilters == null || readSnpPartitionGeqFilters.isEmpty()) {
            return base;
        }
        // Г3: пороги должны быть реальными колонками секционирования целевой таблицы;
        // у не-hist таблиц такой колонки нет — фолбэк к vendor-поведению.
        Optional<CatalogTable> targetCatalogTable = dao.getCatalogTableOptional(targetTableInfo);
        if (!targetCatalogTable.isPresent()) {
            return base;
        }
        List<String> targetPartitionColumns = JavaConverters.seqAsJavaListConverter(
                targetCatalogTable.get().partitionColumnNames()).asJava();
        if (!targetPartitionColumns.containsAll(readSnpPartitionGeqFilters.keySet())) {
            return base;
        }

        try {
            CatalogTable stgCatalogTable = dao.getCatalogTableOptional(stgTableInfo).get();
            if (stgCatalogTable.partitionSchema().isEmpty()) {
                return base;
            }
            SessionCatalog sessionCatalog = SparkSession.active().sessionState().catalog();
            List<CatalogTablePartition> stgPartitions = JavaConverters.seqAsJavaListConverter(
                    sessionCatalog.listPartitions(stgCatalogTable.identifier(), Option.empty())).asJava();
            List<Map<String, String>> restricted = new ArrayList<>();
            for (CatalogTablePartition partition : stgPartitions) {
                Map<String, String> spec = JavaConverters.mapAsJavaMapConverter(partition.spec()).asJava();
                boolean affected = readSnpPartitionGeqFilters.entrySet().stream().anyMatch(geq -> {
                    String value = spec.get(geq.getKey());
                    return value != null && (value.compareTo(geq.getValue()) >= 0
                            || value.equals(MAX_DATE_SECTION_VALUE));
                });
                if (affected) {
                    restricted.add(spec);
                }
            }
            if (restricted.isEmpty()) {
                // Ничего не отобрано — возврат к vendor-поведению: пустая спека = перенос всех партиций.
                return base;
            }
            RtfwLogger.log(new LogMessage("", LogType.TECH_INFO, format(
                    "Скоуп переноса партиций %s -> %s ограничен: к переносу отобрано %d из %d партиций stg (фильтры секций: %s)",
                    stgTableInfo.getQualifiedTableName(),
                    targetTableInfo.getQualifiedTableName(),
                    restricted.size(),
                    stgPartitions.size(),
                    readSnpPartitionGeqFilters)));
            return restricted;
        } catch (Exception e) {
            // Любой сбой перечисления партиций — откат к vendor-поведению (все партиции):
            // перенос не должен терять данные и быть менее надёжным, чем vendor-путь.
            RtfwLogger.log(new LogMessage("", LogType.TECH_ERROR, format(
                    "Не удалось определить фактические партиции %s — скоуп переноса НЕ ограничен (%s)",
                    stgTableInfo.getQualifiedTableName(), e.getMessage())));
            return base;
        }
    }

    @Override
    public ReportUpdatedBusinessDatesStep dropStgTable() {
        // Очищаем stg - поскольку данные оттуда были перемещены, нет смысла хранить.
        dao.dropTableWithData(stgTableInfo);
        return this;
    }

    @Override
    public ReportChangedSetsStep reportUpdatedBusinessDates() {
        // TODO: USL-2715
        //  publisher−14
        //  > В режиме инкрементальной загрузки, модуль должен рассчитывать и передавать модулю rt-fw-core список затронутых обновлением бизнес-дат в соответствии с алгоритмом, описанным на странице SA USL data certification.
        //  > Список должен быть общим для всех ПЕ, составляющих data-продукт, поскольку с т. з. функции сертификации data-продукт неделим (определение ПЕ см. в разделе 1.8 страницы SA CAP process flow control service).
        if (targetTableInfo.isManSchema()) {
            //  > Это требование действует только при обновлении публикуемой области data-продуктов ЕСС (не требуется для man-каталогов)..
            return this;
        }
        return this;
    }

    @Override
    public void reportChangedSets(Recordset... sets) {
        // TODO: USL-4721
    }
}
