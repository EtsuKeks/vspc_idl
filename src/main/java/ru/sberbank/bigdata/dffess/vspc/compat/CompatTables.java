package ru.sberbank.bigdata.dffess.vspc.compat;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.metadata.domain.HistType;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Method;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Partition;
import ru.sberbank.bigdata.sl.rt.publisher.MergeMode;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfoFactory;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TablePairInfo;
import ru.sberbank.bigdata.sl.rt.publisher.validation.EntityInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.sectionBy;

// Что ESSA-поправкам нужно знать о публикуемой таблице. Описание строится и кладётся в реестр
// по имени таблицы: внутри публикации RT-FW наши вставки знают только имя таблицы и достают описание оттуда.
public final class CompatTables {

    // Историческая таблица (версии со start_dt/end_dt) или хаб (без дат версии).
    public enum Kind { PERIOD, HUB }

    // Загрузчик, которым ESSA грузила таблицу. Какой у какой таблицы — в constant/EssaLoaders.
    public enum EssaLoader {
        HUB_DIFF(Kind.HUB), HIST_DIFF(Kind.PERIOD), HIST_MIX(Kind.PERIOD), HIST_SNP(Kind.PERIOD);

        private final Kind kind;

        EssaLoader(Kind kind) {
            this.kind = kind;
        }
    }

    // Описание одной таблицы. Пример значений по таблице div_type из vspc_idl.
    public static final class Table {
        private final String code;              // div_type
        private final String snpTable;          // usl_vspc_idl_snp.div_type
        private final String tmpSchema;         // схема наших промежуточных таблиц: usl_vspc_idl_stgtmp
        private final EssaLoader loader;        // HIST_SNP
        private final List<String> versionKey;  // ключ версии: [div_sk, start_dt]
        private final String partitionColumn;   // партиция по end_dt: p1month, но бывают и p100day и пр.; у хаба null
        private final Partition partition;      // как RT-FW считает эту партицию из end_dt; у хаба null
        private String openPartition;           // партиция открытых версий, считается один раз при первом запросе
        private final String cacheTable;        // кэш закрытий HistSnp: usl_vspc_idl_stg.loader_cache_div_type; иначе
                                                // null, если loader != HIST_SNP
        private final String cacheDraft;        // его черновик этой загрузки: usl_vspc_idl_stgtmp.loader_cache_div_type

        private Table(String code, String snpTable, String tmpSchema, EssaLoader loader, List<String> versionKey,
                        String partitionColumn, Partition partition, String cacheTable, String cacheDraft) {
            this.code = code;
            this.snpTable = snpTable;
            this.tmpSchema = tmpSchema;
            this.loader = loader;
            this.versionKey = versionKey;
            this.partitionColumn = partitionColumn;
            this.partition = partition;
            this.cacheTable = cacheTable;
            this.cacheDraft = cacheDraft;
        }

        // Кэш закрытий загрузчика HistSnp и его черновик; у остальных загрузчиков null.
        public String cacheTable() { return cacheTable; }
        public String cacheDraft() { return cacheDraft; }

        public String code() { return code; }
        public String snpTable() { return snpTable; }

        // Полное имя таблицы в stgtmp по суффиксу:
        // tmpTable("_compat_open_snp") -> usl_vspc_idl_stgtmp.div_type_compat_open_snp.
        public String tmpTable(String suffix) { return tmpSchema + "." + code + suffix; }
        public Kind kind() { return loader.kind; }
        public boolean isPeriod() { return loader.kind == Kind.PERIOD; }

        // Ключ версии: у исторической таблицы — ключ сущности + start_dt, у хаба — просто ключ.
        public List<String> versionKey() { return versionKey; }

        // Ключ сущности — ключ версии без start_dt: [div_sk]. По нему собираются все версии одного объекта.
        public List<String> entityKey() {
            List<String> key = new ArrayList<>(versionKey);
            key.remove(START_DT_FIELD_NAME);
            return key;
        }

        // Только у исторической таблицы: колонка партиции по end_dt (p1month обычно).
        public String partitionColumn() {
            requirePeriod();
            return partitionColumn;
        }

        // Партиция открытых версий (end_dt = 9999-12-31): "999912".
        public String openPartition() {
            if (openPartition == null) {
                openPartition = partitionOf(PLUS_INFINITY_DT);
            }
            return openPartition;
        }

        // В какую партицию RT-FW положит строку с данной end_dt, например: "2026-10-15" → "202610".
        public String partitionOf(String endDt) {
            requirePeriod();
            Dataset<Row> row = SparkSession.active().range(1)
                .select(lit(endDt).cast(DataTypes.DateType).as(END_DT_FIELD_NAME));
            return sectionBy(row, Collections.singletonMap(partitionColumn, partition)).first().getAs(partitionColumn);
        }

        public boolean histSnp() { return loader == EssaLoader.HIST_SNP; }

        // ESSA некоторые таблицы грузила загрузчиком HistMix — в них в INIT ctl_validfrom берётся из start_dt.
        public boolean initVfFromStart() { return loader == EssaLoader.HIST_MIX; }

        private void requirePeriod() {
            if (!isPeriod()) {
                throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                    code + ": партиция по end_dt есть только у исторических таблиц"));
            }
        }
    }

    private static final Map<String, Table> BY_CODE = new ConcurrentHashMap<>();

    private CompatTables() {
    }

    // Строит описание таблицы набора mask и кладёт его в реестр. loader — загрузчик ESSA для этой таблицы
    // (из EssaLoaders); null — таблицы в ESSA не было, поправок к ней нет.
    public static Table register(App app, String mask, EssaLoader loader) {
        // Таблица набора в meta — так же, как её находит публикатор RT-FW.
        ru.sberbank.bigdata.sl.rt.metadata.domain.Table meta =
            TablePairInfo.fromConsumerList(app.ctx().metadata2().getConsumers(mask)).getSnpTable();
        String code = meta.getTableCode();

        // Наш загрузчик ESSA объявлен и того же типа, что таблица в meta.
        if (loader == null) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                mask + " (" + code + "): загрузчик ESSA для таблицы не объявлен — ESSA-поправки не применимы"));
        }
        Kind kind = meta.getHistoryType() == HistType.PERIOD 
                    ? Kind.PERIOD
                    : meta.getHistoryType() == HistType.NONE ? Kind.HUB : null;
        if (kind != loader.kind) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                code + ": тип истории в meta " + meta.getHistoryType() + ", а загрузчик ESSA " + loader));
        }

        // Имя таблицы и партиции — из meta тем же классом, что у публикатора RT-FW.
        EntityInfo ei = new EntityInfo(app.ctx().getLogEnv(), MergeMode.WITH_REWRITE, meta);
        String snpTable = new TableInfoFactory(ei).createTableInfo(SchemaName.SNP).getQualifiedTableName();
        String tmpSchema = new TableInfoFactory(ei).createTableInfo(SchemaName.STGTMP).getDatabaseName();
        Map<String, Partition> partitions = ei.getDerivedPartitioningFields();

        // Партиции. ESSA-поправки проверены на двух раскладках: у исторической таблицы одна партиция по дате end_dt
        // (p1month), у хаба партиций нет. RT-FW допускает и другие (несколько партиций, партиции по type_code) —
        // на них ошибка, чтобы не писать данные по непроверенному пути.
        String partitionColumn = null;
        Partition partition = null;
        if (kind == Kind.PERIOD) {
            if (partitions.size() == 1) {
                partitionColumn = partitions.keySet().iterator().next();
                partition = partitions.get(partitionColumn);
            }
            if (partition == null || partition.getMethod() != Method.BUSINESS_HISTORY
                    || !END_DT_FIELD_NAME.equals(partition.getOriginColumnCode())) {
                throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, 
                    code + ": партиции " + partitions + " — ESSA-поправки проверены только на партиции по end_dt"));
            }
        } else if (!partitions.isEmpty()) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, 
                code + ": партиции " + partitions + " — ESSA-поправки проверены только на хабах без партиций"));
        }

        // Кэш закрытий есть только у загрузчика HistSnp. Имя — по правилу ESSA: loader_cache_<таблица> в схеме stg, 
        // черновик — то же имя в stgtmp. Схемы — из RT-FW.
        String cacheTable = null;
        String cacheDraft = null;
        if (loader == EssaLoader.HIST_SNP) {
            TableInfoFactory tif = new TableInfoFactory(ei);
            String cacheName = "loader_cache_" + code;
            cacheTable = tif.createTableInfo(SchemaName.STG).getDatabaseName() + "." + cacheName;
            cacheDraft = tif.createTableInfo(SchemaName.STGTMP).getDatabaseName() + "." + cacheName;
        }

        Table t = new Table(code, snpTable, tmpSchema, loader,
            Collections.unmodifiableList(new ArrayList<>(ei.getPkFields())), partitionColumn, partition, cacheTable,
                cacheDraft);
        BY_CODE.put(code, t);
        return t;
    }

    // Описание, которое положил publishChecked. Нет — таблицу публикуют в обход publishChecked, поправки ESSA
    // не применятся, поэтому отказ.
    public static Table byCode(String code) {
        Table t = BY_CODE.get(code);
        if (t == null) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                code + ": публикация не через publishChecked, модель таблицы не зарегистрирована"));
        }
        return t;
    }
}
