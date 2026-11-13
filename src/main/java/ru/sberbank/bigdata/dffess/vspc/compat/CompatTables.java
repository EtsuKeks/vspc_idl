package ru.sberbank.bigdata.dffess.vspc.compat;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.metadata.domain.HistType;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Method;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Partition;
import ru.sberbank.bigdata.sl.rt.publisher.MergeMode;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SectionBy;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfoFactory;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TablePairInfo;
import ru.sberbank.bigdata.sl.rt.publisher.validation.EntityInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.fail;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

/**
 * Таблицы витрины для поправок ESSA. Структура таблицы (код, тип, ключ, колонки, секция) — из meta RT-FW ровно так, как её
 * видит публикатор (TablePairInfo/EntityInfo, V/PublisherImpl.java:218-224), порядок колонок хэша — из каталога, как в ESSA.
 * Чего в meta нет — какой загрузчик ESSA вёл таблицу — передаёт продукт (его константы), см. register.
 */
public final class CompatTables {

    public enum Kind { PERIOD, HUB }

    /** Загрузчик ESSA, который вёл таблицу (у продукта: tables/&lt;t&gt;/&lt;T&gt;Configuration.getDataLoader()). */
    public enum EssaLoader {
        HUB_DIFF(Kind.HUB), HIST_DIFF(Kind.PERIOD), HIST_MIX(Kind.PERIOD), HIST_SNP(Kind.PERIOD), LINK_HIST_DIFF(Kind.PERIOD);

        private final Kind kind;

        EssaLoader(Kind kind) {
            this.kind = kind;
        }
    }

    public static final class Table {
        private final String code;
        private final String snpTable;
        private final EssaLoader loader;
        private final List<String> pk;
        private final List<String> columns;
        private final String p1month;
        private final Partition p1Partition;
        private final String openPartition;

        private Table(String code, String snpTable, EssaLoader loader, List<String> pk, List<String> columns,
                      String p1month, Partition p1Partition, String openPartition) {
            this.code = code;
            this.snpTable = snpTable;
            this.loader = loader;
            this.pk = pk;
            this.columns = columns;
            this.p1month = p1month;
            this.p1Partition = p1Partition;
            this.openPartition = openPartition;
        }

        public String code() { return code; }
        /** Полное имя snp-таблицы (схема — как у публикатора). */
        public String snpTable() { return snpTable; }
        public Kind kind() { return loader.kind; }
        public boolean isPeriod() { return loader.kind == Kind.PERIOD; }
        public List<String> pk() { return pk; }

        /** PK без start_dt (для хабов = PK). */
        public List<String> bk() {
            List<String> bk = new ArrayList<>(pk);
            bk.remove(START_DT_FIELD_NAME);
            return bk;
        }

        public Column bkNotNull() {
            return bk().stream().map(c -> col(c).isNotNull()).reduce(Column::and).get();
        }

        /** PERIOD: секция по end_dt (p1month) и её значение для открытых версий (end_dt = 9999-12-31). */
        public String p1month() { return requirePeriod(p1month); }
        public Partition p1Partition() { return requirePeriod(p1Partition); }
        public String openPartition() { return requirePeriod(openPartition); }

        /** div_type — в ESSA DataLoaderHistSnp: VANISH, правило возвращенца, маркер пустого INCR. */
        public boolean histSnp() { return loader == EssaLoader.HIST_SNP; }
        /** div_h: INIT ctl_validfrom := start_dt (DataLoaderHistMix). */
        public boolean initVfFromStart() { return loader == EssaLoader.HIST_MIX; }

        /** HUB_HASH_ESSA (E/slave/loading/SnpDataLoader.java:116-133, O/impl/VspcMetadataKeeper.java:19-25): xxhash64(concat) колонок snp в порядке каталога, NULL -> '', у period без start_dt/end_dt. */
        public Column essaHash() {
            return xxhash64(concat(columns.stream()
                    .filter(c -> !isPeriod() || !(START_DT_FIELD_NAME.equals(c) || END_DT_FIELD_NAME.equals(c)))
                    .map(c -> when(col(c).isNotNull(), col(c).cast(StringType)).otherwise(lit("")))
                    .toArray(Column[]::new)));
        }

        private <T> T requirePeriod(T value) {
            if (!isPeriod()) {
                throw fail(code + ": секция end_dt есть только у period-таблиц");
            }
            return value;
        }
    }

    /** Модель набора на время его публикации: хуки ProcessingStage видят только имя таблицы (CompatHooks: tif -> byCode). */
    private static final Map<String, Table> BY_CODE = new ConcurrentHashMap<>();

    private CompatTables() {
    }

    /**
     * Строит модель таблицы набора mask из meta и каталога и регистрирует её для хуков публикации.
     * loader — загрузчик ESSA, который вёл эту таблицу; null — в ESSA таблицы не было, поправки ESSA к ней не применимы: отказ.
     */
    public static Table register(App app, String mask, EssaLoader loader) {
        TablePairInfo pair = TablePairInfo.fromConsumerList(app.ctx().metadata2().getConsumers(mask));
        ru.sberbank.bigdata.sl.rt.metadata.domain.Table meta = pair.getSnpTable();
        String code = meta.getTableCode();
        if (loader == null) {
            throw fail(mask + " (" + code + "): загрузчик ESSA для таблицы не объявлен — поправки ESSA к ней не определены");
        }
        Kind kind = meta.getHistoryType() == HistType.PERIOD ? Kind.PERIOD
                : meta.getHistoryType() == HistType.NONE ? Kind.HUB : null;
        if (kind != loader.kind) {
            throw fail(code + ": тип истории в meta " + meta.getHistoryType() + ", а загрузчик ESSA " + loader);
        }
        EntityInfo ei = new EntityInfo(app.ctx().getLogEnv(), MergeMode.WITH_REWRITE, meta);
        if (kind == Kind.PERIOD ? !(START_DT_FIELD_NAME.equals(ei.getStartDtField()) && END_DT_FIELD_NAME.equals(ei.getEndDtField()))
                : ei.getStartDtField() != null || ei.getEndDtField() != null) {
            throw fail(code + ": даты версии в meta " + ei.getStartDtField() + "/" + ei.getEndDtField()
                    + ", поправки ESSA рассчитаны на " + (kind == Kind.PERIOD ? START_DT_FIELD_NAME + "/" + END_DT_FIELD_NAME : "их отсутствие"));
        }
        Map<String, Partition> derived = ei.getDerivedPartitioningFields();
        SparkSession spark = app.getSparkSession();
        String snpTable = new TableInfoFactory(ei).createTableInfo(SchemaName.SNP).getQualifiedTableName();

        // Колонки хэша ESSA: схема snp в порядке каталога (ESSA приводит вход к ней, E/slave/app/SparkWrapper.java:104-113)
        // без технических ctl* (E/slave/loading/SnpDataLoader.java:127) и секций
        Predicate<String> data = c -> !c.startsWith("ctl") && !derived.containsKey(c);
        spark.catalog().refreshTable(snpTable);
        List<String> columns = Collections.unmodifiableList(Arrays.stream(spark.table(snpTable).schema().fieldNames())
                .filter(data).collect(toList()));
        Set<String> metaColumns = meta.getColumns().stream()
                .map(ru.sberbank.bigdata.sl.rt.metadata.domain.Column::getColumnCode).filter(data).collect(toSet());
        if (!metaColumns.equals(new HashSet<>(columns))) {
            throw fail(snpTable + ": колонки каталога " + columns + " не совпадают с meta " + new TreeSet<>(metaColumns));
        }

        String p1month = null;
        Partition p1Partition = null;
        String openPartition = null;
        if (kind == Kind.PERIOD) {
            Map.Entry<String, Partition> bh = derived.entrySet().stream()
                    .filter(e -> e.getValue().getMethod() == Method.BUSINESS_HISTORY
                            && END_DT_FIELD_NAME.equals(e.getValue().getOriginColumnCode()))
                    .findFirst()
                    .orElseThrow(() -> fail(code + ": нет секции BUSINESS_HISTORY по end_dt в метаданных"));
            p1month = bh.getKey();
            p1Partition = bh.getValue();
            openPartition = sectionOf(spark, p1month, p1Partition, PLUS_INFINITY_DT);
        }
        Table t = new Table(code, snpTable, loader, Collections.unmodifiableList(new ArrayList<>(ei.getPkFields())), columns,
                p1month, p1Partition, openPartition);
        BY_CODE.put(code, t);
        return t;
    }

    /** Значение секции для даты end_dt — тем же SectionBy, что у вендора (V/publisher/util/PublisherUtils.java:107-120). */
    public static String sectionOf(SparkSession spark, String name, Partition partition, String endDt) {
        Dataset<Row> d = spark.range(1).select(lit(endDt).cast(DataTypes.DateType).as(END_DT_FIELD_NAME));
        return SectionBy.valueOf(partition.getUnit().name())
                .compute(new SectionBy.SectionData(d, name, partition))
                .first().getAs(name);
    }

    /** Модель, зарегистрированная publishChecked этого набора; иначе публикация идёт мимо слоя совместимости — отказ. */
    public static Table byCode(String code) {
        Table t = BY_CODE.get(code);
        if (t == null) {
            throw fail(code + ": публикация не через publishChecked, модель таблицы не зарегистрирована");
        }
        return t;
    }
}
