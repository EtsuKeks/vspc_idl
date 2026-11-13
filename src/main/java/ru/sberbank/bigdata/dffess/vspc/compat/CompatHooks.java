package ru.sberbank.bigdata.dffess.vspc.compat;

import org.apache.spark.sql.*;
import ru.sberbank.bigdata.dffess.vspc.compat.hub.HubCompatCalculator;
import ru.sberbank.bigdata.dffess.vspc.compat.merge.MergeCompatCalculator;
import ru.sberbank.bigdata.dffess.vspc.compat.period.PeriodRewriteCompatCalculator;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Partition;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.*;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.changes.*;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.merge.*;

import java.lang.reflect.Field;
import java.util.*;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatTables.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.period.PeriodRewriteCompatCalculator.EMPTY_INPUT_MARKER;
import static ru.sberbank.bigdata.sl.rt.core.Constants.*;

/** Точки входа блоков VSPC-COMPAT в shadow ProcessingStage. */
public final class CompatHooks {

    /** Калькулятор изменений стадии -> её merge (runIncr вызывает оба у одного ProcessingStage). */
    private static final Map<Object, ChangesCalculator> BY_STAGE = Collections.synchronizedMap(new WeakHashMap<>());

    private CompatHooks() {
    }

    /** DIVH_INIT_VALIDFROM (O/impl/loader/DataLoaderHistMix.java:108-110): поверх вендорского штампа now(). */
    public static Dataset<Row> enrichInit(Dataset<Row> dataset, TableInfoFactory tif) {
        Table t = byCode(tif.createTableInfo(SchemaName.SNP).getTableName());
        return t.initVfFromStart() ? dataset.withColumn(CTL_VALIDFROM, col(START_DT_FIELD_NAME).cast(TimestampType)) : dataset;
    }

    /** DV-07 (E/slave/check/EmptyChecker.java:13-16), DIVTYPE_EMPTY_INCR (E/slave/loading/DataLoaderHistSnp.java:70): пустой INCR при группе окна ESSA в snp (div_type — при любой открытой) — маркер мимо раннего выхода вендора. */
    public static Dataset<Row> emptyIncrMarker(Dataset<Row> ds, Table t, AppContext ctx, SparkSession spark) {
        if (ctx.getLoadMode() != App.LoadMode.INCREMENT || !ds.isEmpty() || spark.table(t.snpTable())
                .filter(t.isPeriod() ? col(t.p1month()).equalTo(lit(t.openPartition())) : lit(true)) // DV-79 (O/impl/loader/DataLoaderHistMix.java:66): PERIOD — открытые
                .groupBy(cols(t.bk())).count().filter(col("count").gt(t.histSnp() ? 0 : 1)).isEmpty()) {
            return ds;
        }
        String pk = t.bk().get(0);
        return spark.range(1).select(Arrays.stream(ds.schema().fields())
                .map(f -> (f.name().equals(pk) ? lit(EMPTY_INPUT_MARKER) : f.name().equals(END_DT_FIELD_NAME) ? lit(PLUS_INFINITY_DT) : lit(null))
                        .cast(f.dataType()).as(f.name()))
                .toArray(Column[]::new));
    }

    /** GUARD_UNKNOWN_CALCULATOR: только точный класс вендора в режиме INCR, иначе отказ. */
    public static ChangesCalculator wrapChanges(Object stage, ChangesCalculator cc, PublisherDao dao, TableInfoFactory tif,
                                                Map<String, Partition> derivedPartitioningFields) {
        Table t = byCode(tif.createTableInfo(SchemaName.SNP).getTableName());
        ChangesCalculator wrapped;
        if (cc != null && cc.getClass() == HistoricalPeriodWithRewriteChangesCalculator.class && t.isPeriod()) {
            Class<?> rewriteBase = cc.getClass().getSuperclass(); // package-private AbstractHistoricalWithRewriteChangesCalculator
            requireFalse(cc, rewriteBase, "fullSlice");
            requireFalse(cc, rewriteBase, "reInit");
            requireFalse(cc, cc.getClass(), "collapsePeriods");
            wrapped = new PeriodRewriteCompatCalculator(cc, t, loadingId(cc), dao, tif, derivedPartitioningFields);
        } else if (cc != null && cc.getClass() == NonHistoricalChangesCalculator.class && !t.isPeriod()) {
            requireFalse(cc, cc.getClass(), "fullSlice");
            requireFalse(cc, cc.getClass(), "physicalDeletion");
            wrapped = new HubCompatCalculator(cc, t, loadingId(cc), dao, tif);
        } else {
            throw fail("Калькулятор изменений " + (cc == null ? "null" : cc.getClass().getName()) + " для " + t.code()
                    + " (" + t.kind() + "): поправки ESSA для него не объявлены");
        }
        BY_STAGE.put(stage, wrapped);
        return wrapped;
    }

    /** GUARD_UNKNOWN_CALCULATOR: merge только после обёрнутого calculateDiff той же стадии. */
    public static MergeCalculator wrapMerge(Object stage, MergeCalculator mc) {
        ChangesCalculator cc = BY_STAGE.remove(stage);
        if (mc == null || mc.getClass() != GeneralMergeCalculator.class || cc == null) {
            throw fail("Merge без обёрнутого calculateDiff той же стадии или неизвестный калькулятор merge "
                    + (mc == null ? "null" : mc.getClass().getName()));
        }
        if (cc instanceof HubCompatCalculator) {
            HubCompatCalculator hub = (HubCompatCalculator) cc;
            return new MergeCompatCalculator(mc, hub.table(), null, hub.dupKeys(), hub.nullSnp());
        }
        PeriodRewriteCompatCalculator period = (PeriodRewriteCompatCalculator) cc;
        return new MergeCompatCalculator(mc, period.table(), period.stash(), period.winKeys(), period.winSnp());
    }

    private static int loadingId(ChangesCalculator cc) {
        return (Integer) readField(cc, AbstractChangesCalculator.class, "loadingId");
    }

    private static void requireFalse(Object target, Class<?> declaring, String field) {
        if (!Boolean.FALSE.equals(readField(target, declaring, field))) {
            throw fail("Калькулятор " + target.getClass().getSimpleName() + ": " + field
                    + " != false — режим вне INCR merge-пути, поправки ESSA не применимы");
        }
    }

    /** Отсутствие поля = вендорский класс изменился = отказ. */
    private static Object readField(Object target, Class<?> declaring, String name) {
        try {
            Field f = declaring.getDeclaredField(name);
            f.setAccessible(true);
            return f.get(target);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw fail("Поле " + declaring.getName() + "." + name + " недоступно (" + e + "): вендорский класс изменился");
        }
    }
}
