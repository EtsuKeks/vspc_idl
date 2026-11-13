package ru.sberbank.bigdata.dffess.vspc.compat.period;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import ru.sberbank.bigdata.dffess.vspc.common.VspcLoadDate;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;
import ru.sberbank.bigdata.sl.rt.metadata.domain.Partition;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.*;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.changes.ChangesCalculator;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.*;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.*;

/**
 * Декоратор вендорского HistoricalPeriodWithRewriteChangesCalculator (INCR) для addr_h, div_h, div_type,
 * div_div_relation: вендор работает как есть, меняются его вход и выход (шаги 1-11 в {@link #calculate}).
 * Опора: в INCR трансформации ставят start_dt = loadDate-1, поэтому вендорское закрытие MIN_DT-1 = loadDate-2,
 * как vf-2 в ESSA (O/impl/loader/DataLoaderHistMix.java:85).
 */
public final class PeriodRewriteCompatCalculator implements ChangesCalculator {

    /** DV-07, DIVTYPE_EMPTY_INCR: ключ строки-маркера пустого входа (CompatHooks.emptyIncrMarker). */
    public static final long EMPTY_INPUT_MARKER = Long.MIN_VALUE;

    private static final String H = "_vspc_h";
    private static final String OPEN_FLG = "_vspc_open";
    private static final String CLOSED_END = "_vspc_closed_end";
    private static final String PREV_SRC = "_vspc_prev_src";
    private static final String PREV_VF = "_vspc_prev_vf";

    private final ChangesCalculator delegate;
    private final CompatTables.Table table;
    private final int loadingId;
    private final PublisherDao dao;
    private final TableInfoFactory tif;
    private final String p1month;
    private final Partition p1Partition;
    private Map<String, String> zone;
    // состояние одного calculate (стадия вызывает его один раз): спецификация, метка, открытые версии, их хэш ESSA
    private List<Map<String, String>> specs;
    private LocalDateTime now;
    private Dataset<Row> openAll;
    private Dataset<Row> openIdx;
    private Dataset<Row> stash;
    private Dataset<Row> winKeys;
    private Dataset<Row> winSnp;

    public PeriodRewriteCompatCalculator(ChangesCalculator delegate, CompatTables.Table table, int loadingId, PublisherDao dao,
                                         TableInfoFactory tif, Map<String, Partition> derivedPartitioningFields) {
        this.delegate = delegate;
        this.table = table;
        this.loadingId = loadingId;
        this.dao = dao;
        this.tif = tif;
        this.p1month = table.p1month();
        this.p1Partition = table.p1Partition();
        // модель строится по meta тем же EntityInfo, что у публикатора: его секция обязана совпасть
        Partition pub = derivedPartitioningFields.get(p1month);
        if (pub == null || pub.getMethod() != p1Partition.getMethod() || pub.getUnit() != p1Partition.getUnit()
                || !END_DT_FIELD_NAME.equals(pub.getOriginColumnCode())) {
            throw fail(table.code() + ": секции публикатора " + derivedPartitioningFields.keySet() + " не совпадают с моделью (" + p1month + ")");
        }
    }

    /** PERIOD_ZONE_MONTH (E/slave/move/HistMovingGenerator.java:30-32): p1month >= yyyyMM(D-1 мес), одно значение для чтения и переноса. */
    @Override
    public Map<String, String> getMinBusinessDateUpdateFilters() {
        if (zone == null) {
            LocalDate threshold = LocalDate.now().minusMonths(1).withDayOfMonth(1); // DV-05: часы мувера (E/slave/move/HistMovingGenerator.java:31)
            String value = CompatTables.sectionOf(SparkSession.active(), p1month, p1Partition, threshold.toString());
            zone = Collections.singletonMap(p1month, value);
            info(table.code() + ": зона перезаписи " + p1month + " >= " + value);
        }
        return zone;
    }

    @Override
    public Column getMinBusinessDateUpdateFilter() {
        return col(p1month).geq(lit(getMinBusinessDateUpdateFilters().get(p1month)));
    }

    public CompatTables.Table table() { return table; }
    /** (R,D), которые ESSA снимает молча: в diff их нет, merge удаляет их из снапа. */
    public Dataset<Row> stash() { return stash; }
    /** DV-03: bk окна ESSA и их новый снап (открытые + закрытия), мимо вендорского merge. */
    public Dataset<Row> winKeys() { return winKeys; }
    public Dataset<Row> winSnp() { return winSnp; }

    @Override
    public SnapshotChanges calculate(Dataset<Row> incr, Dataset<Row> snp, List<Map<String, String>> specs) {
        this.specs = specs;
        List<String> bk = table.bk();
        List<String> pk = table.pk();
        Column nn = table.bkNotNull();
        // (1) DIVTYPE_EMPTY_INCR, DV-79: снять маркер, обходящий ранний выход вендора на пустом входе (V/PublisherImpl.java:230-235)
        incr = incr.filter(not(col(bk.get(0)).eqNullSafe(lit(EMPTY_INPUT_MARKER))));
        now = LocalDateTime.now();
        Dataset<Row> winLegs = windowLegs(incr, snp);
        incr = nullSafeJoin(incr, winKeys, bk, "left_anti");
        snp = nullSafeJoin(snp, winKeys, bk, "left_anti");
        // (2) PERIOD_OPEN_ONLY: ESSA сравнивает вход только с p1month='999912' (O/impl/loader/DataLoaderHistMix.java:65-66)
        openAll = snp.filter(col(p1month).equalTo(lit(table.openPartition())));
        Column h = table.essaHash();
        openIdx = tmp("_vspc_open_idx", openAll.select(cols(pk, h.as(H))));
        // (3) PERIOD_EQUALS_RETENTION: строка с хэшем ESSA открытой версии ключа строк diff не даёт (E/slave/loading/DataLoaderHistSnp.java:82-86)
        List<String> keyH = new ArrayList<>(bk);
        keyH.add(H);
        Dataset<Row> incAll = tmp("_vspc_inc",
                nullSafeJoin(incr.withColumn(H, h), openIdx.select(cols(keyH)).distinct(), keyH, "left_anti").drop(H));
        // PERIOD_NULL_KEY (O/impl/loader/DataLoaderHistMix.java:84-88): NULL-ключ мимо вендора (join не null-safe)
        Dataset<Row> nullInc = incAll.filter(not(nn));
        Dataset<Row> nullOld = nullSafeJoin(openAll.filter(not(nn)), nullInc.select(cols(bk)), bk, "left_semi");
        Dataset<Row> inc = incAll.filter(nn);
        Dataset<Row> open = openAll.filter(nn);
        // (4) PERIOD_CONSEC_DAY: пары с равным полным PK решаются без вендорского хэша (O/impl/loader/DataLoaderHistMix.java:84-89)
        Dataset<Row> pairs = tmp("_vspc_consec_pk", inc.select(cols(pk)).join(openIdx.select(cols(pk)), scalaSeq(pk), "inner").distinct());
        // (5) вендорский калькулятор как есть
        SnapshotChanges ch = delegate.calculate(join(inc, pairs, pk, "left_anti"), join(open, pairs, pk, "left_anti"), specs);
        return legs(ch, join(inc, pairs, pk, "left_semi").unionByName(nullInc), join(open, pairs, pk, "left_semi"), nullOld, winLegs, incr);
    }

    /** Шаги (6)-(11) над выходом вендора: newRows — новая версия консекутивного дня и NULL-ключа, consecOld — её старая версия. */
    private SnapshotChanges legs(SnapshotChanges ch, Dataset<Row> newRows, Dataset<Row> consecOld, Dataset<Row> nullOld,
                                 Dataset<Row> winLegs, Dataset<Row> incr) {
        Column d2 = date_sub(lit(VspcLoadDate.sqlDate()), 2);
        Column nullKept = col(START_DT_FIELD_NAME).leq(d2); // PERIOD_NULL_KEY: открытая NULL-версия закрывается loadDate-2, при start > end выпадает молча
        Dataset<Row> ins = ch.getInserts();
        Dataset<Row> upd = ch.getUpdates();
        Dataset<Row> del = ch.getDeletes();
        StructType updSchema = upd.schema();
        StructType delSchema = del.schema();
        // (6) PERIOD_SILENT_DROP: удаления части-1 ESSA не публикует, молча (E/slave/loading/DataLoaderHistSnp.java:117-119)
        Dataset<Row> stashSilent = tmp("_vspc_stash_silent", del.filter(isLeg(CTL_DO_REDO, CTL_ACTION_DELETE)));
        del = join(del, stashSilent.select(cols(table.pk())).distinct(), table.pk(), "left_anti");
        // (7) новая версия консекутивного дня и NULL-ключа: (R,I)+(U,D) с payload инкремента
        ins = ins.unionByName(leg(newRows, ins.schema(), CTL_DO_REDO, CTL_ACTION_INSERT))
                .unionByName(leg(newRows, ins.schema(), CTL_DO_UNDO, CTL_ACTION_DELETE));
        upd = upd.unionByName(close(nullOld.filter(nullKept), d2, updSchema))
                .unionByName(castDataset(winLegs, updSchema));
        ins = newKeyStart(ins); // (8)
        // (9) DIVTYPE_VANISH: открытые версии ключей без входа закрываются current_date()-1 (E/slave/loading/DataLoaderHistSnp.java:103-108)
        if (table.histSnp()) {
            Dataset<Row> vanished = tmp("_vspc_vanished", nullSafeJoin(openAll, incr.select(cols(table.bk())).distinct(), table.bk(), "left_anti"));
            upd = upd.unionByName(close(vanished, date_sub(current_date(), 1), updSchema));
        }
        // (10) одна метка ctl_validfrom на все строки diff, как NOW_TC в ESSA
        SnapshotChanges out = new SnapshotChanges(ins, upd, del).enrichedWithTechnicalFields(loadingId, now);
        // (11) stash: silent-drop, старая версия консекутивного дня, выпавшая NULL-версия
        stash = tmp("_vspc_compat_stash", leg(stashSilent, delSchema, CTL_DO_REDO, CTL_ACTION_DELETE)
                .unionByName(leg(consecOld, delSchema, CTL_DO_REDO, CTL_ACTION_DELETE))
                .unionByName(leg(nullOld.filter(not(nullKept)), delSchema, CTL_DO_REDO, CTL_ACTION_DELETE)));
        // DV-08 (O/impl/loader/DataLoaderHistMix.java:92): открытая версия с ctl_loading=L без строк diff — холостая пара (R,U)+(U,U)
        Dataset<Row> idle = nullSafeJoin(openAll.filter(col(CTL_LOADING).equalTo(lit((long) loadingId))), upd.select(cols(table.pk()))
                .unionByName(del.select(cols(table.pk()))).unionByName(stash.select(cols(table.pk()))), table.pk(), "left_anti");
        StructType outUpd = out.getUpdates().schema();
        return new SnapshotChanges(out.getInserts(), out.getUpdates().unionByName(leg(idle, outUpd, CTL_DO_REDO, CTL_ACTION_UPDATE))
                .unionByName(leg(idle, outUpd, CTL_DO_UNDO, CTL_ACTION_UPDATE)), out.getDeletes());
    }

    /** (8) PERIOD_NEW_KEY_1900, DIVTYPE_RETURNER_START (E/slave/loading/DataLoaderHistSnp.java:88-96): вставка ключа без открытой версии. */
    private Dataset<Row> newKeyStart(Dataset<Row> ins) {
        List<String> bk = table.bk();
        Dataset<Row> openKeys = openIdx.select(cols(bk)).distinct().withColumn(OPEN_FLG, lit(true));
        Dataset<Row> j = returner(nullSafeJoin(ins, openKeys, bk, "left"),
                nullSafeJoin(ins.select(cols(bk)).distinct(), openKeys, bk, "left_anti"), "_vspc_returner_cand");
        return j.withColumn(START_DT_FIELD_NAME, when(col(OPEN_FLG).isNull(), newStart()).otherwise(col(START_DT_FIELD_NAME))).select(cols(Arrays.asList(ins.columns())));
    }

    /** DV-03 (E/slave/loading/DataLoaderHistSnp.java:82-122, O/impl/loader/DataLoaderLinkHistDiff.java:80-97): окно ESSA по открытым; возврат — строки diff. */
    private Dataset<Row> windowLegs(Dataset<Row> incr, Dataset<Row> snp) {
        List<String> bk = table.bk();
        Dataset<Row> open = snp.filter(col(p1month).equalTo(lit(table.openPartition())));
        winKeys = tmp("_vspc_win_keys", essaWindowKeys(table, incr, open, 1)); // DV-79, DV-81 (O/impl/loader/DataLoaderHistMix.java:66,74-89): NULL-ключ — со входом или >1 открытой
        WindowSpec byRn = Window.partitionBy(cols(bk)).orderBy(col(RN));
        Dataset<Row> win = tmp("_vspc_win", essaWindow(table, nullSafeJoin(incr, winKeys, bk, "left_semi"), nullSafeJoin(open, winKeys, bk, "left_semi"), loadingId, now)
                .withColumn(PREV_SRC, lag(col(SRC), 1).over(byRn))
                .withColumn(PREV_VF, lag(col(CTL_VALIDFROM), 1).over(byRn)));
        Column closing = st(NOTEQUALS).and(rn(2)).and(col(SRC).equalTo(lit(OLD)));
        Column vanish = st(OLD).and(lit(table.histSnp()));
        Column kept = st(OLD).and(lit(!table.histSnp())); // DV-79 (O/impl/loader/DataLoaderHistMix.java:79): OLD rn1 остаётся как есть
        Column closeAt = when(col(PREV_SRC).equalTo(lit(OLD)), date_sub(to_date(col(PREV_VF)), 2))
                .otherwise(date_sub(lit(VspcLoadDate.sqlDate()), 2));
        win = returner(win, win.filter(st(NEW)), "_vspc_win_returner"); // DV-85 (E/slave/loading/DataLoaderHistSnp.java:90-96): NEW окна при закрытой истории, как шаг 8
        Dataset<Row> res = win.filter(essaSnp().or(kept))
                .withColumn(START_DT_FIELD_NAME, when(st(NEW), newStart()).otherwise(col(START_DT_FIELD_NAME)))
                .unionByName(SnapshotChanges.enrichedWithTechnicalFields(win.withColumn(END_DT_FIELD_NAME, closeAt).filter(closing.or(vanish))
                        .withColumn(END_DT_FIELD_NAME, when(vanish, date_sub(current_date(), 1)).otherwise(col(END_DT_FIELD_NAME))), loadingId, now)
                        .filter(vanish.or(col(END_DT_FIELD_NAME).geq(col(START_DT_FIELD_NAME)))))
                .drop(CLOSED_END);
        res = belowZone(tmp("_vspc_win_res", section(res)));
        winSnp = res.select(cols(Arrays.asList(snp.columns())));
        Dataset<Row> mine = res.filter(col(CTL_LOADING).equalTo(lit(loadingId)));
        return mine.withColumn(CTL_DO, lit(CTL_DO_REDO))
                        .withColumn(CTL_ACTION, when(essaFirst(), lit(CTL_ACTION_INSERT)).otherwise(lit(CTL_ACTION_UPDATE)))
                .unionByName(section(mine.withColumn(CTL_DO, lit(CTL_DO_UNDO))
                        .withColumn(CTL_ACTION, when(essaFirst(), lit(CTL_ACTION_DELETE)).otherwise(lit(CTL_ACTION_UPDATE)))
                        .withColumn(END_DT_FIELD_NAME, when(closing.or(vanish), lit(PLUS_INFINITY_DT).cast(DataTypes.DateType)).otherwise(col(END_DT_FIELD_NAME)))));
    }

    /** DV-79, DV-80 (E/slave/loading/DataLoaderHistSnp.java:103-108,124): закрытие ниже зоны перезаписи — группа остаётся на пути вендора. */
    private Dataset<Row> belowZone(Dataset<Row> res) {
        List<String> bk = table.bk();
        Dataset<Row> below = res.filter(col(p1month).lt(lit(getMinBusinessDateUpdateFilters().get(p1month)))).select(cols(bk)).distinct();
        if (below.isEmpty()) {
            return res;
        }
        info(table.code() + ": группы окна ESSA с закрытием ниже зоны перезаписи остаются на пути вендора");
        winKeys = tmp("_vspc_win_keys2", nullSafeJoin(winKeys, below, bk, "left_anti"));
        return nullSafeJoin(res, below, bk, "left_anti");
    }

    /** DIVTYPE_RETURNER_START, DV-85 (E/slave/loading/DataLoaderHistSnp.java:88-96): div_type — CLOSED_END по bk из cand, иначе NULL. */
    private Dataset<Row> returner(Dataset<Row> ds, Dataset<Row> cand, String suffix) {
        if (table.histSnp()) {
            cand = tmp(suffix, cand.select(cols(table.bk())).distinct());
            if (!cand.isEmpty()) {
                return ds.join(closedEnd(cand), scalaSeq(table.bk()), "left");
            }
        }
        return ds.withColumn(CLOSED_END, lit(null).cast(DataTypes.DateType));
    }

    /** start_dt ключа без открытой версии: max(end_dt)+1 закрытой истории, иначе 1900-01-01 (PERIOD_NEW_KEY_1900). */
    private static Column newStart() {
        return coalesce(date_add(col(CLOSED_END), 1), lit(MINUS_INFINITY_DT).cast(DataTypes.DateType));
    }

    /** max(end_dt) закрытой истории ключей cand; join по нему не null-safe, как кэш ESSA (E/slave/loading/DataLoaderHistSnp.java:91). */
    private Dataset<Row> closedEnd(Dataset<Row> cand) {
        return dao.readTable(tif.createTableInfo(SchemaName.SNP)).filter(buildFilterByPartitions(specs))
                .filter(col(p1month).notEqual(lit(table.openPartition()))).filter(col(END_DT_FIELD_NAME).notEqual(lit(PLUS_INFINITY_DT).cast(DataTypes.DateType)))
                .select(cols(table.bk(), col(END_DT_FIELD_NAME))).join(broadcast(cand), scalaSeq(table.bk()), "left_semi")
                .groupBy(cols(table.bk())).agg(max(col(END_DT_FIELD_NAME)).as(CLOSED_END));
    }

    /** Закрытие открытых версий: (R,U) с новым end_dt и пересчитанной секцией + (U,U) как было. */
    private Dataset<Row> close(Dataset<Row> rows, Column endDt, StructType updSchema) {
        return leg(section(rows.withColumn(END_DT_FIELD_NAME, endDt)), updSchema, CTL_DO_REDO, CTL_ACTION_UPDATE)
                .unionByName(leg(rows, updSchema, CTL_DO_UNDO, CTL_ACTION_UPDATE));
    }

    private Dataset<Row> section(Dataset<Row> ds) {
        return sectionBy(ds, Collections.singletonMap(p1month, p1Partition), SectionBy.SectionData.D_MIN, SectionBy.SectionData.D_MAX);
    }

    private Dataset<Row> tmp(String suffix, Dataset<Row> ds) {
        return CompatSupport.land(dao, tif, specs, suffix, ds);
    }
}
