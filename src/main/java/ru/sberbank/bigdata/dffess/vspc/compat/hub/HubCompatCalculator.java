package ru.sberbank.bigdata.dffess.vspc.compat.hub;

import org.apache.spark.sql.*;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SnapshotChanges;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfoFactory;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.changes.ChangesCalculator;

import java.time.LocalDateTime;
import java.util.*;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.period.PeriodRewriteCompatCalculator.EMPTY_INPUT_MARKER;
import static ru.sberbank.bigdata.sl.rt.core.Constants.*;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.*;

/** Декоратор вендорского NonHistoricalChangesCalculator (INCR) для хабов addr, div_, terr_div_type, time_zone_rf. */
public final class HubCompatCalculator implements ChangesCalculator {

    private static final String HN = "_vspc_h_new";
    private static final String HO = "_vspc_h_old";
    private static final String OLD_VF = "_vspc_old_vf";

    private final ChangesCalculator delegate;
    private final CompatTables.Table table;
    private final int loadingId;
    private final PublisherDao dao;
    private final TableInfoFactory tif;
    private Dataset<Row> nullSnp;
    private Dataset<Row> dupKeys;

    public HubCompatCalculator(ChangesCalculator delegate, CompatTables.Table table, int loadingId,
                               PublisherDao dao, TableInfoFactory tif) {
        this.delegate = delegate;
        this.table = table;
        this.loadingId = loadingId;
        this.dao = dao;
        this.tif = tif;
    }

    @Override
    public Map<String, String> getMinBusinessDateUpdateFilters() {
        return delegate.getMinBusinessDateUpdateFilters();
    }

    @Override
    public Column getMinBusinessDateUpdateFilter() {
        return delegate.getMinBusinessDateUpdateFilter();
    }

    public CompatTables.Table table() {
        return table;
    }

    /** Снап групп окна ESSA (NULL-pk и dupKeys) для MergeCompatCalculator. */
    public Dataset<Row> nullSnp() {
        return nullSnp;
    }

    public Dataset<Row> dupKeys() {
        return dupKeys;
    }

    @Override
    public SnapshotChanges calculate(Dataset<Row> incrAll, Dataset<Row> snpAll, List<Map<String, String>> specs) {
        List<String> pk = table.pk();
        incrAll = incrAll.filter(not(col(pk.get(0)).eqNullSafe(lit(EMPTY_INPUT_MARKER)))); // DV-07: маркер CompatHooks.emptyIncrMarker
        Column nn = table.bkNotNull();
        LocalDateTime now = LocalDateTime.now();
        Dataset<Row> nullLegs = windowLegs(incrAll, snpAll, specs, now);

        Dataset<Row> incr = join(incrAll.filter(nn), dupKeys, pk, "left_anti");
        Dataset<Row> snp = join(snpAll.filter(nn), dupKeys, pk, "left_anti");
        SnapshotChanges ch = delegate.calculate(incr, snp, specs);
        SnapshotChanges out = new SnapshotChanges(ch.getInserts(), hashLegs(ch.getUpdates(), incr, snp, specs), ch.getDeletes())
                .enrichedWithTechnicalFields(loadingId, now);
        Dataset<Row> stamped = out.getUpdates();
        return new SnapshotChanges(out.getInserts(), undoValidFrom(stamped, snp, ch.getUpdates().columns())
                .unionByName(castDataset(nullLegs, stamped.schema())), out.getDeletes());
    }

    /** HUB_HASH_ESSA (E/slave/loading/SnpDataLoader.java:116-133): равные по хэшу ESSA пары теряют строки diff вендора, неравные без строк diff получают (R,U)+(U,U). */
    private Dataset<Row> hashLegs(Dataset<Row> vendor, Dataset<Row> incr, Dataset<Row> snp, List<Map<String, String>> specs) {
        List<String> pk = table.pk();
        Column h = table.essaHash();
        Dataset<Row> essaChanged = land(dao, tif, specs, "_vspc_hub_chg_essa",
                incr.select(cols(pk, h.as(HN)))
                        .join(snp.select(cols(pk, h.as(HO))), scalaSeq(pk), "inner")
                        .filter(col(HN).notEqual(col(HO)))
                        .select(cols(pk)).distinct());
        Dataset<Row> vendorPk = vendor.filter(col(CTL_DO).equalTo(lit(CTL_DO_REDO))).select(cols(pk)).distinct();
        Dataset<Row> add = essaChanged.join(vendorPk, scalaSeq(pk), "left_anti");
        Dataset<Row> upd = join(vendor, vendorPk.join(essaChanged, scalaSeq(pk), "left_anti"), pk, "left_anti");
        if (add.isEmpty()) { // без проверки — два лишних полных скана incr и snp
            return upd;
        }
        return upd.unionByName(leg(join(incr, add, pk, "left_semi"), vendor.schema(), CTL_DO_REDO, CTL_ACTION_UPDATE))
                .unionByName(leg(join(snp, add, pk, "left_semi"), vendor.schema(), CTL_DO_UNDO, CTL_ACTION_UPDATE));
    }

    /** HUB_UNDO_VALIDFROM (E/slave/loading/DataLoaderHubDiff.java:127-135): (U,U) хранит ctl_validfrom цели. */
    private Dataset<Row> undoValidFrom(Dataset<Row> stamped, Dataset<Row> snp, String[] updCols) {
        Column uu = isLeg(CTL_DO_UNDO, CTL_ACTION_UPDATE);
        Column[] sel = Arrays.stream(updCols)
                .map(c -> CTL_VALIDFROM.equals(c) ? col(OLD_VF).as(CTL_VALIDFROM) : col(c))
                .toArray(Column[]::new);
        Dataset<Row> undoFixed = stamped.filter(uu)
                .join(snp.select(cols(table.pk(), col(CTL_VALIDFROM).as(OLD_VF))), scalaSeq(table.pk()), "left")
                .select(sel);
        return stamped.filter(not(uu)).unionByName(undoFixed);
    }

    /** HUB_NULL_KEY_ESSA, DV-03 (E/slave/loading/DataLoaderHubDiff.java:75-80,127-144): группы окна ESSA (любой NULL-pk) мимо вендора; возврат — строки diff. */
    private Dataset<Row> windowLegs(Dataset<Row> incr, Dataset<Row> snp, List<Map<String, String>> specs, LocalDateTime now) {
        dupKeys = land(dao, tif, specs, "_vspc_hub_dup", essaWindowKeys(table, incr, snp, 0));
        Dataset<Row> win = land(dao, tif, specs, "_vspc_hub_null", essaWindow(table, inWin(incr), inWin(snp), loadingId, now));
        nullSnp = win.filter(essaSnp().or(st(OLD))).select(cols(Arrays.asList(snp.columns())));
        return win.filter(essaFirst().or(st(NOTEQUALS).and(rn(2))))
                .withColumn(CTL_DO, when(rn(1), lit(CTL_DO_REDO)).otherwise(lit(CTL_DO_UNDO)))
                .withColumn(CTL_ACTION, when(st(NEW), lit(CTL_ACTION_INSERT)).otherwise(lit(CTL_ACTION_UPDATE)))
                .unionByName(win.filter(st(NEW)).withColumn(CTL_DO, lit(CTL_DO_UNDO)).withColumn(CTL_ACTION, lit(CTL_ACTION_DELETE)))
                .withColumn(CTL_LOADING, lit(loadingId));
    }

    /** DV-77 (E/slave/loading/SnpDataLoader.java:181): вход окна — NULL-pk ∪ semi-join по dupKeys; план задаёт порядок ничьей. */
    private Dataset<Row> inWin(Dataset<Row> ds) {
        return ds.filter(not(table.bkNotNull())).unionByName(join(ds.filter(table.bkNotNull()), dupKeys, table.pk(), "left_semi"));
    }
}
