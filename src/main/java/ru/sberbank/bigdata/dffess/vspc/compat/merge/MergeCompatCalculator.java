package ru.sberbank.bigdata.dffess.vspc.compat.merge;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.*;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.merge.MergeCalculator;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatTables.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.*;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.castDataset;

/**
 * Декоратор вендорского GeneralMergeCalculator: stash (R,D) калькулятора изменений — в deletes; строки с NULL-ключом
 * мимо вендора (его join по PK не null-safe, V/strategy/process/merge/GeneralMergeCalculator.java:61-66, 79-82).
 */
public final class MergeCompatCalculator implements MergeCalculator {

    private final MergeCalculator delegate;
    private final CompatTables.Table table;
    private final Dataset<Row> stash;
    private final Dataset<Row> winKeys;
    private final Dataset<Row> winSnp;

    /** stash — PERIOD: (R,D) вне diff; winKeys/winSnp — bk и снап групп окна 3.0 (DV-03), мимо вендора. */
    public MergeCompatCalculator(MergeCalculator delegate, CompatTables.Table table, Dataset<Row> stash,
                                 Dataset<Row> winKeys, Dataset<Row> winSnp) {
        this.delegate = delegate;
        this.table = table;
        this.stash = stash;
        this.winKeys = winKeys;
        this.winSnp = winSnp;
    }

    @Override
    public Dataset<Row> calculate(Dataset<Row> oldSnp, SnapshotChanges changes, TableInfoFactory tableInfoFactory,
                                  PublisherDao dao, SchemaName targetSchemaName, List<Map<String, String>> partitionSpecs) {
        Column nn = table.bkNotNull();
        // PERIOD_CLOSED_KEEP, DV-04: закрытые секции зоны 3.0 переносит как есть (O/impl/loader/DataLoaderHistMix.java:66-69,94)
        Dataset<Row> closed = null;
        Dataset<Row> deletes = changes.getDeletes();
        if (table.isPeriod()) {
            Column open = col(P1MONTH).equalTo(lit(OPEN_PARTITION));
            closed = oldSnp.filter(not(open));
            oldSnp = oldSnp.filter(open);
            deletes = deletes.unionByName(castDataset(stash, deletes.schema())); // PERIOD_SILENT_DROP, PERIOD_CONSEC_DAY
        }
        List<String> bk = table.bk();
        Dataset<Row> ins = join(changes.getInserts().filter(nn), winKeys, bk, "left_anti");
        Dataset<Row> upd = join(changes.getUpdates().filter(nn), winKeys, bk, "left_anti");
        Dataset<Row> snpNn = join(oldSnp.filter(nn), winKeys, bk, "left_anti");
        if (table.isPeriod()) { // DV-80 (E/slave/loading/DataLoaderHistSnp.java:103-108,124): N закрытий одного PK (группа ниже зоны) — одна строка на PK вместо N×N (GMC:79-82)
            List<String> pk = table.pk();
            Dataset<Row> redoUpd = upd.filter(col(CTL_DO).equalTo(lit(CTL_DO_REDO)));
            Dataset<Row> dupPk = land(dao, tableInfoFactory, partitionSpecs, "_vspc_merge_dup_pk",
                    redoUpd.groupBy(cols(pk)).count().filter(col("count").gt(1)).select(cols(pk)));
            ins = ins.unionByName(castDataset(land(dao, tableInfoFactory, partitionSpecs, "_vspc_merge_dup_upd",
                    join(redoUpd, dupPk, pk, "left_semi").dropDuplicates(pk.toArray(new String[0]))), ins.schema()));
            upd = join(upd, dupPk, pk, "left_anti");
            snpNn = join(snpNn, dupPk, pk, "left_anti");
        }
        SnapshotChanges nonNull = new SnapshotChanges(ins, upd, join(deletes.filter(nn), winKeys, bk, "left_anti"));
        Dataset<Row> merged = delegate.calculate(snpNn, nonNull,
                tableInfoFactory, dao, targetSchemaName, partitionSpecs).unionByName(castDataset(winSnp, oldSnp.schema()));
        if (table.isPeriod()) {
            merged = merged.unionByName(mergeNullKeys(outWin(oldSnp.filter(not(nn))), outWin(changes.getInserts().filter(not(nn))),
                    outWin(changes.getUpdates().filter(not(nn))), outWin(deletes.filter(not(nn)))));
        }
        return closed == null ? merged : merged.unionByName(closed);
    }

    /** DV-07: NULL-группа окна 3.0 (div_type без входа) уже в winSnp. */
    private Dataset<Row> outWin(Dataset<Row> ds) {
        return nullSafeJoin(ds, winKeys, table.bk(), "left_anti");
    }

    /** PERIOD_NULL_KEY: шаги GeneralMergeCalculator.java:45-108 (snp - deletes + inserts, затем updates) с eqNullSafe по PK. */
    private Dataset<Row> mergeNullKeys(Dataset<Row> snp, Dataset<Row> ins, Dataset<Row> upd, Dataset<Row> del) {
        List<String> pk = table.pk();
        Column redo = col(CTL_DO).equalTo(lit(CTL_DO_REDO));
        Dataset<Row> base = nullSafeJoin(snp, del.filter(redo).select(cols(pk)), pk, "left_anti")
                .unionByName(castDataset(ins.filter(redo), snp.schema()));
        Dataset<Row> u = castDataset(upd.filter(redo), snp.schema());
        return nullSafeJoin(base, u.select(cols(pk)), pk, "left_anti")
                .unionByName(nullSafeJoin(u, base.select(cols(pk)), pk, "left_semi"));
    }
}
