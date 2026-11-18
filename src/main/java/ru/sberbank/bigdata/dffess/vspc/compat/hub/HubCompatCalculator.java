package ru.sberbank.bigdata.dffess.vspc.compat.hub;

import org.apache.spark.sql.*;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SnapshotChanges;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.changes.ChangesCalculator;

import java.util.*;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.*;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.*;

// Обёртка над калькулятором изменений RT-FW, мы даём ему снимок с одной строкой на ключ и у строк (U,U) возвращаем\
// старый ctl_validfrom, как в ESSA.
public final class HubCompatCalculator implements ChangesCalculator {

    private static final String OLD_VF = "_compat_old_vf";

    private final ChangesCalculator delegate;
    private final CompatTables.Table table;

    public HubCompatCalculator(ChangesCalculator delegate, CompatTables.Table table) {
        this.delegate = delegate;
        this.table = table;
    }

    @Override
    public Map<String, String> getMinBusinessDateUpdateFilters() {
        return delegate.getMinBusinessDateUpdateFilters();
    }

    @Override
    public Column getMinBusinessDateUpdateFilter() {
        return delegate.getMinBusinessDateUpdateFilter();
    }

    @Override
    public SnapshotChanges calculate(Dataset<Row> incr, Dataset<Row> snpAll, List<Map<String, String>> specs) {
        // Одна строка на ключ.
        Dataset<Row> snp = oneOpenPerKey(table, snpAll);
        SnapshotChanges out = delegate.calculate(incr, snp, specs);
        return new SnapshotChanges(out.getInserts(), undoValidFrom(out.getUpdates(), snp), out.getDeletes());
    }

    // RT-FW ставит ctl_validfrom = now() всем строкам diff. В ESSA (U,U) — старая версия из snp со своим ctl_validfrom:
    // возвращаем его из snp.
    private Dataset<Row> undoValidFrom(Dataset<Row> stamped, Dataset<Row> snp) {
        Column uu = isLeg(CTL_DO_UNDO, CTL_ACTION_UPDATE);
        Column[] sel = Arrays.stream(stamped.columns())
            .map(c -> CTL_VALIDFROM.equals(c) ? col(OLD_VF).as(CTL_VALIDFROM) : col(c)).toArray(Column[]::new);
        Dataset<Row> undoFixed = stamped.filter(uu)
            .join(snp.select(cols(table.versionKey(), col(CTL_VALIDFROM).as(OLD_VF))), scalaSeq(table.versionKey()), "left")
            .select(sel);
        return stamped.filter(not(uu)).unionByName(undoFixed);
    }
}
