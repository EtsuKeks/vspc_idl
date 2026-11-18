package ru.sberbank.bigdata.dffess.vspc.compat.merge;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.*;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.merge.MergeCalculator;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.*;

// Обёртка над GeneralMergeCalculator RT-FW (слияние старого snp с diff): сам он работает как есть.
// Но перед слиянием старый snp приводим к тому же виду, с которым считал калькулятор изменений: одна открытая версия
// на ключ (лишних дубли в новый snp не переносим, ESSA-версия их теряла), у исторических таблиц ещё и без будущих
// закрытых версий.
public final class MergeCompatCalculator implements MergeCalculator {

    private final MergeCalculator delegate;
    private final CompatTables.Table table;

    public MergeCompatCalculator(MergeCalculator delegate, CompatTables.Table table) {
        this.delegate = delegate;
        this.table = table;
    }

    @Override
    public Dataset<Row> calculate(Dataset<Row> oldSnp, SnapshotChanges changes, TableInfoFactory tableInfoFactory,
                                  PublisherDao dao, SchemaName targetSchemaName, List<Map<String, String>> partitionSpecs) {
        // oneOpenPerKey выбирает ту же строку, что и в калькуляторе изменений (HubCompatCalculator или
        // PeriodRewriteCompatCalculator): выбор однозначный, снимок тот же (один и тот же oldSnp передается в
        // HubCompatCalculator и в PeriodRewriteCompatCalculator).
        Dataset<Row> snp = oneOpenPerKey(table, oldSnp);
        if (table.isPeriod()) {
            Column open = col(table.partitionColumn()).equalTo(lit(table.openPartition()));

            // Закрытые версии в партициях позже текущего месяца (кроме открытой) не переносим, как в ESSA-версии: её 
            // мувер стирал все партиции с прошлого месяца, а загрузчик возвращал только прошлый, текущий месяц и
            // открытую. Иначе будущие версии из истории INIT пересекаются с новой открытой версией.
            Column future = col(table.partitionColumn()).gt(lit(table.partitionOf(LocalDate.now().toString())));
            snp = oldSnp.filter(not(open).and(not(future))).unionByName(oneOpenPerKey(table, oldSnp.filter(open)));
        }
        return delegate.calculate(snp, changes, tableInfoFactory, dao, targetSchemaName, partitionSpecs);
    }
}
