package ru.sberbank.bigdata.dffess.vspc.compat.merge;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.dffess.vspc.common.LoadDate;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.*;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.merge.MergeCalculator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_LOADING;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.scalaSeq;

// Обёртка над GeneralMergeCalculator RT-FW (слияние старого snp с diff).
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
        if (!table.isPeriod()) {
            // За аргументами onePerKey см. комментарий к HubCompatCalculator.calculate - мы должны быть согласованы с 
            // своем вызове, чтобы корректно передать результат hub калькулятора в merge стадию, и сами ситуации похожи.
            return delegate.calculate(onePerKey(oldSnp, table.entityKey(), Collections.emptyList(), desc(CTL_LOADING)),
                changes, tableInfoFactory, dao, targetSchemaName, partitionSpecs);
        }
        // 1. Убираем будущие версии: в closed их нет, докладываем туда только открытые, а потом добавляем закрытых
        //    соседок открытых ключей из OPEN_SNP — и нигде не возникает будущего. ESSA-версия тоже не писала будущего.
        // 2. Спасаем закрытых соседок открытых ключей из OPEN_SNP: RT-FW делает тупой джоин, который забивает на то,
        //    что эти соседки вообще-то закрыты. Поэтому докладываем их из aside уже после всех дел.
        // 3. Работа идёт только с очищенными открытыми, грязных открытых просто нет: пользуемся благами калькулятора
        //    и забираем таблицу, им любезно оставленную.
        Dataset<Row> snp = SparkSession.active().table(table.tmpTable(OPEN_SNP));
        Column open = col(table.partitionColumn()).equalTo(lit(table.openPartition()));
        Column future = col(table.partitionColumn()).gt(lit(table.partitionOf(LoadDate.sqlDate().toString())));
        Dataset<Row> closed = oldSnp.filter(not(open).and(not(future)));

        Dataset<Row> openKeys = snp.select(cols(table.versionKey()));
        Dataset<Row> aside = closed.join(openKeys, scalaSeq(table.versionKey()), "left_semi");
        snp = closed.join(openKeys, scalaSeq(table.versionKey()), "left_anti").unionByName(snp);
        return delegate.calculate(snp, changes, tableInfoFactory, dao, targetSchemaName, partitionSpecs)
            .unionByName(aside);
    }
}
