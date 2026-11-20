package ru.sberbank.bigdata.dffess.vspc.compat.period;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.cols;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.writeWorkTable;

// Кэш загрузчика как из ESSA ключей, пропавших из источника.
public final class LoaderCache {

    private static final String ORD = "_compat_cache_ord";
    private static final String RN = "_compat_cache_rn";
    private static final Set<String> STAGED = ConcurrentHashMap.newKeySet();

    private LoaderCache() {
    }

    // Кэш как есть: ключ сущности + end_dt; null — таблицы ещё нет.
    static Dataset<Row> read(CompatTables.Table t) {
        SparkSession spark = SparkSession.active();
        if (t.cacheTable() == null || !spark.catalog().tableExists(t.cacheTable())) {
            return null;
        }
        return spark.table(t.cacheTable()).select(cols(t.entityKey(), col(END_DT_FIELD_NAME)));
    }

    // Черновик нового кэша в stgtmp: старый кэш + закрытия пропавших ключей этой загрузки, одна строка на ключ,
    // новая побеждает (как в ESSA-реализации LoaderCache - происходит сливание строк).
    static void stage(CompatTables.Table t, Dataset<Row> closures) {
        Dataset<Row> fresh = closures.select(cols(t.entityKey(), col(END_DT_FIELD_NAME))).withColumn(ORD, lit(1));
        Dataset<Row> old = read(t);
        Dataset<Row> all = old == null ? fresh : old.withColumn(ORD, lit(2)).unionByName(fresh);
        WindowSpec w = Window.partitionBy(cols(t.entityKey())).orderBy(col(ORD));
        writeWorkTable(t.cacheDraft(),
            all.withColumn(RN, row_number().over(w)).filter(col(RN).equalTo(1)).drop(RN, ORD));
        STAGED.add(t.cacheDraft());
    }

    // После успешной публикации: на INIT — чистим кэш, на INCR — черновик этой загрузки тянем в stg - так, если шаг 
    // упал, то не закоммитив мы будем в корректном состоянии, поэтому коммитим только после успешной публикации.
    public static void commit(CompatTables.Table t, boolean init) {
        if (t.cacheTable() == null) {
            return;
        }
        SparkSession spark = SparkSession.active();
        if (init) {
            writeWorkTable(t.cacheTable(),
                spark.table(t.snpTable()).select(cols(t.entityKey(), col(END_DT_FIELD_NAME))).limit(0));
        } else if (STAGED.remove(t.cacheDraft())) {
            writeWorkTable(t.cacheTable(), spark.table(t.cacheDraft()));
        }
    }
}
