package ru.sberbank.bigdata.dffess.vspc.compat;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import ru.sberbank.bigdata.sl.rt.publisher.CalculationMode;
import ru.sberbank.bigdata.sl.rt.publisher.CalculationModes;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.getDatabaseLocationPath;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.scalaSeq;

// Общие помощники слоя совместимости: одна открытая версия на ключ, строки diff, соединения по ключам, запись в stgtmp.
public final class CompatSupport {

    private static final String KEY_RN = "_compat_key_rn";

    // Открытые версии снимка, по одной на ключ: пишет калькулятор изменений стадии, читает её же merge.
    public static final String OPEN_SNP = "_compat_open_snp";

    private CompatSupport() {
    }

    public static Column[] cols(List<String> names, Column... extra) {
        return Stream.concat(names.stream().map(c -> col(c)), Arrays.stream(extra)).toArray(Column[]::new);
    }

    // Режим публикации (publishChecked): HARD — единственный режим RT-FW; способ SPARK_REBALANCE — раскладка по файлам
    // через hint("rebalance"), без включённого AQE RT-FW сам вернётся к SALTING. Им же пишем и свои staging таблицы.
    public static final CalculationMode PUBLISH_MODE =
        new CalculationMode(CalculationModes.HARD).withApproach(CalculationMode.Approach.SPARK_REBALANCE);

    // Staging таблица, которая пересобирается целиком на каждой загрузке (черновики, кэш закрытий, подготовленный
    // срез источника — на примере vspc_idl: chg_div_h), — тем же способом, что RT-FW пишет свои временные таблицы:
    // раскладка режимом публикации, затем таблица с данными создаётся заново (старая удаляется вместе с файлами) в
    // папке схемы. Возвращает чтение записанного.
    public static Dataset<Row> writeWorkTable(String database, String table, Dataset<Row> ds) {
        SparkSession spark = SparkSession.active();
        PublisherDao dao = new PublisherDao(spark, PUBLISH_MODE);
        TableInfo ti = new TableInfo(database, table, getDatabaseLocationPath(spark, database));
        dao.writeTable(ti, scalaSeq(Collections.<String>emptyList()), ds);
        return dao.readTable(ti);
    }

    // То же по полному имени "схема.таблица".
    public static Dataset<Row> writeWorkTable(String qualifiedTable, Dataset<Row> ds) {
        int dot = qualifiedTable.indexOf('.');
        return writeWorkTable(qualifiedTable.substring(0, dot), qualifiedTable.substring(dot + 1), ds);
    }

    public static Column isLeg(String ctlDo, String ctlAction) {
        return col(CTL_DO).equalTo(lit(ctlDo)).and(col(CTL_ACTION).equalTo(lit(ctlAction)));
    }

    // Join по списку ключей переносит ключи в начало — возвращаем исходный порядок колонок левой таблицы.
    public static Dataset<Row> join(Dataset<Row> left, Dataset<Row> right, List<String> keys, String type) {
        return left.join(right, scalaSeq(keys), type).select(cols(Arrays.asList(left.columns())));
    }

    // Одна строка на ключ key, остальные строки ключа отбрасываются. Порядок внутри ключа — сначала first (если
    // задано), затем хэш строки без колонок skip, чтобы выбор не зависел от того, как строки лежат на кластере. NULL 
    // заменяется на "\u0000": xxhash64 пропускает NULL ((NULL,'a') и ('a',NULL) дали бы один хэш), а с '' NULL совпал 
    // бы с пустой строкой.
    public static Dataset<Row> onePerKey(Dataset<Row> ds, List<String> key, List<String> skip, Column... first) {
        Column[] vals = Arrays.stream(ds.columns()).filter(c -> !skip.contains(c))
            .map(c -> coalesce(col(c).cast("string"), lit("\u0000"))).toArray(Column[]::new);
        Column[] order = Stream.concat(Arrays.stream(first), Stream.of(xxhash64(vals))).toArray(Column[]::new);
        WindowSpec w = Window.partitionBy(cols(key)).orderBy(order);
        return ds.withColumn(KEY_RN, row_number().over(w)).filter(col(KEY_RN).equalTo(1)).drop(KEY_RN);
    }
}
