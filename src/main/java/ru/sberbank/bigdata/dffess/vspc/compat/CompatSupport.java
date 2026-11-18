package ru.sberbank.bigdata.dffess.vspc.compat;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfo;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfoFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_LOADING;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.scalaSeq;

// Общие помощники слоя совместимости: одна открытая версия на ключ, строки diff, соединения по ключам, запись в stgtmp.
public final class CompatSupport {

    private static final String KEY_RN = "_compat_key_rn";

    private CompatSupport() {
    }

    public static Column[] cols(List<String> names, Column... extra) {
        return Stream.concat(names.stream().map(c -> col(c)), Arrays.stream(extra)).toArray(Column[]::new);
    }

    // Промежуточная запись в stgtmp тем же способом, что у RT-FW: имя уникально для таблицы
    // и спецификации партиций. Дальше работаем с записанным, а не пересчитываем план заново.
    public static Dataset<Row> land(PublisherDao dao, TableInfoFactory tif, List<Map<String, String>> specs,
                                    String suffix, Dataset<Row> ds) {
        TableInfo ti = tif.createTmpTableInfo(SchemaName.STGTMP, suffix, specs);
        dao.writeTable(ti, scalaSeq(Collections.<String>emptyList()), ds);
        return dao.readTable(ti);
    }

    public static Column isLeg(String ctlDo, String ctlAction) {
        return col(CTL_DO).equalTo(lit(ctlDo)).and(col(CTL_ACTION).equalTo(lit(ctlAction)));
    }

    // Join по списку ключей переносит ключи в начало — возвращаем исходный порядок колонок левой таблицы.
    public static Dataset<Row> join(Dataset<Row> left, Dataset<Row> right, List<String> keys, String type) {
        return left.join(right, scalaSeq(keys), type).select(cols(Arrays.asList(left.columns())));
    }

    // Одна открытая версия на ключ. Если в snp у ключа больше одной строки — остаётся самая
    // свежая по ctl_loading, при равном — с наименьшим хэшем всей строки, чтобы выбор не зависел от того, как строки
    // лежат на кластере. Остальные строки ключа отбрасываются, как ESSA-версия теряла лишние строки ключа.
    public static Dataset<Row> oneOpenPerKey(CompatTables.Table t, Dataset<Row> snp) {
        Column[] vals = Arrays.stream(snp.columns())
            .map(c -> coalesce(col(c).cast("string"), lit(""))).toArray(Column[]::new);
        WindowSpec w = Window.partitionBy(cols(t.entityKey())).orderBy(desc(CTL_LOADING), xxhash64(vals));
        return snp.withColumn(KEY_RN, row_number().over(w)).filter(col(KEY_RN).equalTo(1)).drop(KEY_RN);
    }
}
