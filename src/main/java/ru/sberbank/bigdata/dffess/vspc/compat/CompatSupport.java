package ru.sberbank.bigdata.dffess.vspc.compat;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SchemaName;
import ru.sberbank.bigdata.sl.rt.publisher.dto.SnapshotChanges;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfo;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfoFactory;
import ru.sberbank.bigdata.sl.rt.util.RtfwLogger;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_ACTION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_DO;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_LOADING;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.castDataset;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.scalaSeq;

/** Общие помощники слоя совместимости. */
public final class CompatSupport {

    private static final String LOG_PREFIX = "[vspc-compat] ";
    private static final String HN = "_vspc_h_new";
    public static final String ST = "_vspc_st";
    public static final String RN = "_vspc_rn";
    public static final String SRC = "_vspc_src";
    public static final String NEW = "NEW";
    public static final String OLD = "OLD";
    public static final String EQUALS = "EQUALS";
    public static final String NOTEQUALS = "NOTEQUALS";

    private CompatSupport() {
    }

    /** Вендор пробрасывает TransformationException как TECH_ERROR трансформации. */
    public static TransformationException fail(String message) {
        LogMessage lm = new LogMessage("", LogType.TECH_ERROR, LOG_PREFIX + message);
        RtfwLogger.log(lm);
        return new TransformationException(lm);
    }

    public static void info(String message) {
        RtfwLogger.log(new LogMessage("", LogType.TECH_INFO, LOG_PREFIX + message));
    }

    public static Column[] cols(List<String> names, Column... extra) {
        return Stream.concat(names.stream().map(c -> col(c)), Arrays.stream(extra)).toArray(Column[]::new);
    }

    /** Приземление в stgtmp, как вендорский landStgTmpDataset (имя уникально для таблицы и спецификации партиций). */
    public static Dataset<Row> land(PublisherDao dao, TableInfoFactory tif, List<Map<String, String>> specs,
                                    String suffix, Dataset<Row> ds) {
        TableInfo ti = tif.createTmpTableInfo(SchemaName.STGTMP, suffix, specs);
        dao.writeTable(ti, scalaSeq(Collections.<String>emptyList()), ds);
        return dao.readTable(ti);
    }

    /** Строки в схеме ноги с метками (ctl_do, ctl_action). */
    public static Dataset<Row> leg(Dataset<Row> rows, StructType legSchema, String ctlDo, String ctlAction) {
        return castDataset(rows, legSchema)
                .withColumn(CTL_DO, lit(ctlDo))
                .withColumn(CTL_ACTION, lit(ctlAction));
    }

    public static Column isLeg(String ctlDo, String ctlAction) {
        return col(CTL_DO).equalTo(lit(ctlDo)).and(col(CTL_ACTION).equalTo(lit(ctlAction)));
    }

    /** semi/anti join по ключам с eqNullSafe: NULL-ключи — одна группа, как окно 3.0 (SnpDataLoader.java:181). */
    public static Dataset<Row> nullSafeJoin(Dataset<Row> left, Dataset<Row> right, List<String> keys, String type) {
        Column cond = lit(true);
        for (String k : keys) {
            right = right.withColumnRenamed(k, "_vspc_r_" + k);
            cond = cond.and(col(k).eqNullSafe(col("_vspc_r_" + k)));
        }
        return left.join(right, cond, type);
    }

    /** Join по Seq переносит ключи в начало: возврат исходного порядка колонок. */
    public static Dataset<Row> join(Dataset<Row> left, Dataset<Row> right, List<String> keys, String type) {
        return left.join(right, scalaSeq(keys), type).select(cols(Arrays.asList(left.columns())));
    }

    /** DV-03, DV-79, DV-81 (E/slave/loading/SnpDataLoader.java:181): bk групп окна 3.0 — &gt;1 строки во входе или snp; NULL-bk — со входом или &gt;nullSnp строк snp. */
    public static Dataset<Row> keys30(CompatTables.Table t, Dataset<Row> incr, Dataset<Row> snp, int nullSnp) {
        Column ni = col("_vspc_ni");
        Column no = col("_vspc_no");
        return incr.select(cols(t.bk(), lit(1).as("_vspc_ni"), lit(0).as("_vspc_no")))
                .unionByName(snp.select(cols(t.bk(), lit(0).as("_vspc_ni"), lit(1).as("_vspc_no"))))
                .groupBy(cols(t.bk())).agg(sum(ni).as("_vspc_ni"), sum(no).as("_vspc_no"))
                .filter(ni.gt(1).or(no.gt(1)).or(not(t.bkNotNull()).and(ni.geq(1).or(no.gt(nullSnp)))))
                .select(cols(t.bk()));
    }

    /** Окно 3.0 по строкам групп окна (E/slave/loading/SnpDataLoader.java:181-204): snp OLD + вход NEW, rn по desc(ctl_loading), asc(record_status). */
    public static Dataset<Row> window30(CompatTables.Table t, Dataset<Row> incr, Dataset<Row> snp, int loadingId, LocalDateTime now) {
        Dataset<Row> u = snp.withColumn(ST, lit(OLD))
                .unionByName(SnapshotChanges.enrichedWithTechnicalFields(castDataset(incr, snp.schema()), loadingId, now)
                        .withColumn(ST, lit(NEW)))
                .withColumn(HN, t.hash30())
                .withColumn(SRC, col(ST));
        WindowSpec w = Window.partitionBy(cols(t.bk())).orderBy(desc(CTL_LOADING), asc(ST));
        Column another = when(st(NEW), lead(col(HN), 1).over(w)).otherwise(lag(col(HN), 1).over(w));
        return u.withColumn(RN, row_number().over(w))
                .withColumn(ST, when(another.equalTo(col(HN)), lit(EQUALS))
                        .when(another.notEqual(col(HN)), lit(NOTEQUALS)).otherwise(col(ST)));
    }

    public static Column st(String status) {
        return col(ST).equalTo(lit(status));
    }

    public static Column rn(int n) {
        return col(RN).equalTo(n);
    }

    /** Первая строка группы окна 3.0: NEW или NOTEQUALS rn1. */
    public static Column first30() {
        return st(NEW).or(st(NOTEQUALS).and(rn(1)));
    }

    /** Новый снап группы окна 3.0 (без OLD rn1): первая строка и EQUALS rn2. */
    public static Column snp30() {
        return first30().or(st(EQUALS).and(rn(2)));
    }
}
