package ru.sberbank.bigdata.dffess.vspc.common.publish;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.Logger;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Transformation;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.*;

public abstract class BaseStrategy {

    public static final String HASH_COLUMN = "hash";
    public static final String RECORD_STATUS = "record_status";
    public static final String RN_COLUMN = "rn";
    public static final String PREV_STATE_OF_DEL = "prev_state_of_del";
    private static final Set<String> ADDITIONAL_FIELDS = new HashSet<>(Arrays.asList(
            RN_COLUMN, RECORD_STATUS, HASH_COLUMN, "string_hash",
            "another_del_flag", "another_ctl_loading", "another_ctl_validfrom", PREV_STATE_OF_DEL));

    protected final Transformation t;
    protected final Spec spec;

    protected List<String> excludeHashColumns = Collections.emptyList();

    protected BaseStrategy(Transformation t, Spec spec) {
        this.t = t;
        this.spec = spec;
    }

    public final SnpDiff prepare(Dataset<Row> result) {
        App.LoadMode mode = t.ctx().getLoadMode();
        validateResultContract(result, mode);
        if (mode == App.LoadMode.INITIAL) {
            return prepareInit(result);
        }
        return prepareIncr(result);
    }

    protected abstract SnpDiff prepareInit(Dataset<Row> result);

    protected abstract SnpDiff prepareIncr(Dataset<Row> result);

    protected List<String> requiredInputFields(App.LoadMode mode) {
        return new ArrayList<>(Arrays.asList(spec.pk));
    }

    private void validateResultContract(Dataset<Row> result, App.LoadMode mode) {
        Set<String> present = Arrays.stream(result.columns()).map(String::toLowerCase).collect(toSet());
        List<String> missing = requiredInputFields(mode).stream()
                .filter(name -> !present.contains(name.toLowerCase()))
                .collect(toList());
        if (!missing.isEmpty()) {
            String message = "Трансформация " + t.getClass().getSimpleName()
                    + " не положила в результат обязательные поля " + missing
                    + " (контракт стратегии " + getClass().getSimpleName()
                    + ", режим " + mode + ", таблица " + spec.code + ")";
            new Logger(app(), BaseStrategy.class).logError(message);
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, message));
        }
    }

    protected long loadingId() {
        return (long) t.ctx().getLoadingId();
    }

    protected String ctlValidFromNow() {
        return DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN).format(LocalDateTime.now());
    }

    protected App app() {
        return ((BaseTransformation) t).getApp();
    }

    protected Dataset<Row> readOpenSnp() {
        Dataset<Row> df = readSnp();
        if (hasColumn(df, END_DT_FIELD_NAME)) {
            return df.where(col(END_DT_FIELD_NAME).equalTo(lit(PLUS_INFINITY_DT).cast("date")));
        }
        return df;
    }

    protected Dataset<Row> readSnp() {
        return app().getSparkSession().sql("select * from " + BaseTransformation.SNP_SCHEMA + "." + spec.code);
    }

    protected Dataset<Row> withRecordStatus(Dataset<Row> df, String status) {
        return df.withColumn(RECORD_STATUS, lit(status));
    }

    protected Dataset<Row> withTechFields(Dataset<Row> df) {
        if (!hasColumn(df, CTL_LOADING)) {
            df = df.withColumn(CTL_LOADING, lit(loadingId()).cast("bigint"));
        }
        if (!hasColumn(df, CTL_VALIDFROM)) {
            df = df.withColumn(CTL_VALIDFROM, lit(ctlValidFromNow()).cast("timestamp"));
        }
        return df;
    }

    protected boolean hasColumn(Dataset<Row> df, String name) {
        return Arrays.asList(df.columns()).contains(name);
    }

    protected List<String> hashColumns(Dataset<Row> reference, Dataset<Row> df) {
        Set<String> dfColumns = new HashSet<>(Arrays.asList(df.columns()));
        return Arrays.stream(reference.columns())
                .filter(dfColumns::contains)
                .filter(this::includedIntoHash)
                .collect(toList());
    }

    private boolean includedIntoHash(String name) {
        return !name.startsWith("ctl") && !excludeHashColumns.contains(name) && !ADDITIONAL_FIELDS.contains(name);
    }

    protected Dataset<Row> withHash(Dataset<Row> df, List<String> columns) {
        Column acc = lit("");
        for (String name : columns) {
            acc = concat(acc, when(col(name).isNotNull(), col(name).cast("string")).otherwise(lit("")));
        }
        return df.withColumn(HASH_COLUMN, xxhash64(acc));
    }

    protected Dataset<Row> alignToReference(Dataset<Row> reference, Dataset<Row> df) {
        Set<String> dfColumnsLower = Arrays.stream(df.columns()).map(String::toLowerCase).collect(toSet());
        for (String name : reference.columns()) {
            if (!dfColumnsLower.contains(name.toLowerCase())) {
                df = df.withColumn(name, lit(null).cast(reference.schema().apply(name).dataType()));
            }
        }
        return df.select(Arrays.stream(reference.columns()).map(name -> col(name)).toArray(Column[]::new));
    }

    protected Dataset<Row> dropBadClosures(Dataset<Row> df) {
        if (!hasColumn(df, END_DT_FIELD_NAME) || !hasColumn(df, START_DT_FIELD_NAME)) {
            return df;
        }
        return df.where(col(END_DT_FIELD_NAME).isNull().or(col(END_DT_FIELD_NAME).geq(col(START_DT_FIELD_NAME))));
    }

    protected Column p1monthCol() {
        return date_format(col(END_DT_FIELD_NAME), "yyyyMM");
    }
}