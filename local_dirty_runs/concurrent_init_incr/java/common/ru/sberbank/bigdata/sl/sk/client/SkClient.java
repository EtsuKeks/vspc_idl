package ru.sberbank.bigdata.sl.sk.client;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.sl.sk.SkCommonException;
import ru.sberbank.bigdata.sl.sk.domain.Product;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * Local stand mock of sl-sk-client 1.30.1 SkClient (same FQCN, same public API).
 * Semantics reproduced from the decompiled client (Incrementor/Initializer/SkClientSplitBy*):
 *  - registry key = (dp_code, instance_code, entity, sid); the SK is a deterministic positive hash of it,
 *    so 3.0 and 4.0 runs get identical keys for identical natural keys (like a shared SK service);
 *  - map(): per attribute "prefix@entity" reads <col>_sid, <col>_dp_code, optional <col>_instance_code,
 *    writes <col>_sk; NULL/blank dp_code, NULL or "" sid -> NULL sk; empty input -> <col>_sk = 0L;
 *  - init(): product = system properties service_data_product_code / service_instance_code;
 *    reads <entity>_sid, left-joins <entity>_sk; no non-empty sid at all -> <entity>_sk = 0L for every row.
 */
public final class SkClient implements Closeable {

    private static final SkClient INSTANCE = new SkClient();

    public static SkClient getInstance() {
        return INSTANCE;
    }

    public boolean testService(Product product, String s) {
        return true;
    }

    @Override
    public void close() {
    }

    static Column key(Column dp, Column inst, String entity, Column sid) {
        Column k = concat_ws("\u0001", coalesce(dp, lit("<null>")), coalesce(inst, lit("<null>")), lit(entity),
                sid.cast(DataTypes.StringType));
        // positive, non-zero bigint
        return abs(xxhash64(k).bitwiseAND(lit(Long.MAX_VALUE))).plus(lit(1L));
    }

    static boolean blank(String s) {
        return s == null || s.trim().isEmpty();
    }

    @SafeVarargs
    public final Map<Integer, Dataset<Row>> init(String entityCode, Dataset<Row>... datasets)
            throws SkClientException, SkCommonException {
        if (datasets == null || datasets.length < 1) {
            throw new SkClientException("Array of input datasets could not be NULL or empty");
        }
        String dp = System.getProperty("service_data_product_code");
        String inst = System.getProperty("service_instance_code");
        String phys = entityCode.replace("&", "_").replace(">", "_");
        Map<Integer, Dataset<Row>> out = new HashMap<>();
        Dataset<Row> d0 = datasets[0];
        Column sid0 = d0.col(phys + "_sid");
        boolean anyKey = !d0.select(sid0).filter(sid0.isNotNull().and(not(sid0.equalTo(lit(""))))).isEmpty();
        for (int i = 0; i < datasets.length; i++) {
            Dataset<Row> d = datasets[i];
            if (!anyKey) {
                out.put(i, d.withColumn(phys + "_sk", lit(0L)));
            } else {
                Column sid = col(phys + "_sid");
                Column sk = when(sid.isNull().or(sid.cast(DataTypes.StringType).equalTo(lit(""))), lit(null).cast(DataTypes.LongType))
                        .otherwise(key(lit(dp), blank(inst) ? lit(null).cast(DataTypes.StringType) : lit(inst), phys, sid));
                out.put(i, d.withColumn(phys + "_sk", sk));
            }
        }
        return out;
    }

    public Dataset<Row> map(Dataset<Row> ds, List<String> attributeCodes) throws SkClientException {
        return map(ds, attributeCodes, null);
    }

    public Dataset<Row> map(Dataset<Row> ds, List<String> attributeCodes, String entityCode) throws SkClientException {
        if (ds == null) {
            throw new SkClientException("Input dataset could not be NULL");
        }
        List<String> cols = Arrays.asList(ds.columns());
        Dataset<Row> res = ds;
        boolean empty = ds.isEmpty();
        for (String code : attributeCodes) {
            String colName = code.replaceAll("^@", "").replace("@", "_").replace(">", "_");
            String entity = code.substring(code.indexOf("@") + 1).replaceAll("^@", "").replace("@", "_").replace(">", "_");
            if (empty) {
                res = res.withColumn(colName + "_sk", lit(0L));
                continue;
            }
            Column sid = col(colName + "_sid");
            Column dp = col(colName + "_dp_code");
            Column inst = cols.contains(colName + "_instance_code") ? col(colName + "_instance_code") : lit(null).cast(DataTypes.StringType);
            Column dpBlank = dp.isNull().or(trim(dp.cast(DataTypes.StringType)).equalTo(lit("")));
            // instance "" (blank, not null) never matches the client's filter -> NULL
            Column instBad = inst.isNotNull().and(trim(inst.cast(DataTypes.StringType)).equalTo(lit("")));
            Column sidBad = sid.isNull().or(sid.cast(DataTypes.StringType).equalTo(lit("")));
            Column sk = when(dpBlank.or(instBad).or(sidBad), lit(null).cast(DataTypes.LongType))
                    .otherwise(key(dp.cast(DataTypes.StringType), inst.cast(DataTypes.StringType), entity, sid));
            res = res.withColumn(colName + "_sk", sk);
        }
        return res;
    }
}
