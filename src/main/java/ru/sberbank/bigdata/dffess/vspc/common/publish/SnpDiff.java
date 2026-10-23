package ru.sberbank.bigdata.dffess.vspc.common.publish;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public final class SnpDiff {

    public final Dataset<Row> snp;
    public final Dataset<Row> diff;

    public SnpDiff(Dataset<Row> snp, Dataset<Row> diff) {
        this.snp = snp;
        this.diff = diff;
    }
}