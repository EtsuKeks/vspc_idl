package ru.sberbank.bigdata.sl.rt;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import ru.sberbank.bigdata.sl.rt.core.App;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation.HIST_POSTFIX;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.ABBREVIATION;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.NAME00;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.TIMEDESIGNATION;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.UTCFORMAT;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_M_RFCZONETIME_0002_RFCZONETIME;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.VSPC_SNP_SRC_SCHEMA;
import static ru.sberbank.bigdata.sl.rt.reader.constants.ReaderConstants.*;
import static ru.sberbank.bigdata.sl.rt.BaseTransformationTest.spark;

public enum SourceTable {
    RFCZONETIME(VSPC_SNP_SRC_SCHEMA, EBX_REP_M_RFCZONETIME_0002_RFCZONETIME, new StructType()
            .add(CODE, StringType)
            .add(TIMEDESIGNATION, StringType)
            .add(ABBREVIATION, StringType)
            .add(NAME00, StringType)
            .add(UTCFORMAT, StringType)
            .add(CTL_LOADING, LongType)
            .add(CTL_VALIDFROM, TimestampType));

    private final String schemaName;
    private final String tableName;
    private final StructType structType;

    SourceTable(String schemaName, String tableName, StructType structType) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.structType = structType;
    }

    public void writeD0() {
        Dataset<Row> inputSnp = BaseTransformationTest.readInputCsv(structType, "d0", format("%s-%s.csv", schemaName, tableName));
        BaseTransformationTest.writeManagedTable(inputSnp, schemaName, tableName);
    }

    public void writeD1() {
        Dataset<Row> inputSnp = BaseTransformationTest.readInputCsv(structType, "d1", format("%s-%s.csv", schemaName, tableName));

        if (schemaName.contains(HIST_POSTFIX) || schemaName.contains("_diff") || schemaName.contains("_delta")) {
            BaseTransformationTest.writeManagedTableDiff(inputSnp, schemaName, tableName);
        } else {
            BaseTransformationTest.writeManagedTable(inputSnp, schemaName, tableName);
        }
    }

    public void writeEmptyTable() {
        Dataset<Row> emptyDataset = spark.createDataFrame(emptyList(), structType);
        BaseTransformationTest.writeManagedTable(emptyDataset, schemaName, tableName);
    }

    public void run(App app) {

    }
}
