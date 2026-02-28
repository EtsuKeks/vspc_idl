package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName;
import ru.sberbank.bigdata.essa.common.structure.MetadataKeeper;

import static org.apache.spark.sql.functions.col;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_DIV_RELATION_SK;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.FULLDIVCODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.*;
import static ru.sberbank.bigdata.essa.common.utils.Expression.stringList;

public class VspcMetadataKeeper extends MetadataKeeper {

    @Override
    public void build() {
        addTable(CHG_DIVH, stringList(FULLDIVCODE));
        addTable(ADDR, stringList(ColumnsName.ADDR_SK));
        addTable(ADDRH, stringList(ColumnsName.ADDR_SK), col(END_DT), PARTITION_COLUMN_MONTH, stringList(START_DT, END_DT, PARTITION_COLUMN_MONTH));
        addTable(DIV, stringList(ColumnsName.DIV_SK));
        addTable(DIVH, stringList(ColumnsName.DIV_SK), col(END_DT), PARTITION_COLUMN_MONTH, stringList(START_DT, END_DT, PARTITION_COLUMN_MONTH));
        addTable(DIVTYPE, stringList(ColumnsName.DIV_SK), col(END_DT), PARTITION_COLUMN_MONTH, stringList(START_DT, END_DT, PARTITION_COLUMN_MONTH));
        addTable(TERRDIVTYPE, stringList(ColumnsName.TERR_DIV_TYPE_SK));
        addTable(TIMEZONE, stringList(ColumnsName.TIME_ZONE_SK));
        addTable(DIVDIVRELATION, stringList(DIV_DIV_RELATION_SK), col(END_DT), PARTITION_COLUMN_MONTH, stringList(START_DT, END_DT, PARTITION_COLUMN_MONTH));
    }
}
