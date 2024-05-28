package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName;
import ru.sberbank.bigdata.essa.common.structure.MetadataKeeper;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.spark.sql.functions.col;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.*;

public class VspcMetadataKeeper extends MetadataKeeper {
    @Override
    public void build() {
        addTable(ADDR, Collections.singletonList(ColumnsName.ADDR_SK), null, Collections.EMPTY_LIST);
        addTable(ADDRH, Collections.singletonList(ColumnsName.ADDR_SK), col(END_DT), Arrays.asList(START_DT, END_DT, PARTITION_COLUMN_MONTH));
        addTable(DIV, Collections.singletonList(ColumnsName.DIV_SK), null, Collections.EMPTY_LIST);
        addTable(DIVH, Collections.singletonList(ColumnsName.DIV_SK), col(END_DT), Arrays.asList(START_DT, END_DT, PARTITION_COLUMN_MONTH));
        addTable(DIVTYPE, Collections.singletonList(ColumnsName.DIV_SK), col(END_DT), Arrays.asList(START_DT, END_DT, PARTITION_COLUMN_MONTH));
        addTable(TERRDIVTYPE, Collections.singletonList(ColumnsName.TERR_DIV_TYPE_SK), null, Collections.EMPTY_LIST);
        addTable(TIMEZONE, Collections.singletonList(ColumnsName.TIME_ZONE_SK), null, Collections.EMPTY_LIST);
    }
}
