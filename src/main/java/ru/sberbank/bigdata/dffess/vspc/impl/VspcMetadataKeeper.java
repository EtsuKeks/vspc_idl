package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.essa.common.structure.MetadataKeeper;
import ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName;

import java.util.Collections;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;

public class VspcMetadataKeeper extends MetadataKeeper {
    @Override
    public void build() {
        addTable(ADDR, Collections.singletonList(ColumnsName.ADDR_SK),null, Collections.EMPTY_LIST);
        addTable(ADDRH, Collections.singletonList(ColumnsName.ADDR_SK),null, Collections.EMPTY_LIST);
        addTable(DIV, Collections.singletonList(ColumnsName.DIV_SK),null, Collections.EMPTY_LIST);
        addTable(DIVH, Collections.singletonList(ColumnsName.DIV_SK),null, Collections.EMPTY_LIST);
    }
}
