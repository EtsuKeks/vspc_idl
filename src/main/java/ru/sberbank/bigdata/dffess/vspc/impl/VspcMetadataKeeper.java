package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.essa.common.structure.MetadataKeeper;
import ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName;

import java.util.Collections;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_D_SFS_0002_SFSDIVISION;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_M_SFS_0002_SFSDIVISION;

public class VspcMetadataKeeper extends MetadataKeeper {
    @Override
    public void build() {
        addTable(EBX_REP_M_SFS_0002_SFSDIVISION, Collections.singletonList(ColumnsName.IFRS_ASST_BSNS_MODEL_TYPE_SK),null, Collections.EMPTY_LIST);
        addTable(EBX_REP_D_SFS_0002_SFSDIVISION, Collections.singletonList(ColumnsName.IFRS_SUBSQNT_ACC_MODEL_TYPE_SK),null, Collections.EMPTY_LIST);
    }
}
