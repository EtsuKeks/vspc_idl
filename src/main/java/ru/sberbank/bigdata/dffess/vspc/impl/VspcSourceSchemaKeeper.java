package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.essa.common.master.app.StandInventory;
import ru.sberbank.bigdata.essa.common.structure.SourceSchemaKeeper;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.CHG_DIVH;

public class VspcSourceSchemaKeeper implements SourceSchemaKeeper {

    @Override
    public String getSnpSrcSchema(String instanceCode, String tableName, StandInventory standInventory) {
        if (tableName.equals(CHG_DIVH)) {
            return "usl_vspc_idl_stg";
        }
        return "usl_vspc_idl_in_snp";
    }

    @Override
    public String getDiffSrcSchema(String instanceCode, String tableName, StandInventory standInventory) {
        return "usl_vspc_idl_diff";
    }
}
