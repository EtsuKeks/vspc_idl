package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.dffess.vspc.constant.TablesName;
import ru.sberbank.bigdata.essa.common.structure.SourceSchemaKeeper;

public class VspcSourceSchemaKeeper implements SourceSchemaKeeper {

    @Override
    public String getSnpSrcSchema(String instanceCode, String tableName) {
        return "usl_vspc_idl_in_snp";
    }

    @Override
    public String getDiffSrcSchema(String instanceCode, String tableName) {
        return "usl_vspc_idl_diff";
    }
}