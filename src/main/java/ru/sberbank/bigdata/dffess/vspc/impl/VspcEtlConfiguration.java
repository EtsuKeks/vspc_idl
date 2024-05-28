package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.essa.common.slave.app.EtlConfiguration;
import ru.sberbank.bigdata.essa.common.slave.compress.CompressedTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.compress.SimpleCompressedTableGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityProxyRelation;
import ru.sberbank.bigdata.essa.common.slave.templates.TransformConfiguration;

public abstract class VspcEtlConfiguration implements EtlConfiguration {

    @Override
    public TransformConfiguration getTransformConfiguration() {
        return new VspcTransformConfiguration();
    }

    @Override
    public EntityProxyRelation getEntityProxyRelation() {
        return new VspcEntityProxyRelation();
    }

    @Override
    public CompressedTablesGenerator getCompressedTablesGenerator() {
        return new SimpleCompressedTableGenerator();
    }
}
