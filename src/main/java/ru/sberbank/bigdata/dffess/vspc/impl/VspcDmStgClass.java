package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.dffess.vspc.loading.DataLoaderCreator;
import ru.sberbank.bigdata.essa.common.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.templates.DmStgClass;
import ru.sberbank.bigdata.essa.common.templates.EntityProxyRelation;
import ru.sberbank.bigdata.essa.common.templates.TransformConfiguration;
import ru.sberbank.hermes.core.dmbuilder.config.ModuleConfiguration;

public class VspcDmStgClass extends DmStgClass {
    @Override
    protected DataLoader getDataLoader(String tgtTable) {
        return DataLoaderCreator.getDataLoader(tgtTable);
    }

    @Override
    protected TransformConfiguration getTransformConfiguration(ModuleConfiguration moduleConfiguration) {
        return new VspcTransformConfiguration(moduleConfiguration);
    }

    @Override
    protected EntityProxyRelation getEntityProxyRelation() {
        return null;
    }
}
