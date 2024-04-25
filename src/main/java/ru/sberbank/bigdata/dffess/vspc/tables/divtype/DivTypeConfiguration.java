package ru.sberbank.bigdata.dffess.vspc.tables.divtype;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEtlConfiguration;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHistSnp;
import ru.sberbank.bigdata.essa.common.slave.move.AfterCutDateGenerator;
import ru.sberbank.bigdata.essa.common.slave.move.MovingTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVTYPE;

public class DivTypeConfiguration extends VspcEtlConfiguration {
    @Override
    public String getTableName() {
        return DIVTYPE;
    }

    @Override
    public EntityTransformer getEntityTransformer() {
        return new DivTypeTransformer();
    }

    @Override
    public DataLoader getDataLoader() {
        return new DataLoaderHistSnp();
    }///DataLoaderHubSnp

    @Override
    public MovingTablesGenerator getMovingTablesGenerator() {
        return new AfterCutDateGenerator();
    }
}
