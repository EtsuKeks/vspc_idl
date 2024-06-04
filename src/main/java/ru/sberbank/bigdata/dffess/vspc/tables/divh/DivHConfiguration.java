package ru.sberbank.bigdata.dffess.vspc.tables.divh;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEtlConfiguration;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHistSnp;
import ru.sberbank.bigdata.essa.common.slave.move.AfterCutDateGenerator;
import ru.sberbank.bigdata.essa.common.slave.move.MovingTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;
import ru.sberbank.bigdata.essa.common.slave.move.AllMovingGenerator;
import ru.sberbank.bigdata.essa.common.slave.move.HistMovingGenerator;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHistDiff;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVH;

public class DivHConfiguration extends VspcEtlConfiguration {
    @Override
    public String getTableName() {
        return DIVH;
    }

    @Override
    public EntityTransformer getEntityTransformer() {
        return new DivHTransformer();
    }

    @Override
    public DataLoader getDataLoader() {
        //return new DataLoaderHistDiff();
        return new DataLoaderHistSnp();
    }///DataLoaderHubSnp

    @Override
    public MovingTablesGenerator getMovingTablesGenerator() {
        return new HistMovingGenerator();
    }
}
