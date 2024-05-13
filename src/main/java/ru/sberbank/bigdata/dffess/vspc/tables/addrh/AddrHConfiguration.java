package ru.sberbank.bigdata.dffess.vspc.tables.addrh;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEtlConfiguration;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHistSnp;
import ru.sberbank.bigdata.essa.common.slave.move.AfterCutDateGenerator;
import ru.sberbank.bigdata.essa.common.slave.move.MovingTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.ADDRH;

public class AddrHConfiguration extends VspcEtlConfiguration {

    @Override
    public String getTableName() {
        return ADDRH;
    }

    @Override
    public EntityTransformer getEntityTransformer() {
        return new AddrHTransformer();
    }

    @Override
    public DataLoader getDataLoader() {
        return new DataLoaderHistSnp();
    }///DataLoaderHubSnp

    @Override
    public MovingTablesGenerator getMovingTablesGenerator() {
        return new AllMovingGenerator();
    }
}
