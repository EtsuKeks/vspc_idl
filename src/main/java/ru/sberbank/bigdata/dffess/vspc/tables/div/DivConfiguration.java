package ru.sberbank.bigdata.dffess.vspc.tables.div;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEtlConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.addr.AddrTransformer;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHubSnp;
import ru.sberbank.bigdata.essa.common.slave.move.AfterCutDateGenerator;
import ru.sberbank.bigdata.essa.common.slave.move.MovingTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIV;

public class DivConfiguration extends VspcEtlConfiguration {
    @Override
    public String getTableName() {
        return DIV;
    }

    @Override
    public EntityTransformer getEntityTransformer() {
        return new DivTransformer();
    }

    @Override
    public DataLoader getDataLoader() {
        return new DataLoaderHubSnp();
    }///DataLoaderHubSnp

    @Override
    public MovingTablesGenerator getMovingTablesGenerator() {
        return new AfterCutDateGenerator();
    }
}
