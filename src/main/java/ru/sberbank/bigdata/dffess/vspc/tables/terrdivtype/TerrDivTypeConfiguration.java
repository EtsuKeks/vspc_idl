package ru.sberbank.bigdata.dffess.vspc.tables.terrdivtype;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEtlConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.divtype.DivTypeTransformer;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHistSnp;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHubDiff;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHubSnp;
import ru.sberbank.bigdata.essa.common.slave.move.AfterCutDateGenerator;
import ru.sberbank.bigdata.essa.common.slave.move.MovingTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TERRDIVTYPE;

public class TerrDivTypeConfiguration extends VspcEtlConfiguration {
    @Override
    public String getTableName() {
        return TERRDIVTYPE;
    }

    @Override
    public EntityTransformer getEntityTransformer() {
        return new TerrDivTypeTransformer();
    }

    @Override
    public DataLoader getDataLoader() {
        return new DataLoaderHubSnp();
    }///DataLoaderHubSnp

    @Override
    public MovingTablesGenerator getMovingTablesGenerator() {
        return new AllMovingGenerator();
    }
}
