package ru.sberbank.bigdata.dffess.vspc.tables.addr;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEtlConfiguration;
import ru.sberbank.bigdata.essa.common.slave.app.EtlLoadingStrategy;
import ru.sberbank.bigdata.essa.common.slave.compress.CompressedTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.compress.SimpleCompressedTableGenerator;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHubDiff;
import ru.sberbank.bigdata.essa.common.slave.move.AllMovingGenerator;
import ru.sberbank.bigdata.essa.common.slave.move.MovingTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.ADDR;

public class AddrConfiguration extends VspcEtlConfiguration {

    @Override
    public String getTableName() {
        return ADDR;
    }

    @Override
    public EntityTransformer getEntityTransformer() {
        return new AddrTransformer();
    }

    @Override
    public EtlLoadingStrategy getEtlLoadingStrategy() {
        return new EtlLoadingStrategy() {

            @Override
            public DataLoader getDataLoader() {
                return new DataLoaderHubDiff();
            }

            @Override
            public CompressedTablesGenerator getCompressedTablesGenerator() {
                return new SimpleCompressedTableGenerator();
            }

            @Override
            public MovingTablesGenerator getMovingTablesGenerator() {
                return new AllMovingGenerator();
            }
        };
    }
}
