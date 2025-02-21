package ru.sberbank.bigdata.dffess.vspc.tables.addrh;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEtlConfiguration;
import ru.sberbank.bigdata.essa.common.slave.app.EtlLoadingStrategy;
import ru.sberbank.bigdata.essa.common.slave.compress.CompressedTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.compress.SimpleCompressedTableGenerator;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHistDiff;
import ru.sberbank.bigdata.essa.common.slave.move.HistMovingGenerator;
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
    public EtlLoadingStrategy getEtlLoadingStrategy() {
        return new EtlLoadingStrategy() {

            @Override
            public DataLoader getDataLoader() {
                return new DataLoaderHistDiff();
            }

            @Override
            public CompressedTablesGenerator getCompressedTablesGenerator() {
                return new SimpleCompressedTableGenerator();
            }

            @Override
            public MovingTablesGenerator getMovingTablesGenerator() {
                return new HistMovingGenerator();
            }
        };
    }
}
