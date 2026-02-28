package ru.sberbank.bigdata.dffess.vspc.tables.divdivrelation;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEtlConfiguration;
import ru.sberbank.bigdata.dffess.vspc.impl.loader.DataLoaderLinkHistDiff;
import ru.sberbank.bigdata.dffess.vspc.impl.mover.HistMovingNoCacheGenerator;
import ru.sberbank.bigdata.essa.common.slave.app.EtlLoadingStrategy;
import ru.sberbank.bigdata.essa.common.slave.compress.CompressedTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.compress.SimpleCompressedTableGenerator;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.slave.move.MovingTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVDIVRELATION;

public class DivDivRelationConfiguration extends VspcEtlConfiguration {

    @Override
    public String getTableName() {
        return DIVDIVRELATION;
    }

    @Override
    public EntityTransformer getEntityTransformer() {
        return new DivDivRelationTransformer();
    }

    @Override
    public EtlLoadingStrategy getEtlLoadingStrategy() {
        return new EtlLoadingStrategy() {

            @Override
            public DataLoader getDataLoader() {
                return new DataLoaderLinkHistDiff();
            }

            @Override
            public CompressedTablesGenerator getCompressedTablesGenerator() {
                return new SimpleCompressedTableGenerator();
            }

            @Override
            public MovingTablesGenerator getMovingTablesGenerator() {
                return new HistMovingNoCacheGenerator();
            }
        };
    }
}
