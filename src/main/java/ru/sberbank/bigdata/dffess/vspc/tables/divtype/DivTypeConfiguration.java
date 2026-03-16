package ru.sberbank.bigdata.dffess.vspc.tables.divtype;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEtlConfiguration;
import ru.sberbank.bigdata.essa.common.slave.app.EtlLoadingStrategy;
import ru.sberbank.bigdata.essa.common.slave.compress.CompressedTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.compress.SimpleCompressedTableGenerator;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHistSnp;
import ru.sberbank.bigdata.essa.common.slave.move.HistMovingGenerator;
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
    public EtlLoadingStrategy getEtlLoadingStrategy() {
        return new EtlLoadingStrategy() {

            @Override
            public DataLoader getDataLoader() {
                return new DataLoaderHistSnp();
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
