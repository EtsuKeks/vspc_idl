package ru.sberbank.bigdata.dffess.vspc.tables.chgdivh;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEtlConfiguration;
import ru.sberbank.bigdata.essa.common.slave.app.EtlLoadingStrategy;
import ru.sberbank.bigdata.essa.common.slave.compress.CompressedTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.compress.EmptyCompressedGenerator;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.slave.loading.EmptyDataLoader;
import ru.sberbank.bigdata.essa.common.slave.move.EmptyMovingGenerator;
import ru.sberbank.bigdata.essa.common.slave.move.MovingTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.CHG_DIVH;

public class ChgDivHConfiguration extends VspcEtlConfiguration {

    @Override
    public String getTableName() {
        return CHG_DIVH;
    }

    @Override
    public EntityTransformer getEntityTransformer() {
        return new ChgDivHTransformer();
    }

    @Override
    public EtlLoadingStrategy getEtlLoadingStrategy() {
        return new EtlLoadingStrategy() {

            @Override
            public DataLoader getDataLoader() {
                return new EmptyDataLoader();
            }

            @Override
            public CompressedTablesGenerator getCompressedTablesGenerator() {
                return new EmptyCompressedGenerator();
            }

            @Override
            public MovingTablesGenerator getMovingTablesGenerator() {
                return new EmptyMovingGenerator();
            }
        };
    }
}
