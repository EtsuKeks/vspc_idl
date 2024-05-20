package ru.sberbank.bigdata.dffess.vspc.tables.timezone;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEtlConfiguration;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHistSnp;
import ru.sberbank.bigdata.essa.common.slave.move.AfterCutDateGenerator;
import ru.sberbank.bigdata.essa.common.slave.move.MovingTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;
import ru.sberbank.bigdata.essa.common.slave.move.AllMovingGenerator;
import ru.sberbank.bigdata.essa.common.slave.move.HistMovingGenerator;
import ru.sberbank.bigdata.essa.common.slave.loading.DataLoaderHubSnp;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TIMEZONE;

public class TimeZoneConfiguration extends VspcEtlConfiguration {
    @Override
    public String getTableName() {
        return TIMEZONE;
    }

    @Override
    public EntityTransformer getEntityTransformer() {
        return new TimeZoneTransformer();
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
