package ru.sberbank.bigdata.dffess.vspc.tables.chgdivh;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.essa.common.logger.LoggerWrapper;
import ru.sberbank.bigdata.essa.common.slave.app.SparkWrapper;
import ru.sberbank.bigdata.essa.common.slave.hdfs.HdfsService;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityProxyRelation;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;
import ru.sberbank.bigdata.essa.common.slave.templates.SkClientWrapper;
import ru.sberbank.bigdata.essa.common.slave.templates.TransformConfiguration;
import ru.sberbank.bigdata.essa.common.utils.EntityStaticService;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.CHG_DIVH;
import static ru.sberbank.bigdata.dffess.vspc.tables.chgdivh.ChgDivHSql.getPreselectSQL;

public class ChgDivHTransformer implements EntityTransformer {

    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> preselect = sparkWrapper.fromSql(getPreselectSQL(transformConfiguration.getLoadMode())).distinct();
        String stgSchema = transformConfiguration.getStgSchema();
        String tableName = CHG_DIVH;
        loggerWrapper.info("TruncateTable table " + stgSchema + "." + tableName);
        sparkWrapper.truncateTable(stgSchema, tableName);
        loggerWrapper.info("Writing table " + stgSchema + "." + tableName);
        Dataset<Row> emptyPrototype = sparkWrapper.getDataset(stgSchema, tableName);
        preselect = sparkWrapper.matchSchemas(emptyPrototype, preselect);
        sparkWrapper.writeDataset(preselect, stgSchema, tableName);

        return preselect;
    }
}
