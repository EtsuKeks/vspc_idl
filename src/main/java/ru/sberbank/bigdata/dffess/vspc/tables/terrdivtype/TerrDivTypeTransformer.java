package ru.sberbank.bigdata.dffess.vspc.tables.terrdivtype;

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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_M_SFS_0002_SFSDIVISION;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TERRDIVTYPE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.SID;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.DELETED_FLAG;

public class TerrDivTypeTransformer implements EntityTransformer {

    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> terrdivtype = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(EBX_REP_M_SFS_0002_SFSDIVISION), EBX_REP_M_SFS_0002_SFSDIVISION);
        terrdivtype = terrdivtype
                .select(
                        col(DIVISIONTYPE00).as(SID),
                        lit(DATA_PRODUCT).as(TERR_DIV_TYPE_DP_CODE),
                        col(DIVISIONTYPE00).as(DIV_TYPE_CODE),
                        col(DIVISIONTYPE00).as(TERR_DIV_TYPE_SK),
                        lit(false).as(DIRTY_FLAG)
                ).distinct();

        terrdivtype = terrdivtype.select(
                lit(false).as(DELETED_FLAG),
                col(TERR_DIV_TYPE_SK).as(SID),
                lit(DATA_PRODUCT).as(TERR_DIV_TYPE_DP_CODE),
                col(DIV_TYPE_CODE).as(DIV_TYPE_CODE),
                col(TERR_DIV_TYPE_SK).as(TERR_DIV_TYPE_SK),
                lit(false).as(DIRTY_FLAG)
        ).distinct();

        terrdivtype = skClientWrapper.generateSk(terrdivtype, TERRDIVTYPE, transformConfiguration.getLoadMode(), transformConfiguration);

        return terrdivtype;
    }
}
