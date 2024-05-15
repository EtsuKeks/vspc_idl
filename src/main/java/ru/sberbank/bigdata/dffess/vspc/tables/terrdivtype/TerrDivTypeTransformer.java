package ru.sberbank.bigdata.dffess.vspc.tables.terrdivtype;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName;
import ru.sberbank.bigdata.dffess.vspc.constant.TablesName;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
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
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.CTL_VALIDFROM;

public class TerrDivTypeTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> terrdivtype = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(TablesName.EBX_REP_M_SFS_0002_SFSDIVISION), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);
        terrdivtype = terrdivtype
                .select(
                        col(ColumnsName.DIVISIONTYPE00).as(ColumnsName.SID),
                        lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.TERR_DIV_TYPE_DP_CODE),
                        col(ColumnsName.DIVISIONTYPE00).as(ColumnsName.DIV_TYPE_CODE),
                        col(ColumnsName.DIVISIONTYPE00).as(ColumnsName.TERR_DIV_TYPE_SK),
                        lit(false).as(ColumnsName.DIRTY_FLAG)

                ).distinct();

        terrdivtype = terrdivtype.select(
                //lit(VspcConstant.getCurrentHourCutOffDate().toString()).as(CTL_VALIDFROM),
                col(ColumnsName.TERR_DIV_TYPE_SK).as(ColumnsName.SID),
                lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.TERR_DIV_TYPE_DP_CODE),
                col(ColumnsName.DIV_TYPE_CODE).as(ColumnsName.DIV_TYPE_CODE),
                col(ColumnsName.TERR_DIV_TYPE_SK).as(ColumnsName.TERR_DIV_TYPE_SK),
                lit(false).as(ColumnsName.DIRTY_FLAG)
        );

        terrdivtype = skClientWrapper.generateSk(terrdivtype, transformConfiguration.getStgTable(), transformConfiguration);

        return terrdivtype;
    }
}