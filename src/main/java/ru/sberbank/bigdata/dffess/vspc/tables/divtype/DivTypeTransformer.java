package ru.sberbank.bigdata.dffess.vspc.tables.divtype;

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

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.CTL_VALIDFROM;

public class DivTypeTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> divtype = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(TablesName.EBX_REP_M_SFS_0002_SFSDIVISION), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);
        divtype = divtype
                .select(
                        lit(VspcConstant.getCtlValidFromStr()).as(CTL_VALIDFROM),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SK),
                        lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                        col(ColumnsName.DIVISIONTYPE00).as(ColumnsName.DIV_TYPE_TERR_DIV_TYPE_SK),
                        lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_TYPE_TERR_DIV_TYPE_DP_CODE),
                        lit(false).as(ColumnsName.DIRTY_FLAG)

                ).distinct();

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(ColumnsName.DIV_TYPE_TERR_DIV_TYPE_SK, TablesName.TERRDIVTYPE);
        columnEntities.put(ColumnsName.DIV_SK, TablesName.DIV);

        divtype = skClientWrapper.incrementSk(divtype, TablesName.DIVTYPE, columnEntities, transformConfiguration);

        return divtype;
    }
}