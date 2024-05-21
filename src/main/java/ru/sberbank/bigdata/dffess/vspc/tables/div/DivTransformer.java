package ru.sberbank.bigdata.dffess.vspc.tables.div;

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
import org.apache.spark.sql.types.*;

public class DivTransformer implements EntityTransformer {

    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> div = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(TablesName.EBX_REP_M_SFS_0002_SFSDIVISION), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);

        div = div
                .select(
                        //lit(VspcConstant.getCurrentHourCutOffDate().toString()).as(CTL_VALIDFROM),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.SID),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SK),
                        lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_CODE),
                        col(ColumnsName.SHORTDIVCODE).as(ColumnsName.DIV_SHORT_DIV_CODE),
                        to_date(col(ColumnsName.CLOSEDATE)).as(ColumnsName.DIV_CLOSEDATE),
                        to_date(col(ColumnsName.OPENDATE)).as(ColumnsName.DIV_OPENDATE),
                        concat(lit("0"), col(ColumnsName.FULLDIVCODE)).as(ColumnsName.DIV_SUBBRANCH_CODE),
                        col(ColumnsName.HEADDIVISIONCODE).as(ColumnsName.DIV_PARENT_DIV_SK),
                        lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_PARENT_DIV_DP_CODE),
                        col(ColumnsName.ID).as(ColumnsName.DIV_LEGAL_ADDR_SK),
                        lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_LEGAL_ADDR_DP_CODE),
                        lit(false).as(ColumnsName.DIRTY_FLAG),
                                                
                        when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                        .otherwise(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM)).as(ColumnsName.DIV_FULL_REG_NUM),

                        when(col(ColumnsName.FULLNAME).equalTo("ПАО Сбербанк"), 
                                when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null).otherwise(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM)))
                        .otherwise(
                                when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                                .otherwise(substring_index(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM), "/", -1))).cast(DataTypes.IntegerType).as(ColumnsName.DIV_REG_ORD),
                        
                        col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).as(ColumnsName.DIV_TIME_ZONE_TIME_ZONE_SK),
                        lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_TIME_ZONE_TIME_ZONE_DP_CODE)
                );

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(ColumnsName.DIV_PARENT_DIV_SK, TablesName.DIV);
        columnEntities.put(ColumnsName.DIV_LEGAL_ADDR_SK, TablesName.ADDR);
        columnEntities.put(ColumnsName.DIV_TIME_ZONE_TIME_ZONE_SK, TablesName.TIMEZONE);

        div = skClientWrapper.generateSk(div, TablesName.DIV, transformConfiguration.getLoadMode(), columnEntities, transformConfiguration);
        return div;
    }
}
