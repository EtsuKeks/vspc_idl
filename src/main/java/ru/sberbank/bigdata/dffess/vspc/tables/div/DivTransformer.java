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
import ru.sberbank.bigdata.dffess.vspc.tables.addr.AddrTransformer;
import org.apache.spark.sql.Column;

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

        Dataset<Row> div_raw = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(TablesName.EBX_REP_M_SFS_0002_SFSDIVISION), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);

        div_raw = div_raw.select(
                col(ColumnsName.FULLDIVCODE).as(ColumnsName.SID),
                col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SK),
                col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_CODE),
                col(ColumnsName.SHORTDIVCODE).as(ColumnsName.DIV_SHORT_DIV_CODE),
                to_date(col(ColumnsName.CLOSEDATE)).as(ColumnsName.DIV_CLOSEDATE),
                to_date(col(ColumnsName.OPENDATE)).as(ColumnsName.DIV_OPENDATE),
                concat(lit("0"), col(ColumnsName.FULLDIVCODE)).as(ColumnsName.DIV_SUBBRANCH_CODE),
                col(ColumnsName.HEADDIVISIONCODE).as(ColumnsName.DIV_PARENT_DIV_SK),

                //col(ColumnsName.ADDRESS_LOCATIONADDRESS).as(ColumnsName.DIV_LEGAL_ADDR_SK),
                //col(ColumnsName.ADDRESS_LEGAL_LOCATIONADDRESS).as(ColumnsName.DIV_LEGAL_ADDR_SK),
                                        
                when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                .otherwise(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM)).as(ColumnsName.DIV_FULL_REG_NUM),

                when(col(ColumnsName.FULLNAME).equalTo("ПАО Сбербанк"), 
                        when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null).otherwise(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM)))
                .otherwise(
                        when(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                        .otherwise(substring_index(col(ColumnsName.DIV_REQUISITEDIVISION_REGISTRATIONNUM), "/", -1))).cast(DataTypes.IntegerType).as(ColumnsName.DIV_REG_ORD),
                
                col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).as(ColumnsName.DIV_TIME_ZONE_TIME_ZONE_SK),

                lit(null).as(ColumnsName.ADDR_IDX_NUM),
                lit(null).as(ColumnsName.ADDR_DISTRICT_NAME),
                lit(null).as(ColumnsName.ADDR_LOCTY_NAME),
                lit(null).as(ColumnsName.ADDR_REG_ADDR_NAME),
                col(ColumnsName.ADDRESS_LEGAL_LOCATIONADDRESS).as(ColumnsName.ADDR_SRC_TXT),
                lit(null).as(ColumnsName.ADDR_CITY_NAME),
                lit(null).as(ColumnsName.ADDR_FIAS_CODE),
                lit(null).as(ColumnsName.ADDR_HOUSE_NUM),
                lit(null).as(ColumnsName.ADDR_STREET_NAME),
                lit(null).as(ColumnsName.ADDR_CNTRY_SK),
                lit(null).as(ColumnsName.ADDR_OKATO2_SK),
                lit(null).as(ColumnsName.ADDR_RUS_FED_SUBJ_SK)

        ).distinct();

        Dataset<Row> div = div_raw.select(
                col(ColumnsName.SID).as(ColumnsName.SID),
                col(ColumnsName.DIV_SK).as(ColumnsName.DIV_SK),
                lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                col(ColumnsName.DIV_CODE).as(ColumnsName.DIV_CODE),
                col(ColumnsName.DIV_SHORT_DIV_CODE).as(ColumnsName.DIV_SHORT_DIV_CODE),
                col(ColumnsName.DIV_CLOSEDATE).as(ColumnsName.DIV_CLOSEDATE),
                col(ColumnsName.DIV_OPENDATE).as(ColumnsName.DIV_OPENDATE),
                col(ColumnsName.DIV_SUBBRANCH_CODE).as(ColumnsName.DIV_SUBBRANCH_CODE),
                col(ColumnsName.DIV_PARENT_DIV_SK).as(ColumnsName.DIV_PARENT_DIV_SK),
                lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_PARENT_DIV_DP_CODE),

                //col(ColumnsName.ADDRESS_LOCATIONADDRESS).as(ColumnsName.DIV_LEGAL_ADDR_SK),
                AddrTransformer.getKey("|").as(ColumnsName.DIV_LEGAL_ADDR_SK),

                lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_LEGAL_ADDR_DP_CODE),
                lit(false).as(ColumnsName.DIRTY_FLAG),
                                        
                col(ColumnsName.DIV_FULL_REG_NUM).as(ColumnsName.DIV_FULL_REG_NUM),

                col(ColumnsName.DIV_REG_ORD).as(ColumnsName.DIV_REG_ORD),
                
                col(ColumnsName.DIV_TIME_ZONE_TIME_ZONE_SK).as(ColumnsName.DIV_TIME_ZONE_TIME_ZONE_SK),
                lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_TIME_ZONE_TIME_ZONE_DP_CODE)
        ).distinct();
        
        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(ColumnsName.DIV_PARENT_DIV_SK, TablesName.DIV);
        columnEntities.put(ColumnsName.DIV_LEGAL_ADDR_SK, TablesName.ADDR);
        columnEntities.put(ColumnsName.DIV_TIME_ZONE_TIME_ZONE_SK, TablesName.TIMEZONE);

        div = skClientWrapper.generateSk(div, TablesName.DIV, transformConfiguration.getLoadMode(), columnEntities, transformConfiguration);
        return div;
    }
}
