package ru.sberbank.bigdata.dffess.vspc.tables.timezone;

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
import static org.apache.spark.sql.functions.*;

public class TimeZoneTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> timezone = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(TablesName.EBX_REP_M_SFS_0002_SFSDIVISION), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);
        
        timezone = timezone
                .select(
                        when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("USZ1"), "1")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("KALT"), "1")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("MSK"), "2")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("SAMT"), "3")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("YEKT"), "4")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("OMST"), "5")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("KRAT"), "6")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("IRKT"), "7")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("YAKT"), "8")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("VLAT"), "9")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("MAGT"), "10")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("PETT"), "11")
                        .as(ColumnsName.TIME_ZONE_CODE),

                        when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("USZ1"), "MSK-1")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("KALT"), "MSK-1")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("MSK"), "MSK+0")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("SAMT"), "MSK+1")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("YEKT"), "MSK+2")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("OMST"), "MSK+3")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("KRAT"), "MSK+4")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("IRKT"), "MSK+5")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("YAKT"), "MSK+6")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("VLAT"), "MSK+7")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("MAGT"), "MSK+8")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("PETT"), "MSK+9")
                        .as(ColumnsName.TIME_ZONE_MSK_SHIFT_VAL),

                        when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("USZ1"), "Калининградское время")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("KALT"), "Калининградское время")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("MSK"), "Московское время")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("SAMT"), "Самарское время")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("YEKT"), "Екатеринбургское время")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("OMST"), "Омское время")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("KRAT"), "Красноярское время")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("IRKT"), "Иркутское время")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("YAKT"), "Якутское время")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("VLAT"), "Владивостокское время")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("MAGT"), "Магаданское время")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("PETT"), "Камчатское время")
                        .as(ColumnsName.TIME_ZONE_NAME),

                        when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("USZ1"), "UTC+2")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("KALT"), "UTC+2")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("MSK"), "UTC+3")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("SAMT"), "UTC+4")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("YEKT"), "UTC+5")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("OMST"), "UTC+6")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("KRAT"), "UTC+7")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("IRKT"), "UTC+8")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("YAKT"), "UTC+9")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("VLAT"), "UTC+10")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("MAGT"), "UTC+11")
                        .when(col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).equalTo("PETT"), "UTC+12")
                        .as(ColumnsName.TIME_ZONE_WRLD_SHIFT_VAL),

                        col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).as(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE)

                ).filter(col(ColumnsName.TIME_ZONE_CODE).isNotNull()).distinct(); 

        timezone = timezone.select(
                col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).as(ColumnsName.SID),
                col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).as(ColumnsName.TIME_ZONE_ABBR_CODE),
                col(ColumnsName.TIME_ZONE_CODE).as(ColumnsName.TIME_ZONE_CODE),
                col(ColumnsName.TIME_ZONE_MSK_SHIFT_VAL).as(ColumnsName.TIME_ZONE_MSK_SHIFT_VAL),
                col(ColumnsName.TIME_ZONE_NAME).as(ColumnsName.TIME_ZONE_NAME),
                lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.TIME_ZONE_DP_CODE),
                col(ColumnsName.ADDRESS_ADDRFACT_TIMEZONE).as(ColumnsName.TIME_ZONE_SK),
                col(ColumnsName.TIME_ZONE_WRLD_SHIFT_VAL).as(ColumnsName.TIME_ZONE_WRLD_SHIFT_VAL),
                lit(false).as(ColumnsName.DIRTY_FLAG)
        );

        timezone = skClientWrapper.generateSk(timezone, TablesName.TIMEZONE, transformConfiguration.getLoadMode(), transformConfiguration);

        return timezone;
    }
}