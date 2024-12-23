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

        Dataset<Row> timezone = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(TablesName.EBX_REP_M_RFCZONETIME_0002_RFCZONETIME), TablesName.EBX_REP_M_RFCZONETIME_0002_RFCZONETIME);

        timezone = timezone.select(
                col("abbreviation").as(ColumnsName.SID),
                col("abbreviation").as(ColumnsName.TIME_ZONE_ABBR_CODE),
                col("code").as(ColumnsName.TIME_ZONE_CODE),
                col("timedesignation").as(ColumnsName.TIME_ZONE_MSK_SHIFT_VAL),
                col("name00").as(ColumnsName.TIME_ZONE_NAME),
                lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.TIME_ZONE_DP_CODE),
                col("abbreviation").as(ColumnsName.TIME_ZONE_SK),
                col("utcformat").as(ColumnsName.TIME_ZONE_WRLD_SHIFT_VAL),
                lit(false).as(ColumnsName.DIRTY_FLAG)
        )

                .where(col("abbreviation").isNotNull())
                .distinct();

        timezone = skClientWrapper.generateSk(timezone, TablesName.TIMEZONE, transformConfiguration.getLoadMode(), transformConfiguration);

        return timezone;
    }
}