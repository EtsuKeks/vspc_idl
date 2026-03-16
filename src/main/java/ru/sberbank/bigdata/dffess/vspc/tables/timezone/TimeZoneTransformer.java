package ru.sberbank.bigdata.dffess.vspc.tables.timezone;

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

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_M_RFCZONETIME_0002_RFCZONETIME;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TIMEZONE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.SID;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.DELETED_FLAG;

public class TimeZoneTransformer implements EntityTransformer {

    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> timezone = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(EBX_REP_M_RFCZONETIME_0002_RFCZONETIME), EBX_REP_M_RFCZONETIME_0002_RFCZONETIME);
        timezone = timezone.select(
                        lit(false).as(DELETED_FLAG),
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(SID),
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(TIME_ZONE_ABBR_CODE),
                        col(CODE).as(TIME_ZONE_CODE),
                        col(TIMEDESIGNATION).as(TIME_ZONE_MSK_SHIFT_VAL),
                        col(NAME00).as(TIME_ZONE_NAME),
                        lit(DATA_PRODUCT).as(TIME_ZONE_DP_CODE),
                        when(col(ABBREVIATION).equalTo("USZ1"), "KALT").otherwise(col(ABBREVIATION)).as(TIME_ZONE_SK),
                        col(UTCFORMAT).as(TIME_ZONE_WRLD_SHIFT_VAL),
                        lit(false).as(DIRTY_FLAG)
                ).where(col(ABBREVIATION).isNotNull())
                .distinct();

        timezone = skClientWrapper.generateSk(timezone, TIMEZONE, transformConfiguration.getLoadMode(), transformConfiguration);

        return timezone;
    }
}
