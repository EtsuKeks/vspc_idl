package ru.sberbank.bigdata.dffess.vspc.tables.div;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.tables.addr.AddrTransformer;
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
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.SID;

public class DivTransformer implements EntityTransformer {

    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> div_raw = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(CHG_DIVH), CHG_DIVH);

        div_raw = div_raw.select(
                col(FULLDIVCODE).as(SID),
                col(FULLDIVCODE).as(DIV_SK),
                col(FULLDIVCODE).as(DIV_CODE),
                col(SHORTDIVCODE).as(DIV_SHORT_DIV_CODE),
                to_date(col(CLOSEDATE)).as(DIV_CLOSEDATE),
                to_date(col(OPENDATE)).as(DIV_OPENDATE),
                concat(lit("0"), col(FULLDIVCODE)).as(DIV_SUBBRANCH_CODE),
                col(HEADDIVISIONCODE).as(DIV_PARENT_DIV_SK),

                when(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                        .otherwise(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM)).as(DIV_FULL_REG_NUM),

                when(col(FULLNAME).equalTo("ПАО Сбербанк"),
                        when(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null).otherwise(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM)))
                        .otherwise(
                                when(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                                        .otherwise(substring_index(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM), "/", -1))).cast(IntegerType).as(DIV_REG_ORD),

                when(col(ADDRESS_ADDRFACT_TIMEZONE).equalTo("USZ1"), "KALT")
                        .otherwise(col(ADDRESS_ADDRFACT_TIMEZONE)).as(DIV_TIME_ZONE_TIME_ZONE_SK),

                col(ADDRESS_LEGAL_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                col("address_addrfact_fiascode").as(ADDRESS_FIASCODE_DURTY),
                col("gold_okato").as(ADDR_OKATO2_SK),
                col("gold_oktmo").as(ADDR_OKTMO2_SK)

                ).distinct();

        Dataset<Row> div = div_raw.select(
                col(SID).as(SID),
                col(DIV_SK).as(DIV_SK),
                lit(DATA_PRODUCT).as(DIV_DP_CODE),
                col(DIV_CODE).as(DIV_CODE),
                col(DIV_SHORT_DIV_CODE).as(DIV_SHORT_DIV_CODE),
                col(DIV_CLOSEDATE).as(DIV_CLOSEDATE),
                col(DIV_OPENDATE).as(DIV_OPENDATE),
                col(DIV_SUBBRANCH_CODE).as(DIV_SUBBRANCH_CODE),
                col(DIV_PARENT_DIV_SK).as(DIV_PARENT_DIV_SK),
                lit(DATA_PRODUCT).as(DIV_PARENT_DIV_DP_CODE),

                when(col(ADDR_SRC_TXT).isNotNull(), AddrTransformer.getKey("|")).otherwise(null).as(DIV_LEGAL_ADDR_SK),
                when(col(ADDR_SRC_TXT).isNotNull(), lit(DATA_PRODUCT)).otherwise(null).as(DIV_LEGAL_ADDR_DP_CODE),

                lit(false).as(DIRTY_FLAG),

                col(DIV_FULL_REG_NUM).as(DIV_FULL_REG_NUM),

                col(DIV_REG_ORD).as(DIV_REG_ORD),

                col(DIV_TIME_ZONE_TIME_ZONE_SK).as(DIV_TIME_ZONE_TIME_ZONE_SK),
                lit(DATA_PRODUCT).as(DIV_TIME_ZONE_TIME_ZONE_DP_CODE)
        ).distinct();

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(DIV_PARENT_DIV_SK, DIV);
        columnEntities.put(DIV_LEGAL_ADDR_SK, ADDR);
        columnEntities.put(DIV_TIME_ZONE_TIME_ZONE_SK, TIMEZONE);

        div = skClientWrapper.generateSk(div, DIV, transformConfiguration.getLoadMode(), columnEntities, transformConfiguration);
        return div;
    }
}
