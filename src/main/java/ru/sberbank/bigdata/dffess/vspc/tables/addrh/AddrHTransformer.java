package ru.sberbank.bigdata.dffess.vspc.tables.addrh;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
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
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.CHG_DIVH;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.*;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.*;

public class AddrHTransformer implements EntityTransformer {

    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {


        Dataset<Row> addr_raw = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(CHG_DIVH), CHG_DIVH);

        Dataset<Row> addr_fact = addr_raw.select(
                col(ADDRESS_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                col(ADDRESS_LOCATIONADDRESS).as(ADDRESS_STANDARD_ADDRESS_TXT),

                lit(null).as(ADDR_FIAS_CODE),
                lit(null).as(ADDRESS_FIASCODE_DURTY),

                lit(null).as(ADDR_OKATO2_SK),
                lit(null).as(ADDR_OKTMO2_SK)

        ).where(col(ADDRESS_LOCATIONADDRESS).isNotNull());

        Dataset<Row> addr_legal = addr_raw.select(
                col(ADDRESS_LEGAL_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                col(ADDRESS_LEGAL_LOCATIONADDRESS).as(ADDRESS_STANDARD_ADDRESS_TXT),
                col("gold_fias_code").as(ADDR_FIAS_CODE),
                col("address_addrfact_fiascode").as(ADDRESS_FIASCODE_DURTY),
                col("gold_okato").as(ADDR_OKATO2_SK),
                col("gold_oktmo").as(ADDR_OKTMO2_SK)

        ).where(col(ADDRESS_LEGAL_LOCATIONADDRESS).isNotNull());

        Dataset<Row> addr = addr_fact.union(addr_legal).distinct();

        addr = addr.select(
                AddrTransformer.getKey("|").as(ADDR_SK),

                col(ADDRESS_STANDARD_ADDRESS_TXT).as(ADDRESS_STANDARD_ADDRESS_TXT),
                col(ADDR_FIAS_CODE).as(ADDR_FIAS_CODE),
                col(ADDR_OKATO2_SK).as(ADDR_OKATO2_SK),
                col(ADDR_OKTMO2_SK).as(ADDR_OKTMO2_SK)

        ).distinct();

        Dataset<Row> addrh = addr.select(
                lit(VspcConstant.getCtlValidFromStr()).as(CTL_VALIDFROM),
                col(ADDR_SK).as(ADDR_SK),
                lit(VspcConstant.DATA_PRODUCT).as(ADDR_DP_CODE),
                col(ADDRESS_STANDARD_ADDRESS_TXT).as(ADDRESS_STANDARD_ADDRESS_TXT),
                lit(false).as(DIRTY_FLAG),
                lit(false).as(DELETED_FLAG),

                when(col(ADDR_OKATO2_SK).isNotNull(), col(ADDR_OKATO2_SK)).as(ADDRH_OKATO2_SK),
                when(col(ADDR_OKATO2_SK).isNotNull(), lit(RDM_IDL_SPARK)).as(ADDRH_OKATO2_DP_CODE),

                when(col(ADDR_OKTMO2_SK).isNotNull(), col(ADDR_OKTMO2_SK)).as(ADDRH_OKTMO2_SK),
                when(col(ADDR_OKTMO2_SK).isNotNull(), lit(RDM_IDL_SPARK)).as(ADDRH_OKTMO2_DP_CODE),

                lit(">non_stndrt").as(ADDRESS_TYPE_CODE)
        ).distinct();

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(ADDRH_OKATO2_SK, "okato2");
        columnEntities.put(ADDRH_OKTMO2_SK, "oktmo2");

        addrh = skClientWrapper.incrementSk(addrh, ADDRH, columnEntities, transformConfiguration);

        return addrh;
    }
}
