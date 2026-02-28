package ru.sberbank.bigdata.dffess.vspc.tables.divh;

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
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DISABLED_ACCESS_EQUIPMENT;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.*;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.CTL_VALIDFROM;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.DELETED_FLAG;

public class DivHTransformer implements EntityTransformer {

    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> div_raw = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(CHG_DIVH), CHG_DIVH);

        divh_raw = divh_raw.select(
                col(FULLDIVCODE).as(DIV_SK),
                col(FULLNAME).as(DIV_H_NAME),
                col(FULLDIVCODE).as(DIV_CODE),
                col(REQUSITEDIVISION_BIC).as(DIV_PAY_SYS_PRTCPNT_SK),

                col(START_DT).as(START_DT),
                col(END_DT).as(END_DT),

                //col(ColumnsName.ADDRESS_LOCATIONADDRESS).as(ColumnsName.DIV_FACT_ADDR_SK),

                col(ADDRESS_LOCATIONADDRESS).as(ADDR_SRC_TXT),

                col("serving_isdisabledequip").as(DIV_DISABLED_ACCESS_EQUIPMENT),

                col(ADDRESS_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                lit(null).as(ADDRESS_FIASCODE_DURTY),
                lit(null).as(ADDR_OKATO2_SK),
                lit(null).as(ADDR_OKTMO2_SK)

        ).distinct();

        Dataset<Row> divh = divh_raw.select(
                lit(getCtlValidFromStr()).as(CTL_VALIDFROM),
                col(DIV_SK).as(DIV_SK),
                col(DIV_H_NAME).as(DIV_H_NAME),
                col(DIV_CLOSEDATE).as(DIV_CLOSEDATE),
                col(DIV_OPENDATE).as(DIV_OPENDATE),
                col(DIV_CODE).as(DIV_CODE),
                lit(DATA_PRODUCT).as(DIV_DP_CODE),

                when(col(DIV_PAY_SYS_PRTCPNT_SK).isNotNull(), col(DIV_PAY_SYS_PRTCPNT_SK)).otherwise(null).as(DIV_PAY_SYS_PRTCPNT_SK),
                when(col(DIV_PAY_SYS_PRTCPNT_SK).isNotNull(), lit(RDM_IDL_SPARK)).otherwise(null).as(DIV_PAY_SYS_PRTCPNT_DP_CODE),

                lit(DATA_PRODUCT).as(DIV_FACT_ADDR_DP_CODE),

                when(col(ADDR_SRC_TXT).isNotNull(), AddrTransformer.getKey("|")).otherwise(null).as(DIV_FACT_ADDR_SK),
                when(col(ADDR_SRC_TXT).isNotNull(), lit(DATA_PRODUCT)).otherwise(null).as(DIV_FACT_ADDR_DP_CODE),
                col(DIV_DISABLED_ACCESS_EQUIPMENT).as(DIV_DISABLED_ACCESS_EQUIPMENT),

                lit(false).as(DIRTY_FLAG),
                lit(false).as(DELETED_FLAG),
                col(START_DT).as(START_DT),
                col(END_DT).as(END_DT)
        ).distinct();

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(DIV_PAY_SYS_PRTCPNT_SK, "pay_sys_prtcpnt");
        columnEntities.put(DIV_FACT_ADDR_SK, ADDR);

        divh = skClientWrapper.incrementSk(divh, DIVH, columnEntities, transformConfiguration);

        return divh;
    }
}
