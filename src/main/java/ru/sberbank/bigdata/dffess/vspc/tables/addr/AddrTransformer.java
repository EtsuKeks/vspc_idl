package ru.sberbank.bigdata.dffess.vspc.tables.addr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName;
import ru.sberbank.bigdata.dffess.vspc.constant.TablesName;
import ru.sberbank.bigdata.dffess.vspc.constant.VpscConstant;
import ru.sberbank.bigdata.essa.common.templates.EntityProxyRelation;
import ru.sberbank.bigdata.essa.common.templates.EntityTransformer;
import ru.sberbank.bigdata.essa.common.templates.SkClientWrapper;
import ru.sberbank.bigdata.essa.common.templates.TransformConfiguration;
import ru.sberbank.bigdata.essa.common.utils.CtlStatisticHelper;
import ru.sberbank.bigdata.essa.common.utils.HdfsHelper;
import ru.sberbank.hermes.core.dmbuilder.modules.stg.builder.DmBuilder;
import ru.sberbank.hermes.core.dmbuilder.services.logger.loggers.Logger;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class AddrTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(DmBuilder dmBuilder, SkClientWrapper skClientWrapper, HdfsHelper hdfsHelper, CtlStatisticHelper ctlStatisticHelper, TransformConfiguration transformConfiguration, EntityProxyRelation entityProxyRelation, Logger logger) {

        Dataset<Row> addr = dmBuilder.getDataset(transformConfiguration.getSrcSchemaSnp(), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);
        addr = addr
                .select(
                        col(ColumnsName.ID).as(ColumnsName.SID),
                        col(ColumnsName.ID).as(ColumnsName.ADDR_SK),
                        coalesce(col(ColumnsName.ADDRESS_ADDRFACT_POSTCODE), col(ColumnsName.ADDRESS_POSTCODE)).as(ColumnsName.ADDR_IDX_NUM),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDRAREA_AREANAME).as(ColumnsName.ADDR_DISTRICT_NAME),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDPLACE_PLACENAME).as(ColumnsName.ADDR_LOCTY_NAME),
                        col(ColumnsName.ADDRESS_SUBJECTCODE).as(ColumnsName.ADDR_REG_ADDR_NAME),
                        col(ColumnsName.ADDRESS_LOCATIONADDRESS).as(ColumnsName.ADDR_SRC_TXT),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDRCITY_CITYNAME).as(ColumnsName.ADDR_CITY_NAME),
                        col(ColumnsName.ADDRESS_ADDRFACT_FIASCODE).as(ColumnsName.ADDR_FIAS_CODE),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDRSTREET_HOUSENUMBERS).as(ColumnsName.ADDR_HOUSE_NUM),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDRSTREET_STREETNAME).as(ColumnsName.ADDR_STREET_NAME),
                        lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.ADDR_DP_CODE),
                        col(ColumnsName.ADDRESS_SUBJECTCODE).as(ColumnsName.ADDR_RUS_FED_SUBJ_SK),
                        lit(VpscConstant.RDM_IDL_SPARK).as(ColumnsName.ADDR_REG_FED_SUBJ_DP_CODE),
                        col(ColumnsName.ADDRESS_CNTRYCODE).as(ColumnsName.ADDR_CNTRY_SK),
                        lit(VpscConstant.RDM_IDL_SPARK).as(ColumnsName.ADDR_CNTRY_DP_CODE),
                        col(ColumnsName.ADDRESS_OKATO2_CODE).as(ColumnsName.ADDR_OKATO2_SK),
                        lit(VpscConstant.RDM_IDL_SPARK).as(ColumnsName.ADDR_OKATO2_DP_CODE),


                        lit(false).as(ColumnsName.DIRTY_FLAG)
                );

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(ColumnsName.ADDR_RUS_FED_SUBJ_SK, "rus_fed_subj");
        columnEntities.put(ColumnsName.ADDR_CNTRY_SK, "cntry");
        columnEntities.put(ColumnsName.ADDR_OKATO2_SK, "okato2");


        addr = skClientWrapper.generateSk(addr, transformConfiguration.getStgTable(), transformConfiguration.getLoadMode(), columnEntities, transformConfiguration);

        return addr;
    }
}
