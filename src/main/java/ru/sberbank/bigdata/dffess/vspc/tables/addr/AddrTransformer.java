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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class AddrTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(DmBuilder dmBuilder, SkClientWrapper skClientWrapper, HdfsHelper hdfsHelper, CtlStatisticHelper ctlStatisticHelper, TransformConfiguration transformConfiguration, EntityProxyRelation entityProxyRelation, Logger logger) {

        Dataset<Row> addr = dmBuilder.getDataset(transformConfiguration.getSrcSchemaSnp(), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);
        addr = addr
                .select(
                        col(ColumnsName.ID).as(ColumnsName.SID),
                        col(ColumnsName.ADDINFORMATION).as(ColumnsName.ADDR_SRC_TEXT),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDRCITY_CITYNAME).as(ColumnsName.ADDR_CITY_NAME),
                        col(ColumnsName.ADDRESS_ADDRFACT_FIASCODE).as(ColumnsName.ADDR_FIAS_CODE),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDRSTREET_HOUSENUMBERS).as(ColumnsName.ADDR_HOUSE_NUM),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDRSTREET_STREETNAME).as(ColumnsName.ADDR_STREET_NAME),
                        col(ColumnsName.ADDRESS_LOCATIONADDRESS).as(ColumnsName.ADDR_LOCTY_NAME),
                        col(ColumnsName.SYSTEM_ID).as(ColumnsName.ADDR_RUS_FED_SUBJ_SK),
                        lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.ADDR_REG_FED_SUBJ_DP_CODE),
                        col(ColumnsName.ID).as(ColumnsName.IFRS_ASST_BSNS_MODEL_TYPE_SK),
                        lit(false).as(ColumnsName.DIRTY_FLAG)
                );

        addr = skClientWrapper.generateSk(addr, transformConfiguration.getStgTable(), transformConfiguration.getLoadMode(), transformConfiguration);

        return addr;
    }
}
