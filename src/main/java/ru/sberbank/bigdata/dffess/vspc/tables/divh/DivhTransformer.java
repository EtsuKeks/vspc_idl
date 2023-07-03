package ru.sberbank.bigdata.dffess.vspc.tables.divh;

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

public class DivhTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(DmBuilder dmBuilder, SkClientWrapper skClientWrapper, HdfsHelper hdfsHelper, CtlStatisticHelper ctlStatisticHelper, TransformConfiguration transformConfiguration, EntityProxyRelation entityProxyRelation, Logger logger) {

        Dataset<Row> divh = dmBuilder.getDataset(transformConfiguration.getSrcSchemaSnp(), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);
        divh = divh
                .select(
                        /*col(ColumnsName.CODE).as(ColumnsName.BSNS_MODEL_TYPE_CODE),
                        col(ColumnsName.NAME00).as(ColumnsName.BSNS_MODEL_TYPE_NAME),
                        col(ColumnsName.DESCRIPTION).as(ColumnsName.BSNS_MODEL_TYPE_TXT),
                        lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.IFRS_ASST_BSNS_MODEL_TYPE_DP_CODE),
                        col(ColumnsName.CODE).as(ColumnsName.IFRS_ASST_BSNS_MODEL_TYPE_SK),
                        col(ColumnsName.CODE).as(ColumnsName.SID),
                        lit(false).as(ColumnsName.DIRTY_FLAG)*/
                );

        divh = skClientWrapper.generateSk(divh, transformConfiguration.getStgTable(), transformConfiguration.getLoadMode(), transformConfiguration);

        return divh;
    }
}
