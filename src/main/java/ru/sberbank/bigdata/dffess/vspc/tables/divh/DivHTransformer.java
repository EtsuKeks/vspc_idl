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

public class DivHTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(DmBuilder dmBuilder, SkClientWrapper skClientWrapper, HdfsHelper hdfsHelper, CtlStatisticHelper ctlStatisticHelper, TransformConfiguration transformConfiguration, EntityProxyRelation entityProxyRelation, Logger logger) {

        Dataset<Row> divh = dmBuilder.getDataset(transformConfiguration.getSrcSchemaSnp(), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);
        divh = divh
                .select(
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SK),
                        col(ColumnsName.FULLNAME).as(ColumnsName.DIV_H_NAME),
                        col(ColumnsName.CLOSEDATE).as(ColumnsName.DIV_CLOSEDATE),
                        col(ColumnsName.OPENDATE).as(ColumnsName.DIV_OPENDATE),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_CODE),
                        lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                        lit(VpscConstant.RDM_IDL_SPARK).as(ColumnsName.DIV_UPS_PAY_SYS_PRTCPNT_DP_CODE),
                        col(ColumnsName.REQUSITEDIVISION_BIC).as(ColumnsName.DIV_UPS_PAY_SYS_PRTCPNT_SK),
                        col(VpscConstant.DATA_PRODUCT).as(ColumnsName.DIV_FACT_ADDR_DP_CODE),
                        col(ColumnsName.ID).as(ColumnsName.DIV_FACT_ADDR_SK),
                        lit(false).as(ColumnsName.DIRTY_FLAG)
                );

        divh = skClientWrapper.incrementSk(divh, transformConfiguration.getStgTable(),  transformConfiguration);

        return divh;
    }
}
