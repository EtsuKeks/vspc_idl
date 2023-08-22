package ru.sberbank.bigdata.dffess.vspc.tables.div;

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

import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.essa.common.constant.DmBuilderConst.LEFT_JOIN;

public class DivTransformer implements EntityTransformer {

    @Override
    public Dataset<Row> transformData(DmBuilder dmBuilder, SkClientWrapper skClientWrapper, HdfsHelper hdfsHelper, CtlStatisticHelper ctlStatisticHelper, TransformConfiguration transformConfiguration, EntityProxyRelation entityProxyRelation, Logger logger) {

        Dataset<Row> div = dmBuilder.getDataset(transformConfiguration.getSrcSchemaSnp(), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);

      div = div
                .select(
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.SID),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SK),
                        lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_CODE),
                        col(ColumnsName.CLOSEDATE).as(ColumnsName.DIV_CLOSEDATE),
                        col(ColumnsName.OPENDATE).as(ColumnsName.DIV_OPENDATE),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SUBBRANCH_CODE),
                        col(ColumnsName.HEADDIVISIONCODE).as(ColumnsName.DIV_PARENT_DIV_SK),
                        col(VpscConstant.DATA_PRODUCT).as(ColumnsName.DIV_PARENT_DIV_DP_CODE),
                        col(ColumnsName.ID).as(ColumnsName.DIV_LEGAL_ADDR_SK),
                        col(VpscConstant.DATA_PRODUCT).as(ColumnsName.DIV_LEGAL_ADDR_DP_CODE),
                        lit(false).as(ColumnsName.DIRTY_FLAG)
                );

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(ColumnsName.DIV_PARENT_DIV_SK, TablesName.DIV);
        columnEntities.put(ColumnsName.DIV_LEGAL_ADDR_SK, TablesName.ADDR);


        div = skClientWrapper.generateSk(div, transformConfiguration.getStgTable(), transformConfiguration.getLoadMode(), columnEntities, transformConfiguration);
        return div;
    }



}
