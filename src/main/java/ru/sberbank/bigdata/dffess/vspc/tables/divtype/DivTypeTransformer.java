package ru.sberbank.bigdata.dffess.vspc.tables.divtype;

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

public class DivTypeTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(DmBuilder dmBuilder, SkClientWrapper skClientWrapper, HdfsHelper hdfsHelper, CtlStatisticHelper ctlStatisticHelper, TransformConfiguration transformConfiguration, EntityProxyRelation entityProxyRelation, Logger logger) {

        Dataset<Row> divtype = dmBuilder.getDataset(transformConfiguration.getSrcSchemaSnp(), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);
        divtype = divtype
                .select(
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SK),
                        lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                        col(ColumnsName.DIVISIONTYPE00).as(ColumnsName.DIV_TYPE_TERR_DIV_TYPE_SK),
                        lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.DIV_TYPE_TERR_DIV_TYPE_DP_CODE)
                );

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(ColumnsName.DIV_TYPE_TERR_DIV_TYPE_SK, TablesName.TERRDIVTYPE);
        columnEntities.put(ColumnsName.DIV_SK, TablesName.DIV);


        divtype = skClientWrapper.incrementSk(divtype, transformConfiguration.getStgTable(), columnEntities, transformConfiguration);

        return divtype;
    }
}
