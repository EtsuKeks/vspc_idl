package ru.sberbank.bigdata.dffess.vspc.tables.terrdivtype;

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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class TerrDivTypeTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(DmBuilder dmBuilder, SkClientWrapper skClientWrapper, HdfsHelper hdfsHelper, CtlStatisticHelper ctlStatisticHelper, TransformConfiguration transformConfiguration, EntityProxyRelation entityProxyRelation, Logger logger) {

        Dataset<Row> terrdivtype = dmBuilder.getDataset(transformConfiguration.getSrcSchemaSnp(), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);
        terrdivtype = terrdivtype
                .select(
                        col(ColumnsName.DIVISIONTYPE00).as(ColumnsName.SID),
                        lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.TERR_DIV_TYPE_DP_CODE),
                        col(ColumnsName.DIVISIONTYPE00).as(ColumnsName.DIV_TYPE_CODE),
                        col(ColumnsName.DIVISIONTYPE00).as(ColumnsName.TERR_DIV_TYPE_SK),
                        lit(false).as(ColumnsName.DIRTY_FLAG)

                ).distinct();


        terrdivtype = skClientWrapper.generateSk(terrdivtype, transformConfiguration.getStgTable(), transformConfiguration.getLoadMode(), transformConfiguration);

        terrdivtype = terrdivtype.select(
                col(ColumnsName.TERR_DIV_TYPE_SK).as(ColumnsName.SID),
                lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.TERR_DIV_TYPE_DP_CODE),
                col(ColumnsName.DIV_TYPE_CODE).as(ColumnsName.DIV_TYPE_CODE),
                col(ColumnsName.TERR_DIV_TYPE_SK).as(ColumnsName.TERR_DIV_TYPE_SK),
                lit(false).as(ColumnsName.DIRTY_FLAG)
        );

        return terrdivtype;
    }
}
