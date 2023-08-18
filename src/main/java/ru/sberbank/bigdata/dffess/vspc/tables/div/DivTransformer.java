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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.essa.common.constant.DmBuilderConst.LEFT_JOIN;

public class DivTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(DmBuilder dmBuilder, SkClientWrapper skClientWrapper, HdfsHelper hdfsHelper, CtlStatisticHelper ctlStatisticHelper, TransformConfiguration transformConfiguration, EntityProxyRelation entityProxyRelation, Logger logger) {

        Dataset<Row> div = dmBuilder.getDataset(transformConfiguration.getSrcSchemaSnp(), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);

        List<String> departAttrs = new ArrayList<>(Arrays.asList(ColumnsName.FULLDIVCODE, ColumnsName.HEADDIVISIONCODE));

        Dataset<Row> depart = dmBuilder.getDataset(transformConfiguration.getSrcSchemaSnp(), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION, departAttrs);
        depart = prefillSkWithParent(dmBuilder, transformConfiguration, hdfsHelper, depart, logger, skClientWrapper);
        Dataset<Row> result = dmBuilder.join(div,depart, LEFT_JOIN, div.col(ColumnsName.HEADDIVISIONCODE).equalTo(depart.col(ColumnsName.FULLDIVCODE)), false);


        result = result
                .select(
                    col(ColumnsName.FULLDIVCODE).as(ColumnsName.SID),
                    col(ColumnsName.DIV_SK).as(ColumnsName.DIV_SK),
                    col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_CODE),
                    col(ColumnsName.DIV_SK_HEAD).as(ColumnsName.HEADDIVISIONCODE),
                    col(ColumnsName.CLOSEDATE).as(ColumnsName.DIV_CLOSEDATE),
                    col(ColumnsName.OPENDATE).as(ColumnsName.DIV_OPENDATE),
                    lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                    lit(false).as(ColumnsName.DIRTY_FLAG)
                );
        return result;


      /*  ddiv = div
                .select(
                        col(ColumnsName.ID).as(ColumnsName.SID),
                        col(ColumnsName.ID).as(ColumnsName.DIV_SK),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_CODE),
                        col(ColumnsName.CLOSEDATE).as(ColumnsName.DIV_CLOSEDATE),
                        col(ColumnsName.OPENDATE).as(ColumnsName.DIV_OPENDATE),
                        lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                        lit(false).as(ColumnsName.DIRTY_FLAG)
                );

        div = skClientWrapper.generateSk(div, transformConfiguration.getStgTable(), transformConfiguration.getLoadMode(), transformConfiguration);
        */
        //return div;
    }


    private Dataset<Row> prefillSkWithParent(DmBuilder dmBuilder, TransformConfiguration transformConfiguration, HdfsHelper hdfsHelper, Dataset<Row> depart, Logger logger, SkClientWrapper skClientWrapper) {
        depart = depart.select(
                col(ColumnsName.FULLDIVCODE).as(ColumnsName.SID),
                col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SK),
                col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_CODE),
                col(ColumnsName.CLOSEDATE).as(ColumnsName.DIV_CLOSEDATE),
                col(ColumnsName.OPENDATE).as(ColumnsName.DIV_OPENDATE),
                lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                lit(false).as(ColumnsName.DIRTY_FLAG)
        );

        depart = skClientWrapper.generateSk(depart, transformConfiguration.getStgTable(), transformConfiguration.getLoadMode(), transformConfiguration);

        depart = depart.select(
                col(ColumnsName.FULLDIVCODE).as(ColumnsName.SID),
                col(ColumnsName.DIV_SK).as(ColumnsName.DIV_SK),
                col(ColumnsName.DIV_SK).as(ColumnsName.DIV_SK_HEAD),
                col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_CODE),
                col(ColumnsName.CLOSEDATE).as(ColumnsName.DIV_CLOSEDATE),
                col(ColumnsName.OPENDATE).as(ColumnsName.DIV_OPENDATE),
                lit(VpscConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                lit(false).as(ColumnsName.DIRTY_FLAG)
        );

        return depart;
    }
}
