package ru.sberbank.bigdata.dffess.vspc.tables.divh;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName;
import ru.sberbank.bigdata.dffess.vspc.constant.TablesName;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
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
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.CTL_VALIDFROM;

public class DivHTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> divh = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(TablesName.EBX_REP_M_SFS_0002_SFSDIVISION), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);
        divh = divh
                .select(
                        lit(VspcConstant.getCurrentHourCutOffDate().toString()).as(CTL_VALIDFROM),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SK),
                        col(ColumnsName.FULLNAME).as(ColumnsName.DIV_H_NAME),
                        to_date(col(ColumnsName.CLOSEDATE)).as(ColumnsName.DIV_CLOSEDATE),
                        to_date(col(ColumnsName.OPENDATE)).as(ColumnsName.DIV_OPENDATE),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_CODE),
                        lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                        lit(VspcConstant.RDM_IDL_SPARK).as(ColumnsName.DIV_UPS_PAY_SYS_PRTCPNT_DP_CODE),
                        col(ColumnsName.REQUSITEDIVISION_BIC).as(ColumnsName.DIV_UPS_PAY_SYS_PRTCPNT_SK),
                        lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_FACT_ADDR_DP_CODE),
                        col(ColumnsName.ID).as(ColumnsName.DIV_FACT_ADDR_SK),
                        lit(false).as(ColumnsName.DIRTY_FLAG)
                );

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(ColumnsName.DIV_UPS_PAY_SYS_PRTCPNT_SK, "pay_sys_prtcpnt");
        columnEntities.put(ColumnsName.DIV_FACT_ADDR_SK, TablesName.ADDR);

        divh = skClientWrapper.incrementSk(divh, transformConfiguration.getStgTable(), columnEntities, transformConfiguration);
        
        return divh;
    }
}
