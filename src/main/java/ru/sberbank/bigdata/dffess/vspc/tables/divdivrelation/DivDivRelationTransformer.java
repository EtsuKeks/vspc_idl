package ru.sberbank.bigdata.dffess.vspc.tables.divdivrelation;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
import static org.apache.spark.sql.functions.col;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.getCtlValidFromStr;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;

public class DivDivRelationTransformer implements EntityTransformer {

    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> divsuccessor = sparkWrapper.fromSql(
                "SELECT \n" +
                "   successor.id as source_id, \n" +
                "   sfs1.fulldivcode as source_sid, \n" +
                "   sfs2.fulldivcode as successor_sid \n" +
                " FROM usl_vspc_idl_in_snp.ebx_rep_m_sfs_0002_sfsdivision_successors as successor \n" +
                "   LEFT JOIN usl_vspc_idl_in_snp.ebx_rep_m_sfs_0002_sfsdivision as sfs1 \n" +
                "       on successor.id = sfs1.id \n" +
                "   LEFT JOIN usl_vspc_idl_in_snp.ebx_rep_m_sfs_0002_sfsdivision as sfs2 \n" +
                "       on successor.successors_id_successor = sfs2.id " +
                "   WHERE TRIM(lower(successor.successors_successorstype)) = 'правопреемник' "
        ).distinct();

        divsuccessor = divsuccessor
                .select(
                        lit(getCtlValidFromStr()).as(CTL_VALIDFROM),
                        getKey("|").as(SID),

                        getKey("|").as(DIV_DIV_RELATION_SK),
                        lit(DATA_PRODUCT).as(DIV_DIV_RELATION_DP_CODE),

                        col("source_sid").as(SOURCE_DIV_SK),
                        lit(DATA_PRODUCT).as(SOURCE_DIV_DP_CODE),
                        col("successor_sid").as(SUCCESSOR_DIV_SK),
                        lit(DATA_PRODUCT).as(SUCCESSOR_DIV_DP_CODE),

                        lit(false).as(DIRTY_FLAG),
                        lit(false).as(DELETED_FLAG)

                        ).distinct();

        Map<String, String> columnEntities = new HashMap<>();

        columnEntities.put(SOURCE_DIV_SK, DIV);
        columnEntities.put(SUCCESSOR_DIV_SK, DIV);

        divsuccessor = skClientWrapper.generateSk(divsuccessor, DIVDIVRELATION, transformConfiguration.getLoadMode(), columnEntities, transformConfiguration);

        return divsuccessor;
    }

    public static Column getKey(String sep) {
        return concat_ws(sep,
                coalesce(col("source_sid"), lit("NULL").cast("string")),
                coalesce(col("successor_sid"), lit("NULL").cast("string"))
        );
    }
}
