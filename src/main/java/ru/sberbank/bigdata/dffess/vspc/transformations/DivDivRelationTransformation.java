package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIV;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVDIVRELATION;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVDIVRELATION_DS_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_M_SFS_0002_SFSDIVISION;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_M_SFS_0002_SFSDIVISION_SUCCESSORS;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.VSPC_SNP_SRC_SCHEMA;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class DivDivRelationTransformation extends BaseTransformation {

    public static final String CODE = DIVDIVRELATION;
    public static final String SETS = format("%s.%s", DP_CODE, DIVDIVRELATION_DS_CODE);

    public DivDivRelationTransformation(App app) {
        super(app, CODE, SETS);
    }

    @Override
    public void run() {
        Dataset<Row> divsuccessor = selectSQL(
                "SELECT \n" +
                        "   successor.id as source_id, \n" +
                        "   sfs1.fulldivcode as source_sid, \n" +
                        "   sfs2.fulldivcode as successor_sid \n" +
                        " FROM " + VSPC_SNP_SRC_SCHEMA + "." + EBX_REP_M_SFS_0002_SFSDIVISION_SUCCESSORS + " as successor \n" +
                        "   LEFT JOIN " + VSPC_SNP_SRC_SCHEMA + "." + EBX_REP_M_SFS_0002_SFSDIVISION + " as sfs1 \n" +
                        "       on successor.id = sfs1.id \n" +
                        "   LEFT JOIN " + VSPC_SNP_SRC_SCHEMA + "." + EBX_REP_M_SFS_0002_SFSDIVISION + " as sfs2 \n" +
                        "       on successor.successors_id_successor = sfs2.id " +
                        "   WHERE TRIM(lower(successor.successors_successorstype)) = 'правопреемник' "
        ).distinct();

        divsuccessor = divsuccessor
                .select(
                        lit(VspcConstant.getCtlValidFromStr()).cast("date").as(START_DT_FIELD_NAME),
                        lit(PLUS_INFINITY_DT).cast("date").as(END_DT_FIELD_NAME),
                        getKey("|").as(SID),
                        getKey("|").as(DIV_DIV_RELATION_SID),
                        lit(DATA_PRODUCT).as(DIV_DIV_RELATION_DP_CODE),

                        col("source_sid").as(SOURCE_DIV_SID),
                        lit(DATA_PRODUCT).as(SOURCE_DIV_DP_CODE),
                        col("successor_sid").as(SUCCESSOR_DIV_SID),
                        lit(DATA_PRODUCT).as(SUCCESSOR_DIV_DP_CODE),

                        lit(false).as(DIRTY_FLAG),
                        lit(false).as(DELETED_FLAG)

                ).distinct();

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(DIV_DIV_RELATION_SID, DIVDIVRELATION);
        columnEntities.put(SOURCE_DIV_SID, DIV);
        columnEntities.put(SUCCESSOR_DIV_SID, DIV);

        Dataset<Row> result = addSk(divsuccessor, DIVDIVRELATION, columnEntities);

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }


    public static Column getKey(String sep) {
        return concat_ws(sep,
                coalesce(col("source_sid"), lit("NULL").cast("string")),
                coalesce(col("successor_sid"), lit("NULL").cast("string"))
        );
    }

    public static void main(String[] args) {
        runApp(args, DivDivRelationTransformation::new);
    }
}
