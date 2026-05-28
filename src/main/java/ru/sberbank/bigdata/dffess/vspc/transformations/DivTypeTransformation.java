package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIV;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVTYPE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVTYPE_DS_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_M_SFS_0002_SFSDIVISION;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TERRDIVTYPE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.VSPC_SNP_SRC_SCHEMA;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class DivTypeTransformation extends BaseTransformation {

    public static final String CODE = DIVTYPE;
    public static final String SETS = format("%s.%s", DP_CODE, DIVTYPE_DS_CODE);

    public DivTypeTransformation(App app) {
        super(app, CODE, SETS);
    }

    @Override
    public void run() {
        Dataset<Row> divtype = selectFrom(VSPC_SNP_SRC_SCHEMA, EBX_REP_M_SFS_0002_SFSDIVISION);
        divtype = divtype
                .select(
                        lit(false).as(DELETED_FLAG),
                        lit(VspcConstant.getCtlValidFromStr()).cast("date").as(START_DT_FIELD_NAME),
                        lit(PLUS_INFINITY_DT).cast("date").as(END_DT_FIELD_NAME),
                        col(ColumnsName.FULLDIVCODE).as(ColumnsName.DIV_SID),
                        lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_DP_CODE),
                        col(ColumnsName.DIVISIONTYPE00).as(ColumnsName.DIV_TYPE_TERR_DIV_TYPE_SID),
                        lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.DIV_TYPE_TERR_DIV_TYPE_DP_CODE),
                        lit(false).as(ColumnsName.DIRTY_FLAG)
                ).distinct();

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(ColumnsName.DIV_TYPE_TERR_DIV_TYPE_SID, TERRDIVTYPE);
        columnEntities.put(ColumnsName.DIV_SID, DIV);

        Dataset<Row> result = addSk(divtype, DIVTYPE,  columnEntities);

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }

    public static void main(String[] args) {
        runApp(args, DivTypeTransformation::new);
    }
}
