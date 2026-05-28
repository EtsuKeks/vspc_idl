package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_M_SFS_0002_SFSDIVISION;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TERRDIVTYPE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TERRDIVTYPE_DS_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.VSPC_SNP_SRC_SCHEMA;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;

public class TerrDivTypeTransformation extends BaseTransformation {

    public static final String CODE = TERRDIVTYPE;
    public static final String SETS = format("%s.%s", DP_CODE, TERRDIVTYPE_DS_CODE);

    public TerrDivTypeTransformation(App app) {
        super(app, CODE, SETS);
    }

    @Override
    public void run() {
        Dataset<Row> terrdivtype = selectFrom(VSPC_SNP_SRC_SCHEMA, EBX_REP_M_SFS_0002_SFSDIVISION);
        terrdivtype = terrdivtype
                .select(
                        col(DIVISIONTYPE00).as(SID),
                        lit(DATA_PRODUCT).as(TERR_DIV_TYPE_DP_CODE),
                        col(DIVISIONTYPE00).as(DIV_TYPE_CODE),
                        col(DIVISIONTYPE00).as(TERR_DIV_TYPE_SID),
                        lit(false).as(DIRTY_FLAG)
                ).distinct();

        terrdivtype = terrdivtype.select(
                lit(false).as(DELETED_FLAG),
                col(TERR_DIV_TYPE_SID).as(SID),
                lit(DATA_PRODUCT).as(TERR_DIV_TYPE_DP_CODE),
                col(DIV_TYPE_CODE).as(DIV_TYPE_CODE),
                col(TERR_DIV_TYPE_SID).as(TERR_DIV_TYPE_SID),
                lit(false).as(DIRTY_FLAG)
        ).distinct();

        Dataset<Row> result = addSk(terrdivtype, TERRDIVTYPE, "@terr_div_type");

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }

    public static void main(String[] args) {
        runApp(args, TerrDivTypeTransformation::new);
    }
}
