package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.PublisherMode;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Recordset;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TERRDIVTYPE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.SRC_PROVIDER_SFSDIVISION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;

public class TerrDivTypeTransformation extends BaseTransformation {

    public static final String CODE = TERRDIVTYPE;

    public TerrDivTypeTransformation(App app) {
        super(app, CODE, "usl_vspc_idl.terr_div_type.hub");
    }

    @Override
    public void run() {
        Dataset<Row> terr_div_type = read(SRC_PROVIDER_SFSDIVISION).kap803Usl().snp().get();
        // Хаб типов подразделения: ключ строки — код типа (divisiontype00), одна строка на код.
        // Набор полей и .distinct() — как в 3.0 (TerrDivTypeTransformer.java:31-39).
        terr_div_type = terr_div_type
                .select(
                        lit(false).as(DELETED_FLAG),
                        col(DIVISIONTYPE00).as(SID),
                        lit(DATA_PRODUCT).as(TERR_DIV_TYPE_DP_CODE),
                        col(DIVISIONTYPE00).as(DIV_TYPE_CODE),
                        col(DIVISIONTYPE00).as(TERR_DIV_TYPE_SID),
                        lit(false).as(DIRTY_FLAG)
                ).distinct();

        Dataset<Row> result = addSk(terr_div_type, TERRDIVTYPE, Arrays.asList("@terr_div_type")).dropDuplicates("terr_div_type_sk");
        publish(result)
                .calculationMode(PublisherMode.sparkRebalance())
                .recordsetMasks("usl_vspc_idl.terr_div_type.hub")
                .launch();
        registerChangedSets(Recordset.recordsets(getApp(), "usl_vspc_idl.terr_div_type.hub"));
    }
}
