package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.VspcLoadDate;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Recordset;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_sub;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIRTY_FLAG;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIVISIONTYPE00;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_SID;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_TYPE_TERR_DIV_TYPE_DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_TYPE_TERR_DIV_TYPE_SID;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.FULLDIVCODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVTYPE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.SRC_PROVIDER_SFSDIVISION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.MINUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class DivTypeTransformation extends BaseTransformation {

    public static final String CODE = DIVTYPE;

    public DivTypeTransformation(App app) {
        super(app, CODE, "usl_vspc_idl.div.sat=type");
    }

    @Override
    public void run() {
        Dataset<Row> divtype = read(SRC_PROVIDER_SFSDIVISION).kap803Usl().snp().get();

        divtype = divtype
                .select(
                        lit(false).as(DELETED_FLAG),
                        lit(PLUS_INFINITY_DT).cast("date").as(END_DT_FIELD_NAME),
                        col(FULLDIVCODE).as(DIV_SID),
                        lit(DATA_PRODUCT).as(DIV_DP_CODE),
                        col(DIVISIONTYPE00).as(DIV_TYPE_TERR_DIV_TYPE_SID),
                        lit(DATA_PRODUCT).as(DIV_TYPE_TERR_DIV_TYPE_DP_CODE),
                        lit(false).as(DIRTY_FLAG)
                ).distinct();

        Dataset<Row> result = addSk(divtype, DIVTYPE, Arrays.asList("@div", "div_type@terr_div_type"));

        // start_dt в 3.0 ставил лоадер: INIT 1900-01-01, INCR ctl_validfrom-1 (essa-common-lib DataLoaderHistSnp.java:44,47,56-57)
        result = result.withColumn(START_DT_FIELD_NAME,
                getApp().ctx().getLoadMode() == App.LoadMode.INCREMENT
                        ? date_sub(lit(VspcLoadDate.sqlDate()), 1)
                        : lit(MINUS_INFINITY_DT).cast("date"));

        publishChecked(result, "usl_vspc_idl.div.sat=type");
        registerChangedSets(Recordset.recordsets(getApp(), "usl_vspc_idl.div.sat=type"));
    }
}
