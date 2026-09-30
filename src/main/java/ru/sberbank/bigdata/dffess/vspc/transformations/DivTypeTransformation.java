package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_sub;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.when;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIRTY_FLAG;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIVISIONTYPE00;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_SID;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_TYPE_TERR_DIV_TYPE_DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_TYPE_TERR_DIV_TYPE_SID;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.FULLDIVCODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIV;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVTYPE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.DIVTYPE_DS_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TERRDIVTYPE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.SRC_PROVIDER_SFSDIVISION;
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
        Dataset<Row> divtype = read(SRC_PROVIDER_SFSDIVISION).kap803Usl().snp().get();

        // ESSA-правило дат (DataLoaderHistSnp): INIT — 1900-01-01; INCR — новый ключ 1900-01-01, существующий ctl_validfrom - 1
        Column startDt;
        if (isIncrement()) {
            divtype = flagNewKeys(divtype, DIVTYPE, DIV_SID);
            startDt = when(col(NEW_KEY_FLAG), lit(VspcConstant.MIN_BUSINESS_DT))
                    .otherwise(date_sub(lit(VspcConstant.getCtlValidFromStr()).cast("date"), 1))
                    .cast("date").as(START_DT_FIELD_NAME);
        } else {
            startDt = lit(VspcConstant.MIN_BUSINESS_DT).cast("date").as(START_DT_FIELD_NAME);
        }

        divtype = divtype
                .select(
                        lit(false).as(DELETED_FLAG),
                        startDt,
                        lit(PLUS_INFINITY_DT).cast("date").as(END_DT_FIELD_NAME),
                        col(FULLDIVCODE).as(DIV_SID),
                        lit(DATA_PRODUCT).as(DIV_DP_CODE),
                        col(DIVISIONTYPE00).as(DIV_TYPE_TERR_DIV_TYPE_SID),
                        lit(DATA_PRODUCT).as(DIV_TYPE_TERR_DIV_TYPE_DP_CODE),
                        lit(false).as(DIRTY_FLAG)
                ).distinct();

        // Дедупликация по SID защищает инкремент от дублей реплики; в INIT набор уже уникален (семантика release/3.0)
        if (isIncrement()) {
            divtype = divtype
                    .withColumn("_dedup_rn",
                            row_number().over(Window.partitionBy(col(DIV_SID)).orderBy(col(DIV_TYPE_TERR_DIV_TYPE_SID).asc())))
                    .filter(col("_dedup_rn").equalTo(1))
                    .drop("_dedup_rn");
        }

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(DIV_TYPE_TERR_DIV_TYPE_SID, TERRDIVTYPE);
        columnEntities.put(DIV_SID, DIV);

        Dataset<Row> result = addSk(divtype, DIVTYPE,  columnEntities);

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }

    public static void main(String[] args) {
        runApp(args, DivTypeTransformation::new);
    }
}
