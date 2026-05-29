package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;

public class DivTransformation extends BaseTransformation {

    public static final String CODE = DIV;
    public static final String SETS = format("%s.%s", DP_CODE, DIV_DS_CODE);

    public DivTransformation(App app) {
        super(app, CODE, SETS);
    }

    @Override
    public void run() {
        Dataset<Row> div_raw = readTable(STG_SCHEMA, CHG_DIVH);

        div_raw = div_raw.select(
                col(FULLDIVCODE).as(SID),
                col(FULLDIVCODE).as(DIV_SID),
                col(FULLDIVCODE).as(DIV_CODE),
                col(SHORTDIVCODE).as(DIV_SHORT_DIV_CODE),
                col(CLOSEDATE).as(DIV_CLOSEDATE),
                col(OPENDATE).as(DIV_OPENDATE),
                concat(lit("0"), col(FULLDIVCODE)).as(DIV_SUBBRANCH_CODE),
                col(HEADDIVISIONCODE).as(DIV_PARENT_DIV_SID),

                when(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                        .otherwise(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM)).as(DIV_FULL_REG_NUM),

                when(col(FULLNAME).equalTo("ПАО Сбербанк"),
                        when(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null).otherwise(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM)))
                        .otherwise(
                                when(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM).equalTo("-"), null)
                                        .otherwise(substring_index(col(DIV_REQUISITEDIVISION_REGISTRATIONNUM), "/", -1))).cast(IntegerType).as(DIV_REG_ORD),

                when(col(ADDRESS_ADDRFACT_TIMEZONE).equalTo("USZ1"), "KALT")
                        .otherwise(col(ADDRESS_ADDRFACT_TIMEZONE)).as(DIV_TIME_ZONE_TIME_ZONE_SID),

                col(ADDRESS_LEGAL_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                col("address_addrfact_fiascode").as(ADDRESS_FIASCODE_DURTY),
                col("gold_okato").as(ADDR_OKATO2_SID),
                col("gold_oktmo").as(ADDR_OKTMO2_SID)

        ).filter(" coalesce(end_dt, '9999-12-31') = '9999-12-31' ").distinct();

        Dataset<Row> div = div_raw.select(
                lit(false).as(DELETED_FLAG),
                col(SID).as(SID),
                col(DIV_SID).as(DIV_SID),
                lit(DATA_PRODUCT).as(DIV_DP_CODE),
                col(DIV_CODE).as(DIV_CODE),
                col(DIV_SHORT_DIV_CODE).as(DIV_SHORT_DIV_CODE),
                col(DIV_CLOSEDATE).as(DIV_CLOSEDATE),
                col(DIV_OPENDATE).as(DIV_OPENDATE),
                col(DIV_SUBBRANCH_CODE).as(DIV_SUBBRANCH_CODE),
                col(DIV_PARENT_DIV_SID).as(DIV_PARENT_DIV_SID),
                lit(DATA_PRODUCT).as(DIV_PARENT_DIV_DP_CODE),

                when(col(ADDR_SRC_TXT).isNotNull(), AddrTransformation.getKey("|")).otherwise(null).as(DIV_LEGAL_ADDR_SID),
                when(col(ADDR_SRC_TXT).isNotNull(), lit(DATA_PRODUCT)).otherwise(null).as(DIV_LEGAL_ADDR_DP_CODE),

                lit(false).as(DIRTY_FLAG),

                col(DIV_FULL_REG_NUM).as(DIV_FULL_REG_NUM),

                col(DIV_REG_ORD).as(DIV_REG_ORD),

                col(DIV_TIME_ZONE_TIME_ZONE_SID).as(DIV_TIME_ZONE_TIME_ZONE_SID),
                lit(DATA_PRODUCT).as(DIV_TIME_ZONE_TIME_ZONE_DP_CODE)
        ).distinct();

        // Дедубликация по SID
        div = div
                .withColumn("_test_dedup_rn",
                        row_number().over(Window.partitionBy(col(SID)).orderBy(col(DIV_LEGAL_ADDR_SID).asc())))
                .filter(col("_test_dedup_rn").equalTo(1))
                .drop("_test_dedup_rn");

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(DIV_SID, DIV);
        columnEntities.put(DIV_PARENT_DIV_SID, DIV);
        columnEntities.put(DIV_LEGAL_ADDR_SID, ADDR);
        columnEntities.put(DIV_TIME_ZONE_TIME_ZONE_SID, TIMEZONE);

        Dataset<Row> result = addSk(div, DIV, columnEntities);

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }

    public static void main(String[] args) {
        runApp(args, DivTransformation::new);
    }
}
