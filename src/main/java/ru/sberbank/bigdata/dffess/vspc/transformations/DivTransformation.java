package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.Arrays;
import java.util.List;

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

        // Собственный PK хаба (div_sk) — НАТУРАЛЬНЫЙ, строго как в 3.0: там essa initSk удалял @"div" из
        // атрибутов ССК, поэтому в skClient.map уходили ТОЛЬКО FK-атрибуты, а div_sk оставался значением
        // SELECT (FULLDIVCODE). Воспроизводим то же самое: явный список FK-attributeCode (без собственного
        // div_sk) -> div_sk становится суррогатом FK'ов, а собственный ключ материализуется натурально ниже.
        List<String> fkAttributes = Arrays.asList(
                "parent@div",
                "legal@addr",
                "div_time_zone@time_zone_rf"
        );

        // Онбординг сущности div в ССК на INIT сохраняется (skInitOnLoad()=true -> skClient.init),
        // чтобы сателлиты div_h/divtype/divdivrelation продолжали переиспользовать суррогаты @div.
        Dataset<Row> result = addSk(div, DIV, fkAttributes);

        // div_sk — натуральный ключ (SID == FULLDIVCODE), материализуется напрямую, НЕ через ССК
        // (аналог эталона R3 DivDivRelation). Тип BIGINT кастует publisher по DDL, как в 3.0.
        result = result.withColumn(DIV_SK, col(SID)).drop(DIV_SID);

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }

    public static void main(String[] args) {
        runApp(args, DivTransformation::new);
    }
}
