package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.ADDR;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.ADDRH;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.ADDRH_DS_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.CHG_DIVH;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.RDM_IDL_SPARK;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class AddrHTransformation extends BaseTransformation {

    public static final String CODE = ADDRH;
    public static final String SETS = format("%s.%s", DP_CODE, ADDRH_DS_CODE);

    public AddrHTransformation(App app) {
        super(app, CODE, SETS);
    }

    @Override
    public void run() {
        Dataset<Row> addr_raw = readTable(STG_SCHEMA, CHG_DIVH);

        Dataset<Row> addr_fact = addr_raw.select(
                col(ADDRESS_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                col(ADDRESS_LOCATIONADDRESS).as(ADDRESS_STANDARD_ADDRESS_TXT),

                lit(null).as(ADDR_FIAS_CODE),
                lit(null).as(ADDRESS_FIASCODE_DURTY),

                lit(null).as(ADDR_OKATO2_SID),
                lit(null).as(ADDR_OKTMO2_SID)

        ).where(col(ADDRESS_LOCATIONADDRESS).isNotNull());

        Dataset<Row> addr_legal = addr_raw.select(
                col(ADDRESS_LEGAL_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                col(ADDRESS_LEGAL_LOCATIONADDRESS).as(ADDRESS_STANDARD_ADDRESS_TXT),
                col("gold_fias_code").as(ADDR_FIAS_CODE),
                col("address_addrfact_fiascode").as(ADDRESS_FIASCODE_DURTY),
                col("gold_okato").as(ADDR_OKATO2_SID),
                col("gold_oktmo").as(ADDR_OKTMO2_SID)

        ).where(col(ADDRESS_LEGAL_LOCATIONADDRESS).isNotNull());

        Dataset<Row> addr = addr_fact.union(addr_legal).distinct();

        addr = addr.select(
                AddrTransformation.getKey("|").as(ADDR_SID),

                col(ADDRESS_STANDARD_ADDRESS_TXT).as(ADDRESS_STANDARD_ADDRESS_TXT),
                col(ADDR_FIAS_CODE).as(ADDR_FIAS_CODE),
                col(ADDR_OKATO2_SID).as(ADDR_OKATO2_SID),
                col(ADDR_OKTMO2_SID).as(ADDR_OKTMO2_SID)

        ).distinct();

        Dataset<Row> addrh = addr.select(
                lit(PLUS_INFINITY_DT).cast("date").as(END_DT_FIELD_NAME),
                col(ADDR_SID).as(ADDR_SID),
                lit(VspcConstant.DATA_PRODUCT).as(ADDR_DP_CODE),
                col(ADDRESS_STANDARD_ADDRESS_TXT).as(ADDRESS_STANDARD_ADDRESS_TXT),
                lit(false).as(DIRTY_FLAG),
                lit(false).as(DELETED_FLAG),

                when(col(ADDR_OKATO2_SID).isNotNull(), col(ADDR_OKATO2_SID)).as(ADDRH_OKATO2_SID),
                when(col(ADDR_OKATO2_SID).isNotNull(), lit(RDM_IDL_SPARK)).as(ADDRH_OKATO2_DP_CODE),

                when(col(ADDR_OKTMO2_SID).isNotNull(), col(ADDR_OKTMO2_SID)).as(ADDRH_OKTMO2_SID),
                when(col(ADDR_OKTMO2_SID).isNotNull(), lit(RDM_IDL_SPARK)).as(ADDRH_OKTMO2_DP_CODE),

                lit(">non_stndrt").as(ADDRESS_TYPE_CODE)
        ).distinct();

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(ADDR_SID, ADDR);
        columnEntities.put(ADDRH_OKATO2_SID, "okato2");
        columnEntities.put(ADDRH_OKTMO2_SID, "oktmo2");

        Dataset<Row> result = addSk(addrh, ADDRH, columnEntities);

        // ESSA-правило дат (DataLoaderHistDiff): INIT — 1900-01-01; INCR — новый ключ 1900-01-01,
        // существующий ctl_validfrom - 1. Новизна ключа определяется ПОСЛЕ генерации СК по addr_sk:
        // в SNP есть только *_sk, а *_sid на этом этапе уже дропнуты.
        Column startDt;
        if (isIncrement()) {
            result = flagNewKeys(result, ADDRH, ADDR_SK);
            startDt = when(col(NEW_KEY_FLAG), lit(VspcConstant.MIN_BUSINESS_DT))
                    .otherwise(date_sub(lit(VspcConstant.getCtlValidFromStr()).cast("date"), 1))
                    .cast("date");
            result = result.drop(NEW_KEY_FLAG);
        } else {
            startDt = lit(VspcConstant.MIN_BUSINESS_DT).cast("date");
        }
        result = result.withColumn(START_DT_FIELD_NAME, startDt);

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }

    @Override
    protected boolean skInitOnLoad() {
        // ESSA-лоадер этой таблицы вызывал incrementSk (map) безусловно, включая INIT
        return false;
    }

    public static void main(String[] args) {
        runApp(args, AddrHTransformation::new);
    }
}
