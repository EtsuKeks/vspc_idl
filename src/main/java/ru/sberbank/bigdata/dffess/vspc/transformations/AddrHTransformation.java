package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.VspcLoadDate;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Recordset;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.ADDRH;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.CHG_DIVH;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.RDM_IDL_SPARK;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;
import static ru.sberbank.bigdata.sl.rt.core.Constants.END_DT_FIELD_NAME;
import static ru.sberbank.bigdata.sl.rt.core.Constants.MINUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.PLUS_INFINITY_DT;
import static ru.sberbank.bigdata.sl.rt.core.Constants.START_DT_FIELD_NAME;

public class AddrHTransformation extends BaseTransformation {

    public static final String CODE = ADDRH;

    public AddrHTransformation(App app) {
        super(app, CODE, "usl_vspc_idl.addr.sat=h");
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

        Dataset<Row> result = addSk(addrh, ADDRH, Arrays.asList("@addr", "address@okato2", "address@oktmo2"));

        // vspc-3.0 parity: start_dt историчной таблицы — в 3.0 её штамповал framework-лоадер
        // DataLoaderHistDiff (INIT: плоский to_date(lit(MIN_DATE))="1900-01-01", стр. 30-38+133-154;
        // INCR: date_sub(ctl_validfrom_to_start_dt,1), стр. 40-47). Штатный RT-FW Publisher даты не
        // создаёт — castToTargetStructure доливает отсутствующие колонки NULL (PublisherUtils.java:374-390),
        // из-за чего PK [addr_sk,start_dt] не сходится и D2/VANISH/DROPBAD shadow-калькулятора ненаблюдаемы.
        // Якорь «день загрузки» = VspcLoadDate.sqlDate() (дата шага трансформации) — тот же, что в закрывающих
        // формулах shadow (end := loadDate-2 ≡ another_ctl_validfrom-2): стык [.., loadDate-2][loadDate-1, ..] без разрыва, как в 3.0.
        result = result.withColumn(START_DT_FIELD_NAME, getApp().ctx().getLoadMode() == App.LoadMode.INCREMENT
                                                        ? date_sub(lit(VspcLoadDate.sqlDate()), 1)
                                                        : lit(MINUS_INFINITY_DT).cast("date"));

        publishChecked(result, "usl_vspc_idl.addr.sat=h");
        registerChangedSets(Recordset.recordsets(getApp(), "usl_vspc_idl.addr.sat=h"));
    }
}
