package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.ADDR;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.ADDR_DS_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.CHG_DIVH;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.GAR_FIAS_IDL;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.RDM_IDL_SPARK;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;

public class AddrTransformation extends BaseTransformation {

    public static final String CODE = ADDR;
    public static final String SETS = format("%s.%s", DP_CODE, ADDR_DS_CODE);

    public AddrTransformation(App app) {
        super(app, CODE, SETS);
    }

    @Override
    public void run() {
        Dataset<Row> addr_raw = readTable(STG_SCHEMA, CHG_DIVH);

        Dataset<Row> addr_fact = addr_raw.select(
                col(ADDRESS_ADDRFACT_POSTCODE).as(ADDR_IDX_NUM),
                col(ADDRESS_ADDRFACT_ADDRAREA_AREANAME).as(ADDR_DISTRICT_NAME),
                col(ADDRESS_ADDRFACT_ADDPLACE_PLACENAME).as(ADDR_LOCTY_NAME),
                col(ADDRESS_SUBJECTCODE).as(ADDR_REG_ADDR_NAME),
                col(ADDRESS_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                col(ADDRESS_ADDRFACT_ADDRCITY_CITYNAME).as(ADDR_CITY_NAME),

                lit(null).as(ADDR_FIAS_CODE),
                lit(null).as(ADDRESS_FIASCODE_DURTY),

                col(ADDRESS_ADDRFACT_ADDRSTREET_HOUSENUMBERS).as(ADDR_HOUSE_NUM),
                col(ADDRESS_ADDRFACT_ADDRSTREET_STREETNAME).as(ADDR_STREET_NAME),

                when(col(ADDRESS_COUNTRY).equalTo("РОССИЯ(RU)"), "643").when(col(ADDRESS_COUNTRY).equalTo("ИНДИЯ(IN)"), "356").otherwise(null).as(ADDR_CNTRY_SID),

                lit(null).as(ADDR_OKATO2_SID),
                lit(null).as(ADDR_OKTMO2_SID),

                when(col(ADDRESS_SUBJECTCODE).contains("Агинский Бурятский"), "712")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Алтайский край"), "301")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Амурская область"), "308")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Архангельская область"), "314")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Астраханская область"), "320")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Белгородская область"), "326")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Брянская область"), "332")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Владимирская область"), "338")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Волгоградская область"), "344")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Вологодская область"), "350")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Воронежская область"), "356")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Донецкая Народная Республика"), "119")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Еврейская АО"), "701")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Еврейская автономная область"), "701")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Забайкальский край"), "302")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Запорожская область"), "358")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ивановская область"), "362")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Иркутская область"), "368")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Кабардино-Балкарская"), "125")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Калининградская область"), "374")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Калужская область"), "380")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Камчатский край"), "303")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Карачаево-Черкесская Республика"), "133")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Кемеровская область"), "392")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Кировская область"), "398")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Коми-Пермяцкий"), "718")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Корякский"), "724")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Костромская область"), "404")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Краснодарский край"), "410")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Красноярский край"), "416")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Курганская область"), "422")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Курская область"), "428")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ленинградская область"), "434")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Липецкая область"), "440")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Луганская Народная Республика"), "144")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Магаданская область"), "446")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Москва"), "1")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Московская область"), "452")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Мурманская область"), "458")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ненецкий АО"), "730")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ненецкий автономный округ"), "730")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Нижегородская область"), "464")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Новгородская область"), "470")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Новосибирская область"), "476")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Омская область"), "484")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Оренбургская область"), "490")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Орловская область"), "496")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Пензенская область"), "504")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Пермский край"), "417")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Приморский край"), "516")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Псковская область"), "522")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Республика Адыгея"), "101")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Алтай"), "105")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Башкортостан"), "109")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Бурятия"), "113")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Дагестан"), "117")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Ингушетия"), "121")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Калмыкия"), "129")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Карелия"), "137")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Коми"), "141")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Крым"), "142")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Марий Эл"), "145")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Мордовия"), "149")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Республика Саха"), "181")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Республика Северная Осетия"), "153")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Республика Татарстан"), "157")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Тыва"), "161")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Хакасия"), "169")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ростовская область"), "528")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Рязанская область"), "534")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Самарская область"), "540")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Санкт-Петербург"), "2")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Саратовская область"), "546")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Сахалинская область"), "552")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Свердловская область"), "558")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Севастополь"), "3")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Смоленская область"), "564")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ставропольский край"), "570")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Таймырский"), "736")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Тамбовская область"), "576")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Тверская область"), "582")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Томская область"), "588")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Тульская область"), "594")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Тюменская область"), "600")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Удмуртская Республика"), "165")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ульяновская область"), "606")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Усть - Ордынский Бурятский АО"), "742")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Хабаровский край"), "612")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Ханты-Мансийский"), "748")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Херсонская область"), "609")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Челябинская область"), "618")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Чеченская Республика"), "173")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Читинская область"), "624")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Чувашская Республика"), "177")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Чукотский"), "754")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Эвенкийский"), "760")
                        .when(col(ADDRESS_SUBJECTCODE).contains("Ямало-Ненецкий"), "766")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ярославская область"), "630")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Не субъект РФ"), lit(null))
                        .as(ADDR_RUS_FED_SUBJ_SID)

        ).where(col(ADDRESS_LOCATIONADDRESS).isNotNull());

        Dataset<Row> addr_legal = addr_raw.select(
                col(ADDRESS_POSTCODE).as(ADDR_IDX_NUM),
                lit(null).as(ADDR_DISTRICT_NAME),
                lit(null).as(ADDR_LOCTY_NAME),
                lit(null).as(ADDR_REG_ADDR_NAME),
                col(ADDRESS_LEGAL_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                lit(null).as(ADDR_CITY_NAME),

                col("gold_fias_code").as(ADDR_FIAS_CODE),
                col("address_addrfact_fiascode").as(ADDRESS_FIASCODE_DURTY),

                lit(null).as(ADDR_HOUSE_NUM),
                lit(null).as(ADDR_STREET_NAME),
                lit(null).as(ADDR_CNTRY_SID),

                col("gold_okato").as(ADDR_OKATO2_SID),
                col("gold_oktmo").as(ADDR_OKTMO2_SID),

                lit(null).as(ADDR_RUS_FED_SUBJ_SID)
        ).where(col(ADDRESS_LEGAL_LOCATIONADDRESS).isNotNull());

        Dataset<Row> addr = addr_fact.union(addr_legal).distinct();

        addr = addr.select(
                getKey("|").as(SID),
                getKey("|").as(ADDR_SID),
                col(ADDR_IDX_NUM).as(ADDR_IDX_NUM),
                col(ADDR_DISTRICT_NAME).as(ADDR_DISTRICT_NAME),
                col(ADDR_LOCTY_NAME).as(ADDR_LOCTY_NAME),
                col(ADDR_REG_ADDR_NAME).as(ADDR_REG_ADDR_NAME),
                col(ADDR_SRC_TXT).as(ADDR_SRC_TXT),
                col(ADDR_CITY_NAME).as(ADDR_CITY_NAME),
                col(ADDR_FIAS_CODE).as(ADDR_FIAS_CODE),
                col(ADDR_HOUSE_NUM).as(ADDR_HOUSE_NUM),
                col(ADDR_STREET_NAME).as(ADDR_STREET_NAME),
                lit(DATA_PRODUCT).as(ADDR_DP_CODE),

                when(col(ADDR_RUS_FED_SUBJ_SID).isNotNull(), col(ADDR_RUS_FED_SUBJ_SID)).as(ADDR_RUS_FED_SUBJ_SID),
                when(col(ADDR_RUS_FED_SUBJ_SID).isNotNull(), lit(RDM_IDL_SPARK)).as(ADDR_REG_FED_SUBJ_DP_CODE),

                when(col(ADDR_CNTRY_SID).isNotNull(), col(ADDR_CNTRY_SID)).as(ADDR_CNTRY_SID),
                when(col(ADDR_CNTRY_SID).isNotNull(), lit(RDM_IDL_SPARK)).as(ADDR_CNTRY_DP_CODE),

                col(ADDR_OKATO2_SID).as(ADDR_OKATO2_SID),
                lit(RDM_IDL_SPARK).as(ADDR_OKATO2_DP_CODE),
                lit(false).as(DIRTY_FLAG),
                lit(false).as(DELETED_FLAG),

                when(col(ADDR_FIAS_CODE).isNotNull(), col(ADDR_FIAS_CODE)).as(ADDRESS_FIASCODE_SID),
                when(col(ADDR_FIAS_CODE).isNotNull(), lit(GAR_FIAS_IDL)).as(ADDRESS_FIASCODE_DP_CODE),
                lit(">non_stndrt").as(ADDRESS_TYPE_CODE)

        ).distinct();

        addr = addr.groupBy(col(SID), col(ADDR_SID))
                .agg(
                        max(col(ADDR_IDX_NUM)).as(ADDR_IDX_NUM),
                        max(col(ADDR_DISTRICT_NAME)).as(ADDR_DISTRICT_NAME),
                        max(col(ADDR_LOCTY_NAME)).as(ADDR_LOCTY_NAME),
                        max(col(ADDR_REG_ADDR_NAME)).as(ADDR_REG_ADDR_NAME),
                        max(col(ADDR_SRC_TXT)).as(ADDR_SRC_TXT),
                        max(col(ADDR_CITY_NAME)).as(ADDR_CITY_NAME),
                        max(col(ADDR_FIAS_CODE)).as(ADDR_FIAS_CODE),
                        max(col(ADDR_HOUSE_NUM)).as(ADDR_HOUSE_NUM),
                        max(col(ADDR_STREET_NAME)).as(ADDR_STREET_NAME),
                        max(col(ADDR_DP_CODE)).as(ADDR_DP_CODE),
                        max(col(ADDR_RUS_FED_SUBJ_SID)).as(ADDR_RUS_FED_SUBJ_SID),
                        max(col(ADDR_REG_FED_SUBJ_DP_CODE)).as(ADDR_REG_FED_SUBJ_DP_CODE),
                        max(col(ADDR_CNTRY_SID)).as(ADDR_CNTRY_SID),
                        max(col(ADDR_CNTRY_DP_CODE)).as(ADDR_CNTRY_DP_CODE),
                        max(col(ADDR_OKATO2_SID)).as(ADDR_OKATO2_SID),
                        max(col(ADDR_OKATO2_DP_CODE)).as(ADDR_OKATO2_DP_CODE),
                        max(col(DIRTY_FLAG)).as(DIRTY_FLAG),
                        max(col(DELETED_FLAG)).as(DELETED_FLAG),
                        max(col(ADDRESS_FIASCODE_SID)).as(ADDRESS_FIASCODE_SID),
                        max(col(ADDRESS_FIASCODE_DP_CODE)).as(ADDRESS_FIASCODE_DP_CODE),
                        max(col(ADDRESS_TYPE_CODE)).as(ADDRESS_TYPE_CODE)
                ).distinct();

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(ADDR_SID, ADDR);
        columnEntities.put(ADDR_RUS_FED_SUBJ_SID, "rus_fed_subj");
        columnEntities.put(ADDR_CNTRY_SID, "cntry");
        columnEntities.put(ADDR_OKATO2_SID, "okato2");

        Dataset<Row> result = addSk(addr, ADDR, columnEntities);

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }

    public static Column getKey(String sep) {
        return when(
                col(ADDR_SRC_TXT).isNull()
                        .and(col(ADDRESS_FIASCODE_DURTY).isNull())
                        .and(col(ADDR_OKATO2_SID).isNull())
                        .and(col(ADDR_OKTMO2_SID).isNull()),
                null
        ).otherwise(
                concat_ws(sep,
                        coalesce(col(ADDRESS_FIASCODE_DURTY), lit("NULL").cast("string")),
                        coalesce(col(ADDR_SRC_TXT), lit("NULL").cast("string")),
                        coalesce(col(ADDR_OKATO2_SID), lit("NULL").cast("string")),
                        coalesce(col(ADDR_OKTMO2_SID), lit("NULL").cast("string"))
                )
        );
    }

    public static void main(String[] args) {
        runApp(args, AddrTransformation::new);
    }
}
