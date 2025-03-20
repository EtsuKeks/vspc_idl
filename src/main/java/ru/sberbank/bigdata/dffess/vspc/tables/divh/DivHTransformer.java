package ru.sberbank.bigdata.dffess.vspc.tables.divh;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.tables.addr.AddrTransformer;
import ru.sberbank.bigdata.essa.common.logger.LoggerWrapper;
import ru.sberbank.bigdata.essa.common.slave.app.SparkWrapper;
import ru.sberbank.bigdata.essa.common.slave.hdfs.HdfsService;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityProxyRelation;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;
import ru.sberbank.bigdata.essa.common.slave.templates.SkClientWrapper;
import ru.sberbank.bigdata.essa.common.slave.templates.TransformConfiguration;
import ru.sberbank.bigdata.essa.common.utils.EntityStaticService;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.*;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.CTL_VALIDFROM;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.DELETED_FLAG;

public class DivHTransformer implements EntityTransformer {

    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> divh_raw = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(EBX_REP_M_SFS_0002_SFSDIVISION), EBX_REP_M_SFS_0002_SFSDIVISION);

        divh_raw = divh_raw.select(
                col(FULLDIVCODE).as(DIV_SK),
                col(FULLNAME).as(DIV_H_NAME),
                to_date(col(CLOSEDATE)).as(DIV_CLOSEDATE),
                to_date(col(OPENDATE)).as(DIV_OPENDATE),
                col(FULLDIVCODE).as(DIV_CODE),
                col(REQUSITEDIVISION_BIC).as(DIV_PAY_SYS_PRTCPNT_SK),

                //col(ColumnsName.ADDRESS_LOCATIONADDRESS).as(ColumnsName.DIV_FACT_ADDR_SK),

                coalesce(col(ADDRESS_ADDRFACT_POSTCODE), col(ADDRESS_POSTCODE)).as(ADDR_IDX_NUM),
                col(ADDRESS_ADDRFACT_ADDRAREA_AREANAME).as(ADDR_DISTRICT_NAME),
                col(ADDRESS_ADDRFACT_ADDPLACE_PLACENAME).as(ADDR_LOCTY_NAME),
                col(ADDRESS_SUBJECTCODE).as(ADDR_REG_ADDR_NAME),
                col(ADDRESS_LOCATIONADDRESS).as(ADDR_SRC_TXT),
                col(ADDRESS_ADDRFACT_ADDRCITY_CITYNAME).as(ADDR_CITY_NAME),
                col(ADDRESS_ADDRFACT_FIASCODE).as(ADDR_FIAS_CODE),
                col(ADDRESS_ADDRFACT_ADDRSTREET_HOUSENUMBERS).as(ADDR_HOUSE_NUM),
                col(ADDRESS_ADDRFACT_ADDRSTREET_STREETNAME).as(ADDR_STREET_NAME),
                when(col(ADDRESS_COUNTRY).equalTo("РОССИЯ(RU)"), "643").when(col(ADDRESS_COUNTRY).equalTo("ИНДИЯ(IN)"), "356").otherwise(null).as(ADDR_CNTRY_SK),
                col(ADDRESS_OKATO2_CODE).as(ADDR_OKATO2_SK),
                when(col(ADDRESS_SUBJECTCODE).equalTo("Агинский Бурятский АО"), "712")
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
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Забайкальский край"), "302")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Запорожская область"), "358")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ивановская область"), "362")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Иркутская область"), "368")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Кабардино-Балкарская Республика"), "125")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Калининградская область"), "374")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Калужская область"), "380")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Камчатский край"), "303")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Карачаево-Черкесская Республика"), "133")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Кемеровская область"), "392")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Кировская область"), "398")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Коми-Пермяцкий АО"), "718")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Корякский АО"), "724")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Костромская область"), "404")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Краснодарский край"), "410")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Красноярский край"), "416")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Курганская область"), "422")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Курская область"), "428")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ленинградская область"), "434")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Липецкая область"), "440")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Луганская Народная Республика"), "144")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Магаданская область"), "446")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("г.Москва"), "1")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Московская область"), "452")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Мурманская область"), "458")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ненецкий АО"), "730")
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
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Адыгея"), "101")
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
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Саха (Якутия)"), "181")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Северная Осетия - Алания"), "153")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Татарстан"), "157")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Тыва"), "161")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Республика Хакасия"), "169")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ростовская область"), "528")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Рязанская область"), "534")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Самарская область"), "540")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("г.Санкт-Петербург"), "2")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Саратовская область"), "546")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Сахалинская область"), "552")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Свердловская область"), "558")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("г.Севастополь"), "3")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Смоленская область"), "564")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ставропольский край"), "570")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Таймырский (Долгано - Ненецкий) АО"), "736")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Тамбовская область"), "576")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Тверская область"), "582")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Томская область"), "588")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Тульская область"), "594")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Тюменская область"), "600")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Удмуртская Республика"), "165")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ульяновская область"), "606")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Усть - Ордынский Бурятский АО"), "742")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Хабаровский край"), "612")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ханты-Мансийский АО - Югра"), "748")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Херсонская область"), "609")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Челябинская область"), "618")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Чеченская Республика"), "173")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Читинская область"), "624")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Чувашская Республика - Чувашия"), "177")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Чукотский АО"), "754")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Эвенкийский АО"), "760")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ямало-Ненецкий АО"), "766")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Ярославская область"), "630")
                        .when(col(ADDRESS_SUBJECTCODE).equalTo("Не субъект РФ"), "0")
                        .as(ADDR_RUS_FED_SUBJ_SK)
        ).distinct();

        Dataset<Row> divh = divh_raw.select(
                lit(getCtlValidFromStr()).as(CTL_VALIDFROM),
                col(DIV_SK).as(DIV_SK),
                col(DIV_H_NAME).as(DIV_H_NAME),
                col(DIV_CLOSEDATE).as(DIV_CLOSEDATE),
                col(DIV_OPENDATE).as(DIV_OPENDATE),
                col(DIV_CODE).as(DIV_CODE),
                lit(DATA_PRODUCT).as(DIV_DP_CODE),
                lit(RDM_IDL_SPARK).as(DIV_PAY_SYS_PRTCPNT_DP_CODE),
                col(DIV_PAY_SYS_PRTCPNT_SK).as(DIV_PAY_SYS_PRTCPNT_SK),
                lit(DATA_PRODUCT).as(DIV_FACT_ADDR_DP_CODE),

                when(col(ADDR_SRC_TXT).isNotNull(), AddrTransformer.getKey("|")).otherwise(null).as(DIV_FACT_ADDR_SK),

                //AddrTransformer.getKey("|").as(ColumnsName.DIV_FACT_ADDR_SK),

                lit(false).as(DIRTY_FLAG),
                lit(false).as(DELETED_FLAG)
        ).distinct();

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(DIV_PAY_SYS_PRTCPNT_SK, "pay_sys_prtcpnt");
        columnEntities.put(DIV_FACT_ADDR_SK, ADDR);

        divh = skClientWrapper.incrementSk(divh, DIVH, columnEntities, transformConfiguration);

        return divh;
    }
}
