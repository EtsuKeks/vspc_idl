package ru.sberbank.bigdata.dffess.vspc.tables.addrh;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName;
import ru.sberbank.bigdata.dffess.vspc.constant.TablesName;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
import ru.sberbank.bigdata.essa.common.logger.LoggerWrapper;
import ru.sberbank.bigdata.essa.common.slave.app.SparkWrapper;
import ru.sberbank.bigdata.essa.common.slave.hdfs.HdfsService;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityProxyRelation;
import ru.sberbank.bigdata.essa.common.slave.templates.EntityTransformer;
import ru.sberbank.bigdata.essa.common.slave.templates.SkClientWrapper;
import ru.sberbank.bigdata.essa.common.slave.templates.TransformConfiguration;
import ru.sberbank.bigdata.essa.common.utils.EntityStaticService;
import ru.sberbank.bigdata.dffess.vspc.tables.addr.AddrTransformer;
import org.apache.spark.sql.Column;


import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.CTL_VALIDFROM;
import static org.apache.spark.sql.functions.*;

public class AddrHTransformer implements EntityTransformer {
    @Override
    public Dataset<Row> transformData(SparkWrapper sparkWrapper, SkClientWrapper skClientWrapper, HdfsService hdfsService,
                                      EntityStaticService entityStaticService, TransformConfiguration transformConfiguration,
                                      EntityProxyRelation entityProxyRelation, LoggerWrapper loggerWrapper) {

        Dataset<Row> addr_raw = sparkWrapper.getDataset(transformConfiguration.getSnpSrcSchema(TablesName.EBX_REP_M_SFS_0002_SFSDIVISION), TablesName.EBX_REP_M_SFS_0002_SFSDIVISION);
        
        Dataset<Row> addr_fact = addr_raw.select(
                        coalesce(col(ColumnsName.ADDRESS_ADDRFACT_POSTCODE), col(ColumnsName.ADDRESS_POSTCODE)).as(ColumnsName.ADDR_IDX_NUM),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDRAREA_AREANAME).as(ColumnsName.ADDR_DISTRICT_NAME),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDPLACE_PLACENAME).as(ColumnsName.ADDR_LOCTY_NAME),
                        col(ColumnsName.ADDRESS_SUBJECTCODE).as(ColumnsName.ADDR_REG_ADDR_NAME),
                        col(ColumnsName.ADDRESS_LOCATIONADDRESS).as(ColumnsName.ADDR_SRC_TXT),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDRCITY_CITYNAME).as(ColumnsName.ADDR_CITY_NAME),
                        col(ColumnsName.ADDRESS_ADDRFACT_FIASCODE).as(ColumnsName.ADDR_FIAS_CODE),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDRSTREET_HOUSENUMBERS).as(ColumnsName.ADDR_HOUSE_NUM),
                        col(ColumnsName.ADDRESS_ADDRFACT_ADDRSTREET_STREETNAME).as(ColumnsName.ADDR_STREET_NAME),
                        when(col(ColumnsName.ADDRESS_COUNTRY).equalTo("РОССИЯ(RU)"), "643").when(col(ColumnsName.ADDRESS_COUNTRY).equalTo("ИНДИЯ(IN)"), "356").otherwise(null).as(ColumnsName.ADDR_CNTRY_SK),
                        col(ColumnsName.ADDRESS_OKATO2_CODE).as(ColumnsName.ADDR_OKATO2_SK),
                        when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Агинский Бурятский АО"), "712")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Алтайский край"), "301")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Амурская область"), "308")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Архангельская область"), "314")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Астраханская область"), "320")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Белгородская область"), "326")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Брянская область"), "332")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Владимирская область"), "338")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Волгоградская область"), "344")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Вологодская область"), "350")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Воронежская область"), "356")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Донецкая Народная Республика"), "119")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Еврейская АО"), "701")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Забайкальский край"), "302")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Запорожская область"), "358")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Ивановская область"), "362")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Иркутская область"), "368")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Кабардино-Балкарская Республика"), "125")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Калининградская область"), "374")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Калужская область"), "380")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Камчатский край"), "303")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Карачаево-Черкесская Республика"), "133")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Кемеровская область"), "392")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Кировская область"), "398")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Коми-Пермяцкий АО"), "718")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Корякский АО"), "724")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Костромская область"), "404")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Краснодарский край"), "410")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Красноярский край"), "416")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Курганская область"), "422")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Курская область"), "428")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Ленинградская область"), "434")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Липецкая область"), "440")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Луганская Народная Республика"), "144")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Магаданская область"), "446")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("г.Москва"), "1")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Московская область"), "452")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Мурманская область"), "458")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Ненецкий АО"), "730")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Нижегородская область"), "464")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Новгородская область"), "470")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Новосибирская область"), "476")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Омская область"), "484")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Оренбургская область"), "490")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Орловская область"), "496")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Пензенская область"), "504")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Пермский край"), "417")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Приморский край"), "516")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Псковская область"), "522")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Адыгея"), "101")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Алтай"), "105")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Башкортостан"), "109")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Бурятия"), "113")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Дагестан"), "117")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Ингушетия"), "121")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Калмыкия"), "129")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Карелия"), "137")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Коми"), "141")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Крым"), "142")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Марий Эл"), "145")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Мордовия"), "149")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Саха (Якутия)"), "181")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Северная Осетия - Алания"), "153")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Татарстан"), "157")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Тыва"), "161")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Республика Хакасия"), "169")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Ростовская область"), "528")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Рязанская область"), "534")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Самарская область"), "540")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("г.Санкт-Петербург"), "2")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Саратовская область"), "546")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Сахалинская область"), "552")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Свердловская область"), "558")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("г.Севастополь"), "3")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Смоленская область"), "564")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Ставропольский край"), "570")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Таймырский (Долгано - Ненецкий) АО"), "736")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Тамбовская область"), "576")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Тверская область"), "582")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Томская область"), "588")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Тульская область"), "594")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Тюменская область"), "600")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Удмуртская Республика"), "165")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Ульяновская область"), "606")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Усть - Ордынский Бурятский АО"), "742")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Хабаровский край"), "612")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Ханты-Мансийский АО - Югра"), "748")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Херсонская область"), "609")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Челябинская область"), "618")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Чеченская Республика"), "173")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Читинская область"), "624")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Чувашская Республика - Чувашия"), "177")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Чукотский АО"), "754")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Эвенкийский АО"), "760")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Ямало-Ненецкий АО"), "766")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Ярославская область"), "630")
                                .when(col(ColumnsName.ADDRESS_SUBJECTCODE).equalTo("Не субъект РФ"), "0")
                                .as(ColumnsName.ADDR_RUS_FED_SUBJ_SK)
                ).where(col(ColumnsName.ADDRESS_LOCATIONADDRESS).isNotNull());

        Dataset<Row> addr_legal = addr_raw.select(
                        lit(null).as(ColumnsName.ADDR_IDX_NUM),
                        lit(null).as(ColumnsName.ADDR_DISTRICT_NAME),
                        lit(null).as(ColumnsName.ADDR_LOCTY_NAME),
                        lit(null).as(ColumnsName.ADDR_REG_ADDR_NAME),
                        col(ColumnsName.ADDRESS_LEGAL_LOCATIONADDRESS).as(ColumnsName.ADDR_SRC_TXT),
                        lit(null).as(ColumnsName.ADDR_CITY_NAME),
                        lit(null).as(ColumnsName.ADDR_FIAS_CODE),
                        lit(null).as(ColumnsName.ADDR_HOUSE_NUM),
                        lit(null).as(ColumnsName.ADDR_STREET_NAME),
                        lit(null).as(ColumnsName.ADDR_CNTRY_SK),
                        lit(null).as(ColumnsName.ADDR_OKATO2_SK),
                        lit(null).as(ColumnsName.ADDR_RUS_FED_SUBJ_SK)
                ).where(col(ColumnsName.ADDRESS_LEGAL_LOCATIONADDRESS).isNotNull());
        
        Dataset<Row> addr = addr_fact.union(addr_legal).distinct();
        
        Dataset<Row> addrh = addr.select(
                lit(VspcConstant.getCtlValidFromStr()).as(CTL_VALIDFROM),
                AddrTransformer.getKey("|").as(ColumnsName.ADDR_SK),
                AddrTransformer.getKey("|").as(ColumnsName.SID),
                lit(VspcConstant.DATA_PRODUCT).as(ColumnsName.ADDR_DP_CODE),
                col(ColumnsName.ADDR_SRC_TXT).as(ColumnsName.ADDR_STANDART_ADDRESS_TXT),
                lit(false).as(ColumnsName.DIRTY_FLAG),
                lit(false).as(ColumnsName.DELETED_FLAG)
        ).distinct();

        addrh = skClientWrapper.incrementSk(addrh, TablesName.ADDRH, transformConfiguration);

        return addrh;
    }
}
