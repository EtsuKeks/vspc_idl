package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.date_sub;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.xxhash64;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIRTY_FLAG;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIVISIONTYPE00;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_SID;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_SK;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_TYPE_TERR_DIV_TYPE_DP_CODE;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_TYPE_TERR_DIV_TYPE_SID;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.DIV_TYPE_TERR_DIV_TYPE_SK;
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

        Map<String, String> columnEntities = new HashMap<>();
        columnEntities.put(DIV_TYPE_TERR_DIV_TYPE_SID, TERRDIVTYPE);
        columnEntities.put(DIV_SID, DIV);

        Dataset<Row> result = addSk(divtype, DIVTYPE,  columnEntities);

        // Правило дат воспроизводит семантику ESSA DataLoaderHistSnp для классической hub-истории
        // div_type. Новизна ключа определяется ПОСЛЕ генерации СК по div_sk: в SNP есть только *_sk,
        // а *_sid на этом этапе уже дропнуты. Инициализация: start_dt = 1900-01-01.
        // Инкремент — три состояния ключа (аналог LoaderCache из DataLoaderHistSnp):
        //   1) есть открытая версия (end_dt = 9999-12-31)            -> ctl_validfrom - 1;
        //   2) открытой нет, есть закрытые (переоткрытие, IS_CACHED) -> max(end_dt закрытых) + 1;
        //   3) ключа в SNP нет                                       -> 1900-01-01.
        // Дополнительно эмитируются закрывающие строки для исчезнувших из реплики типов: открытая
        // версия SNP получает end_dt = current_date - 1 (ESSA OLD_CONDITION, N1 = 1), а не остаётся
        // открытой навсегда. Publisher смёржит их как UPDATE по PK [div_sk, start_dt] (end_dt входит
        // в хэш изменений). Механический mergeMode(WITH_REWRITE_FULL_SLICE) не используется: его
        // calculateSliceDate = min(start_dt) инкремента некорректен при смешанных стартах (новые
        // ключи со стартом 1900-01-01).
        if (isIncrement()) {
            Column plusInfinityDate = lit(PLUS_INFINITY_DT).cast("date");
            Column openVersion = col(END_DT_FIELD_NAME).equalTo(plusInfinityDate);
            Dataset<Row> snpDivtype = readTable(SNP_SCHEMA, DIVTYPE);

            // Отсев неизменившихся бизнес-ключей (аналог ESSA EQUALS): hash по бизнес-атрибутам БЕЗ
            // start_dt/end_dt/p1month/ctl_* — так же, как ESSA MergeService.getStringHash (exclude =
            // START_DT/END_DT/PARTITION_COLUMN_MONTH). Ключ (div ↔ тип), у которого есть открытая
            // SNP-версия с теми же бизнес-атрибутами, не попадает в publisher и не создаёт лишнюю
            // версию (в 3.0 он давал EQUALS и версия не пересоздавалась).
            Dataset<Row> openSnpHash = snpDivtype.where(openVersion)
                    .select(
                            col(DIV_SK).as("_etl_snp_div_sk"),
                            xxhash64(concat(
                                    when(col(DIV_SK).isNotNull(), col(DIV_SK).cast("string")).otherwise(lit("")),
                                    when(col(DIV_TYPE_TERR_DIV_TYPE_SK).isNotNull(), col(DIV_TYPE_TERR_DIV_TYPE_SK).cast("string")).otherwise(lit("")),
                                    when(col(DIV_TYPE_TERR_DIV_TYPE_DP_CODE).isNotNull(), col(DIV_TYPE_TERR_DIV_TYPE_DP_CODE).cast("string")).otherwise(lit("")),
                                    when(col(DIV_DP_CODE).isNotNull(), col(DIV_DP_CODE).cast("string")).otherwise(lit(""))
                            )).as("_etl_snp_hash"))
                    .distinct();

            // Полный набор входящих div_sk ДО hash-отсева (DEFECT C): closingRows строится по нему,
            // а не по result после отсева, иначе неизменившиеся ключи (отсеянные hash'ом) ложно
            // закрывались бы как «исчезнувшие из реплики» и выпадали из активного среза.
            Dataset<Row> incomingDivSk = result.select(col(DIV_SK).as("_etl_inc_div_sk"));

            result = result
                    .withColumn("_etl_inc_hash", xxhash64(concat(
                            when(col(DIV_SK).isNotNull(), col(DIV_SK).cast("string")).otherwise(lit("")),
                            when(col(DIV_TYPE_TERR_DIV_TYPE_SK).isNotNull(), col(DIV_TYPE_TERR_DIV_TYPE_SK).cast("string")).otherwise(lit("")),
                            when(col(DIV_TYPE_TERR_DIV_TYPE_DP_CODE).isNotNull(), col(DIV_TYPE_TERR_DIV_TYPE_DP_CODE).cast("string")).otherwise(lit("")),
                            when(col(DIV_DP_CODE).isNotNull(), col(DIV_DP_CODE).cast("string")).otherwise(lit(""))
                    )))
                    .join(openSnpHash,
                            col(DIV_SK).equalTo(col("_etl_snp_div_sk")).and(col("_etl_inc_hash").equalTo(col("_etl_snp_hash"))),
                            "left_anti")
                    .drop("_etl_inc_hash");

            // Состояние ключей SNP: есть ли открытая версия и когда ключ последний раз закрывался
            Dataset<Row> snpKeyState = snpDivtype.groupBy(col(DIV_SK).as("_etl_state_div_sk")).agg(
                    max(when(openVersion, lit(1)).otherwise(lit(0))).as("_etl_has_open"),
                    max(when(col(END_DT_FIELD_NAME).notEqual(plusInfinityDate), col(END_DT_FIELD_NAME)))
                            .as("_etl_closed_max_end"));

            result = result.join(snpKeyState, result.col(DIV_SK).equalTo(col("_etl_state_div_sk")), "left")
                    .withColumn(START_DT_FIELD_NAME,
                            when(col("_etl_has_open").equalTo(lit(1)),
                                    date_sub(lit(VspcConstant.getCtlValidFromStr()).cast("date"), 1))
                                    .when(col("_etl_closed_max_end").isNotNull(),
                                            date_add(col("_etl_closed_max_end"), 1))
                                    .otherwise(lit(VspcConstant.MIN_BUSINESS_DT))
                                    .cast("date"))
                    .drop("_etl_state_div_sk", "_etl_has_open", "_etl_closed_max_end");

            // Закрывающие строки: открытые версии SNP, чьи ключи не пришли в текущем инкременте
            Dataset<Row> openVersions = snpDivtype.where(openVersion);
            Dataset<Row> closingRows = openVersions
                    .join(incomingDivSk,
                            openVersions.col(DIV_SK).equalTo(col("_etl_inc_div_sk")), "left_anti")
                    .withColumn(END_DT_FIELD_NAME, date_sub(current_date(), 1));

            Column[] resultColumns = Arrays.stream(result.columns())
                    .map(closingRows::col)
                    .toArray(Column[]::new);
            result = result.union(closingRows.select(resultColumns));
        } else {
            result = result.withColumn(START_DT_FIELD_NAME, lit(VspcConstant.MIN_BUSINESS_DT).cast("date"));
        }

        publish(result)
                .recordsetMasks(sets)
                .launch();
    }

    @Override
    protected boolean skInitOnLoad() {
        // ESSA DataLoaderHistSnp для этой таблицы вызывал incrementSk (map) безусловно, включая INIT
        return false;
    }

    public static void main(String[] args) {
        runApp(args, DivTypeTransformation::new);
    }
}
