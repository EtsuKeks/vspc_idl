package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.dffess.vspc.common.VspcLoadDate;
import ru.sberbank.bigdata.sl.rt.core.App;

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
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.EBX_REP_M_SFS_0002_SFSDIVISION;
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
        Dataset<Row> divtype = read(EBX_REP_M_SFS_0002_SFSDIVISION).kap803Usl().snp().get();

        divtype = divtype
                .select(
                        lit(false).as(DELETED_FLAG),

                        // *_sid — натуральный ключ из источника; addSk отдаёт его в ССК и заменяет на суррогатный *_sk подчищая *_sid после себя.
                        col(FULLDIVCODE).as(DIV_SID),
                        lit(SHORT_PRODUCT_CODE).as(DIV_DP_CODE),
                        col(DIVISIONTYPE00).as(DIV_TYPE_TERR_DIV_TYPE_SID),
                        lit(SHORT_PRODUCT_CODE).as(DIV_TYPE_TERR_DIV_TYPE_DP_CODE),
                        lit(false).as(DIRTY_FLAG)
                ).distinct();

        Dataset<Row> result = addSk(divtype, DIVTYPE, Arrays.asList("@div", "div_type@terr_div_type"));

        // Технические колонки (ctl_loading, ctl_validfrom и др.) — не наши: RT-FW выбрасывает их из входа и ставит сам.
        // Даты версии — наши: публикатор требует их во входе. Заполняем их так же, как загрузчик ESSA-версии витрины:
        // start_dt: INIT — 1900-01-01, INCR — день загрузки − 1; end_dt — 9999-12-31 в обоих режимах.
        result = result
                .withColumn(START_DT_FIELD_NAME, getApp().ctx().getLoadMode() == App.LoadMode.INCREMENT
                        ? date_sub(lit(VspcLoadDate.sqlDate()), 1)
                        : lit(MINUS_INFINITY_DT).cast("date"))
                .withColumn(END_DT_FIELD_NAME, lit(PLUS_INFINITY_DT).cast("date"));

        // Просто вызвать publish нельзя даже с пропатченным RT-FW: до его патча публикатор молча выбросит колонку с другим типом
        // и сразу выйдет на пустом инкременте. Поэтому сначала приводим и проверяем типы и корректно обрабатываем пустой INCR в своем собственном методе.
        // Там же вдобавок публикуем статистики в CTL: помечаем набор изменённым, и по этой пометке RT-FW их публикует.
        publishChecked(result);
    }
}
