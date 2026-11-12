package ru.sberbank.bigdata.dffess.vspc.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.dffess.vspc.common.BaseTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.dffess.vspc.constant.ColumnsName.*;
import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.TERRDIVTYPE;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.SRC_PROVIDER_SFSDIVISION;
import static ru.sberbank.bigdata.sl.rt.core.Constants.DELETED_FLAG;

public class TerrDivTypeTransformation extends BaseTransformation {

    public static final String CODE = TERRDIVTYPE;

    public TerrDivTypeTransformation(App app) {
        super(app, CODE, "usl_vspc_idl.terr_div_type.hub");
    }

    @Override
    public void run() {
        Dataset<Row> terrdivtype = read(SRC_PROVIDER_SFSDIVISION).kap803Usl().snp().get();
        terrdivtype = terrdivtype
                .select(
                        lit(false).as(DELETED_FLAG),
                        col(DIVISIONTYPE00).as(SID),
                        lit(DATA_PRODUCT).as(TERR_DIV_TYPE_DP_CODE),
                        col(DIVISIONTYPE00).as(DIV_TYPE_CODE),

                        // *_sid — натуральный ключ из источника; addSk отдаёт его в ССК и заменяет на суррогатный *_sk подчищая *_sid после себя.
                        col(DIVISIONTYPE00).as(TERR_DIV_TYPE_SID),

                        lit(false).as(DIRTY_FLAG)
                ).distinct();

        Dataset<Row> result = addSk(terrdivtype, TERRDIVTYPE, Arrays.asList("@terr_div_type"));

        // Просто вызвать publish нельзя даже с пропатченным RT-FW: до его патча публикатор молча выбросит колонку с другим типом
        // и сразу выйдет на пустом инкременте. Поэтому сначала приводим и проверяем типы и корректно обрабатываем пустой INCR в своем собственном методе.
        // Там же вдобавок публикуем статистики в CTL: помечаем набор изменённым, и по этой пометке RT-FW их публикует.
        publishChecked(result);
    }
}
