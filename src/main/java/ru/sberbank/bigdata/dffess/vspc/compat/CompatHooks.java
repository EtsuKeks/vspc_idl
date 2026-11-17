package ru.sberbank.bigdata.dffess.vspc.compat;

import org.apache.spark.sql.*;

import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables.Table;
import ru.sberbank.bigdata.dffess.vspc.compat.hub.HubCompatCalculator;
import ru.sberbank.bigdata.dffess.vspc.compat.merge.MergeCompatCalculator;
import ru.sberbank.bigdata.dffess.vspc.compat.period.PeriodRewriteCompatCalculator;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.*;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.changes.*;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.merge.*;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;

import java.lang.reflect.Field;
import java.util.*;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatTables.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.*;

// Методы, которые вызывают вставки COMPAT в нашем теневом ProcessingStage RT-FW.
public final class CompatHooks {

    // Обёрнутый калькулятор изменений каждой стадии — для её merge: runIncr вызывает оба у одного ProcessingStage,
    // а обёртке merge нужно то, что посчитал калькулятор изменений.
    private static final Map<Object, ChangesCalculator> BY_STAGE = Collections.synchronizedMap(new WeakHashMap<>());

    private CompatHooks() {
    }

    // INIT для HistMix: ESSA-версия витрины ставила ctl_validfrom = start_dt.
    public static Dataset<Row> enrichInit(Dataset<Row> dataset, TableInfoFactory tif) {
        Table t = byCode(tif.createTableInfo(SchemaName.SNP).getTableName());
        return t.initVfFromStart() ? dataset.withColumn(CTL_VALIDFROM, col(START_DT_FIELD_NAME).cast(TimestampType))
                                   : dataset;
    }

    // Подменяем калькулятор изменений RT-FW нашей обёрткой. ESSA-поправки написаны под конкретные классы RT-FW и режим
    // INCR: другой класс или включённый режим вне этого пути — ошибка, а не публикация без поправок.
    public static ChangesCalculator wrapChanges(Object stage, ChangesCalculator cc, PublisherDao dao, TableInfoFactory tif) {
        Table t = byCode(tif.createTableInfo(SchemaName.SNP).getTableName());
        ChangesCalculator wrapped;
        if (cc != null && cc.getClass() == HistoricalPeriodWithRewriteChangesCalculator.class && t.isPeriod()) {
            // Режимы калькулятора, которые меняют расчёт изменений, должны быть выключены: поправки ESSA написаны и
            // проверены только для обычного INCR. fullSlice — режим WITH_REWRITE_FULL_SLICE (закрывает все ключи,
            // которых нет во входе), reInit — пересборка на INIT при init_diff_flag, collapsePeriods — склейка
            // одинаковых версий.
            // Родитель — package-private AbstractHistoricalWithRewriteChangesCalculator, напрямую на него не сослаться.
            Class<?> rewriteBase = cc.getClass().getSuperclass();
            requireFalse(cc, rewriteBase, "fullSlice");
            requireFalse(cc, rewriteBase, "reInit");
            requireFalse(cc, cc.getClass(), "collapsePeriods");
            // Проверяем что диапазон партиций — по умолчанию.
            requireValue(cc, cc.getClass(), "sectioningDMin", MINUS_INFINITY_DT);
            requireValue(cc, cc.getClass(), "sectioningDMax", PLUS_INFINITY_DT);
            wrapped = new PeriodRewriteCompatCalculator(cc, t, loadingId(cc), dao, tif);
        } else if (cc != null && cc.getClass() == NonHistoricalChangesCalculator.class && !t.isPeriod()) {
            requireFalse(cc, cc.getClass(), "fullSlice");
            requireFalse(cc, cc.getClass(), "physicalDeletion");
            wrapped = new HubCompatCalculator(cc, t, loadingId(cc), dao, tif);
        } else {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, 
                "Калькулятор изменений " + (cc == null ? "null" : cc.getClass().getName()) + " для " + t.code()
                + " (" + t.kind() + "): поправки ESSA для него не объявлены"));
        }
        BY_STAGE.put(stage, wrapped);
        return wrapped;
    }

    private static int loadingId(ChangesCalculator cc) {
        return (Integer) readField(cc, AbstractChangesCalculator.class, "loadingId");
    }

    // Флаг режима в калькуляторе RT-FW должен быть выключен, иначе — ошибка.
    private static void requireFalse(Object target, Class<?> declaring, String field) {
        if (!Boolean.FALSE.equals(readField(target, declaring, field))) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                "Калькулятор " + target.getClass().getSimpleName() + ": " + field
                + " != false — режим вне INCR merge-пути, ESSA-поправки не применимы"));
        }
    }

    // Настройка калькулятора RT-FW должна иметь значение по умолчанию, иначе — ошибка.
    private static void requireValue(Object target, Class<?> declaring, String field, Object expected) {
        Object actual = readField(target, declaring, field);
        if (!expected.equals(actual)) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                "Калькулятор " + target.getClass().getSimpleName() + ": " + field + " = " + actual + ", ожидалось "
                + expected + " — ESSA-поправки для такой настройки не применимы"));
        }
    }

    // Читаем приватное поле класса RT-FW.
    private static Object readField(Object target, Class<?> declaring, String name) {
        try {
            Field f = declaring.getDeclaredField(name);
            f.setAccessible(true);
            return f.get(target);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    // Подменяем merge RT-FW обёрткой и передаём ей то, что посчитал калькулятор изменений той же стадии.
    public static MergeCalculator wrapMerge(Object stage, MergeCalculator mc) {
        ChangesCalculator cc = BY_STAGE.remove(stage);
        if (cc instanceof HubCompatCalculator) {
            HubCompatCalculator hub = (HubCompatCalculator) cc;
            return new MergeCompatCalculator(mc, hub.table(), hub.winKeys(), hub.winSnp());
        }
        PeriodRewriteCompatCalculator period = (PeriodRewriteCompatCalculator) cc;
        return new MergeCompatCalculator(mc, period.table(), period.winKeys(), period.winSnp());
    }
}
