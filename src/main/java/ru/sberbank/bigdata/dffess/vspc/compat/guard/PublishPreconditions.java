package ru.sberbank.bigdata.dffess.vspc.compat.guard;

import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.*;
import scala.Option;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.util.RtfwLogger;

import java.util.*;

import static org.apache.spark.sql.functions.col;

// Проверки входа перед публикацией RT-FW.
public final class PublishPreconditions {

    private PublishPreconditions() {
    }

    // Тип колонки входа отличается от типа в snp — приводим с проверками ANSI, как insertInto в ESSA. Если так     
    // привести нельзя — отказ: ESSA на такой записи падала, а RT-FW записал бы NULL.
    public static Dataset<Row> alignTypes(Dataset<Row> ds, String snpTable) {
        SparkSession spark = SparkSession.active();
        spark.catalog().refreshTable(snpTable);
        StructType target = spark.table(snpTable).schema();

        List<String> names = Arrays.asList(target.fieldNames()); // колонки таблицы: [div_sk, ..., start_dt]
        List<Column> out = new ArrayList<>();  // колонки результата — по одной на каждую колонку входа
        List<String> cast = new ArrayList<>(); // что привели — для журнала
        List<String> bad = new ArrayList<>();  // что привести нельзя — для ошибки
        for (StructField f : ds.schema().fields()) {
            Column c = col(f.name());

            // Лишние колонки входа не трогаем, с ними дальше разберётся RT-FW.
            if (names.contains(f.name())) {
                DataType to = target.apply(f.name()).dataType();

                // Строка для журнала или ошибки: "div_sk: int -> bigint".
                String diff = f.name() + ": " + f.dataType().simpleString() + " -> " + to.simpleString();
                if (!to.simpleString().equalsIgnoreCase(f.dataType().simpleString())) {
                    if (Cast.canANSIStoreAssign(f.dataType(), to)) {
                        c = ansiCast(c, to, snpTable).as(f.name());
                        cast.add(diff);
                    } else {
                        bad.add(diff);
                    }
                }
            }
            out.add(c);
        }
        if (!bad.isEmpty()) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, 
                snpTable + ": типы колонок входа несовместимы с таргетом (ESSA падала бы на insertInto): " + bad));
        }
        if (cast.isEmpty()) {
            return ds;
        }
        RtfwLogger.log(new LogMessage("", LogType.TECH_INFO, snpTable + ": типы входа приведены к таргету: " + cast));
        return ds.select(out.toArray(new Column[0]));
    }

    // Приведение с проверками ANSI, как при insertInto в ESSA. Конструктор Cast в Spark разный: в версии spark 3.2
    // режим ANSI задаётся флагом (…, boolean ansiEnabled), в spark 3.4+ — значением EvalMode.ANSI. Берём тот, что есть.
    private static Column ansiCast(Column c, DataType to, String snpTable) {
        try {
            for (java.lang.reflect.Constructor<?> k : Cast.class.getConstructors()) {
                Class<?>[] p = k.getParameterTypes();
                if (p.length != 4) {
                    continue;
                }
                if (p[3] == boolean.class) {
                    return new Column((Expression) k.newInstance(c.expr(), to, Option.empty(), true));
                }
                if (p[3].getName().equals("scala.Enumeration$Value")) {
                    Object ansi = Class.forName("org.apache.spark.sql.catalyst.expressions.EvalMode")
                                       .getMethod("ANSI").invoke(null);
                    return new Column((Expression) k.newInstance(c.expr(), to, Option.empty(), ansi));
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, 
                snpTable + ": не удалось создать приведение типа с проверками ANSI: " + e));
        }
        throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, 
            snpTable + ": в этой версии Spark нет конструктора Cast с режимом ANSI"));
    }
}
