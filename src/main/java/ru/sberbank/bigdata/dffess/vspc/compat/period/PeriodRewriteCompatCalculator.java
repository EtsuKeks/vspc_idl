package ru.sberbank.bigdata.dffess.vspc.compat.period;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.dffess.vspc.common.LoadDate;
import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables;
import ru.sberbank.bigdata.sl.rt.publisher.dto.*;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.changes.ChangesCalculator;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.util.RtfwLogger;

import java.time.LocalDate;
import java.util.*;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.*;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.TECH_FIELDS;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.*;

// Обёртка над калькулятором изменений RT-FW HistoricalPeriodWithRewriteChangesCalculator.
public final class PeriodRewriteCompatCalculator implements ChangesCalculator {

    private static final String OPEN_FLG = "_compat_open";
    private static final String CLOSED_END = "_compat_closed_end";

    private final ChangesCalculator delegate;
    private final CompatTables.Table table;
    private final String partitionColumn;
    private Map<String, String> zone;

    // Состояние одного calculate (стадия вызывает его один раз): открытые версии (с колонкой хэша строки), последние
    // закрытия ключей таблицы с загрузчиком HistSnp.
    private Dataset<Row> openH;
    private Dataset<Row> closedEnd;

    public PeriodRewriteCompatCalculator(ChangesCalculator delegate, CompatTables.Table table) {
        this.delegate = delegate;
        this.table = table;
        this.partitionColumn = table.partitionColumn();
    }

    // Зона перезаписи, как у мувера ESSA-версии: p1month >= месяц (сегодня минус 1 месяц). Одно значение и для чтения
    // снапа, и для переноса — считаем один раз и запоминаем.
    @Override
    public Map<String, String> getMinBusinessDateUpdateFilters() {
        if (zone == null) {
            // Порог от даты загрузки шага, как мувер ESSA-версии считал от текущей даты.
            LocalDate threshold = LoadDate.sqlDate().toLocalDate().minusMonths(1).withDayOfMonth(1);
            String value = table.partitionOf(threshold.toString());
            zone = Collections.singletonMap(partitionColumn, value);
            RtfwLogger.log(new LogMessage("", LogType.TECH_INFO,
                table.code() + ": зона перезаписи " + partitionColumn + " >= " + value));
        }
        return zone;
    }

    @Override
    public Column getMinBusinessDateUpdateFilter() {
        return col(partitionColumn).geq(lit(getMinBusinessDateUpdateFilters().get(partitionColumn)));
    }

    @Override
    public SnapshotChanges calculate(Dataset<Row> incr, Dataset<Row> snp, List<Map<String, String>> specs) {
        List<String> bk = table.entityKey();

        // Правило повторного открытия загрузчика HistSnp: ключ без открытой версии открывается днём после своего
        // последнего закрытия. Дату берём из кэша закрытий, как ESSA. Кэш в stg переписывается только после публикации
        // — здесь он ещё прежний. Нет кэша или не HistSnp — null.
        Dataset<Row> cache = table.histSnp() ? LoaderCache.read(table) : null;
        closedEnd = cache == null ? null : cache.withColumnRenamed(END_DT_FIELD_NAME, CLOSED_END);

        // (1) ESSA сравнивает вход только с открытыми версиями (p1month = '999912'), закрытые секции не трогает. У
        // ключа одна открытая версия: дубликаты, оставленные ESSA-версией, сводит onePerKey. За аргументами
        // Collections.emptyList() и desc(CTL_LOADING) см. комментарий к HubCompatCalculator.calculate — ситуация та
        // же. Группируем по ключу сущности без start_dt, хотя таблица историческая: берём только открытую партицию, а
        // в ней у сущности должна быть ровно одна текущая версия — несколько открытых строк с любыми start_dt это
        // мусор от ESSA-версии, а не разные версии.
        // В ту же таблицу кладём колонку ROW_HASH — хэш строки для шага (2); в RT-FW и в merge она не должна попасть,
        // поэтому дальше работаем с open без неё (в MergeCompatCalculator — тоже drop).
        Column h = rowHash(incr, bk);
        openH = writeWorkTable(table.tmpTable(OPEN_SNP),
            onePerKey(snp.filter(col(partitionColumn).equalTo(lit(table.openPartition()))), table.entityKey(),
                      Collections.emptyList(), desc(CTL_LOADING)).withColumn(ROW_HASH, h));
        Dataset<Row> open = openH.drop(ROW_HASH);

        // (2) Строка входа, у которой хэш строки совпадает с открытой версией того же ключа, ничего не меняет и строк
        // diff не даёт — как в ESSA. Строку входа с end_dt не 9999-12-31 — закрытую копию, которую трансформация
        // таблицы с загрузчиком HistSnp кладёт во вход для ключа, пропавшего из источника (на примере vspc_idl:
        // DivTypeTransformation для div_type), — правило не трогает: хэш без дат у неё тот же, но это закрытие версии.
        // Такие строки как раз-таки надо оставить в inc (чтобы калькулятор RT-FW их учел в (3)), а вот строки которые
        // уже лежали открытыми в snp с тем же содержимым - убрать из входа (чтобы калькулятор RT-FW в (3) на них не
        // триггерился).
        // Результат пишем в stgtmp, потому что дальше его читают пять раз: калькулятор RT-FW четырежды и
        // LoaderCache.stage. Без записи каждое чтение заново считало бы хэш всего входа и соединение с открытыми
        // версиями; с записью это делается один раз.
        Column openRow = col(END_DT_FIELD_NAME).equalTo(lit(PLUS_INFINITY_DT).cast(DataTypes.DateType));
        List<String> keyH = new ArrayList<>(bk);
        keyH.add(ROW_HASH);
        Dataset<Row> inc = writeWorkTable(table.tmpTable("_compat_inc"),
            incr.filter(openRow).withColumn(ROW_HASH, h)
                .join(openH.select(cols(keyH)), scalaSeq(keyH), "left_anti")

                // join по списку ключей переносит ключи в начало — возвращаем колонки входа в исходном порядке
                // (заодно уходит ROW_HASH)
                .select(cols(Arrays.asList(incr.columns())))
                .unionByName(incr.filter(not(openRow))));

        // Закрытия пропавших ключей (строки входа не 9999-12-31) — в черновик кэша закрытий HistSnp. Залётных закрытых
        // строк во входе на INCR не бывает: только те, что трансформация намеренно закрыла (end_dt = вчера), остальным
        // строкам все трансформации ставят 9999-12-31. Поэтому отдаём их кэшу без проверок: «запомни про эти ключи дату
        // закрытия» — при возвращении ключ откроется следующим днём.
        if (table.histSnp()) {
            LoaderCache.stage(table, inc.filter(not(openRow)));
        }

        // (3) Остальное считает калькулятор RT-FW как есть. Отдаём ему open, а не весь snp, чтобы он сравнивал наш
        // подготовленный вход только с открытыми версиями и тем самым повторял поведение ESSA.
        SnapshotChanges ch = delegate.calculate(inc, open, specs);

        // (4) Вставкам ключей без открытой версии меняем start_dt, как в ESSA, — это делает метод newKeyStart.
        return new SnapshotChanges(newKeyStart(ch.getInserts()), ch.getUpdates(), ch.getDeletes());
    }

    // Хэш строки для шага (2): md5 склейки через '|' колонок входа без ключа сущности, дат версии, технических полей и
    // партиций, NULL -> ''.
    private Column rowHash(Dataset<Row> incr, List<String> bk) {
        Column[] attrs = Arrays.stream(incr.columns())
            .filter(c -> !bk.contains(c) && !START_DT_FIELD_NAME.equals(c) && !END_DT_FIELD_NAME.equals(c)
                         && !TECH_FIELDS.contains(c) && !partitionColumn.equals(c))
            .map(c -> when(col(c).isNull(), lit("")).otherwise(col(c).cast(DataTypes.StringType)))
            .toArray(Column[]::new);
        return md5(concat_ws("|", attrs));
    }

    // (4) Вставка ключа без открытой версии: start_dt = 1900-01-01, а у таблицы с загрузчиком HistSnp, если у ключа
    // есть закрытые версии, — день после последнего закрытия (как в ESSA-версии).
    private Dataset<Row> newKeyStart(Dataset<Row> ins) {
        List<String> bk = table.entityKey();
        Dataset<Row> openKeys = openH.select(cols(bk)).withColumn(OPEN_FLG, lit(true));
        Dataset<Row> joined = ins.join(openKeys, scalaSeq(bk), "left");

        // К строкам подтягиваем последнюю дату закрытия ключа; у ключа без закрытых версий и у таблиц без кэша
        // закрытий — null.
        joined = closedEnd == null ? joined.withColumn(CLOSED_END, lit(null).cast(DataTypes.DateType))
                                   : joined.join(closedEnd, scalaSeq(bk), "left");

        // start_dt ключа без открытой версии: день после последнего закрытия, иначе 1900-01-01. Если открытая версия
        // есть - то это может значить только то, что сама строка с данным ключом поменялась, но тогда мы полностью
        // принимаем start_dt положенный калькулятором RT-FW - нас устраивает, ведь в ESSA-версии было так же.
        Column newStart = coalesce(date_add(col(CLOSED_END), 1), lit(MINUS_INFINITY_DT).cast(DataTypes.DateType));
        return joined
            .withColumn(START_DT_FIELD_NAME, 
                        when(col(OPEN_FLG).isNull(), newStart).otherwise(col(START_DT_FIELD_NAME)))
            .select(cols(Arrays.asList(ins.columns())));
    }
}
