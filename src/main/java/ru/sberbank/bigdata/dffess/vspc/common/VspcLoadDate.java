package ru.sberbank.bigdata.dffess.vspc.common;

import java.time.LocalDate;

/**
 * Дата загрузки шага. В ESSA-версии витрины у шага была одна дата на всё — и на расчёт, и на публикацию; у нас так же.
 * Шаг ставит её при старте и дальше пользуется только ею. Если шаг упал, его дата больше никому не нужна:
 * следующий шаг при старте поставит свою.
 */
public final class VspcLoadDate {

    private static final ThreadLocal<LocalDate> DATE = new ThreadLocal<>();

    private VspcLoadDate() {
    }

    /** Первая строка run() трансформации, которой нужна дата загрузки. */
    public static void start() {
        DATE.set(LocalDate.now());
    }

    /** Дата как java.sql.Date: lit(...) даёт DateType — тип колонки не меняется. */
    public static java.sql.Date sqlDate() {
        LocalDate date = DATE.get();
        if (date == null) {
            throw new IllegalStateException("Дата загрузки не задана: VspcLoadDate.start() должен быть первой строкой run() трансформации");
        }
        return java.sql.Date.valueOf(date);
    }
}
