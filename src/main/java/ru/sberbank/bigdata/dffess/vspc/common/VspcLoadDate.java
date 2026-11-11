package ru.sberbank.bigdata.dffess.vspc.common;

import java.time.LocalDate;

/** DV-05 (O/constant/VspcConstant.java:19-21): дата шага трансформации, как now() трансформера 3.0; одна на трансформацию и её публикацию. */
public final class VspcLoadDate {

    private static final ThreadLocal<LocalDate> DATE = ThreadLocal.withInitial(LocalDate::now);

    private VspcLoadDate() {
    }

    /** Дата как java.sql.Date: lit(...) даёт DateType — тип колонки не меняется. */
    public static java.sql.Date sqlDate() {
        return java.sql.Date.valueOf(DATE.get());
    }

    static void reset() {
        DATE.remove();
    }
}
