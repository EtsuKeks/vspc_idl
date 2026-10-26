package ru.sberbank.bigdata.dffess.vspc.common;

import java.time.LocalDate;

/**
 * Единый анкер «даты загрузки» на JVM: ОДНО значение на прогон для обеих половин стыка версий —
 * start_dt новых версий (DivH/AddrH/DivType/DivDivRelation) и end_dt закрытия версий/исчезнувших
 * ключей в shadow-калькуляторе (HistoricalPeriodWithRewriteChangesCalculator). Статика, потому что
 * один JVM = одна загрузка (значение фиксируется при инициализации класса). Дата берётся по
 * JVM-default TZ — паритет с ESSA 3.0 (VspcConstant.getCtlValidFromStr() = LocalDateTime.now()),
 * а не по spark.sql.session.timeZone, как Spark-функция current_date().
 */
public final class VspcLoadDate {

    /** Дата загрузки — фиксируется один раз при загрузке класса (на процесс/загрузку). */
    private static final LocalDate LOAD_DATE = LocalDate.now();

    private VspcLoadDate() {
    }

    /** Дата загрузки как java.sql.Date: lit(...) даёт DateType — тип колонки не меняется. */
    public static java.sql.Date sqlDate() {
        return java.sql.Date.valueOf(LOAD_DATE);
    }

    /** Дата загрузки в ISO-виде (yyyy-MM-dd) — для подстановки в SQL-строки. */
    public static String isoDate() {
        return LOAD_DATE.toString();
    }
}
