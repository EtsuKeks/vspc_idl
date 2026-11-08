package ru.sberbank.bigdata.dffess.vspc.common;

import ru.sberbank.bigdata.sl.rt.publisher.CalculationMode;

import static ru.sberbank.bigdata.sl.rt.publisher.CalculationMode.Approach.SPARK_REBALANCE;
import static ru.sberbank.bigdata.sl.rt.publisher.CalculationModes.HARD;

/**
 * Режим ребаланса партиций, в котором издатель RT-FW публикует все целевые таблицы витрины.
 *
 * <p>Что меняется при SPARK_REBALANCE (всё по коду издателя, vendor/rt-fw-publisher-2.16.1-src):
 * <ul>
 *   <li>издатель перестаёт материализовать датасет через {@code persist()+count()} перед записью —
 *       такая материализация в издателе одна, {@code PublisherDao.java:208-212} (ребаланс по SALTING);</li>
 *   <li>издатель перестаёт приземлять датасет в STGTMP и читать его обратно перед финальным
 *       репартиционированием ({@code PublisherDao.java:411-414}); то же приземление в STGTMP делают
 *       калькуляторы слияния и расчёта изменений ({@code GeneralMergeCalculator.java:114-118},
 *       {@code AbstractChangesCalculator.java:104-108});</li>
 *   <li>число и размер выходных файлов определяет не издатель, а AQE: издатель ставит hint
 *       {@code rebalance} и переносит партиционные колонки в конец проекта
 *       ({@code PublisherDao.java:288-304, 315-331}) — иначе OptimizeSkewInRebalancePartitions не срабатывает.</li>
 * </ul>
 *
 * <p>Почему режим задан здесь, а не в каждой трансформации: подход читается в одном месте —
 * {@code PublisherDao.rebalanceTable} (PublisherDao.java:406-409) — и условия там жёсткие:
 * {@code hasApproach(SPARK_REBALANCE)} И {@code spark.sql.adaptive.enabled=true} И
 * {@code spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled=true}. Обе AQE-настройки в
 * Spark 3.5.1 уже имеют значение true по умолчанию (SQLConf: ADAPTIVE_EXECUTION_ENABLED и
 * ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED созданы с default=true), но они
 * переопределяются окружением стенда (spark-defaults.conf), поэтому в {@code vars_all.yml} они
 * заданы явно. Если окружение выставит их в false, издатель молча уйдёт в SALTING — это видно
 * только по логу PUB_054: «используется Spark hint 'rebalance'» ({@code PublisherDao.java:316})
 * вместо «алгоритм SALTING» (текст параметров — {@code PublisherDao.java:220-222}, вывод в журнал —
 * {@code PublisherDao.java:240}).
 *
 * <p>На данные режим не влияет: {@code CalculationMode} не участвует в расчёте изменений, merge и
 * истории (в strategy/process/** нет ни одного обращения к CalculationMode) — меняется только
 * физическая раскладка данных по файлам внутри целевых партиций. Сверка 3.0↔4.0 после
 * переключения — по содержимому строк, а не по набору файлов.
 *
 * <p>Ограничение: черновики витрины (BaseTransformation.readFrozenDraft → saveTable →
 * saveToExternalTemporaryTable) издатель обслуживает своим DAO с режимом по умолчанию
 * (PublisherImpl.java:470-481, конструктор PublisherDao без режима — PublisherDao.java:105-107),
 * поэтому они остаются на SALTING и сохраняют persist()+count(). Целевые таблицы (снап и дифф)
 * публикуются этим DAO — PublisherImpl.java:216 берёт режим из билдера.
 */
public final class PublisherMode {

    private PublisherMode() {
    }

    /**
     * Режим публикации, который выставляется во все публикации витрины (снап и дифф,
     * INIT/INCR/REINIT) через {@code Publisher.Builder.calculationMode(...)}.
     *
     * <p>Новый экземпляр на каждый вызов: {@code CalculationMode.withApproach} меняет сам объект
     * (rt-fw-core 3.16.0, {@code CalculationMode.java:49-52}), поэтому общий изменяемый экземпляр
     * в статическом поле небезопасен.
     */
    public static CalculationMode sparkRebalance() {
        return new CalculationMode(HARD).withApproach(SPARK_REBALANCE);
    }
}
