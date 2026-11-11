package ru.sberbank.bigdata.dffess.vspc.common;

import ru.sberbank.bigdata.sl.rt.publisher.CalculationMode;

import static ru.sberbank.bigdata.sl.rt.publisher.CalculationMode.Approach.SPARK_REBALANCE;
import static ru.sberbank.bigdata.sl.rt.publisher.CalculationModes.HARD;

/** DV-45: SPARK_REBALANCE вместо SALTING (persist+count) для публикаций и черновиков — раскладка файлов, не данные; нужен spark.sql.adaptive.* из vars_all.yml, иначе SALTING (PUB_054). */
public final class PublisherMode {

    private PublisherMode() {
    }

    /** Новый экземпляр на вызов: CalculationMode.withApproach меняет сам объект. */
    public static CalculationMode sparkRebalance() {
        return new CalculationMode(HARD).withApproach(SPARK_REBALANCE);
    }
}
