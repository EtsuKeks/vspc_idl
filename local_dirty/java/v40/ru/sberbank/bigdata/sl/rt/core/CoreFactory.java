package ru.sberbank.bigdata.sl.rt.core;

import org.apache.spark.sql.SparkSession;

import java.util.Map;

/** Local stand: real CoreImpl, but no Kafka logger and the pre-built local SparkSession. */
public class CoreFactory extends Factory<Core> {
    @Override
    public Core create(Transformation transformation) {
        return new CoreImpl() {
            @Override
            public void initializeLoggerFactory(AppContext ctx) {
            }

            @Override
            public void initializeMetricsFactory(AppContext ctx) {
            }

            @Override
            public SparkSession initializeSparkSession(Map<String, String> sparkArgs, String code) {
                return SparkSession.active();
            }
        };
    }

    @Override
    void register() {
        App.registerFactory(Core.class, new CoreFactory());
    }
}
