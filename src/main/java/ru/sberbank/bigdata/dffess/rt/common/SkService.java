package ru.sberbank.bigdata.dffess.rt.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.sl.sk.client.SkClient;
import ru.sberbank.bigdata.sl.sk.client.SkClientException;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class SkService {
    public static final String SOURCE_SK_SUFFIX = "_sid";
    public static final String TARGET_SK_SUFFIX = "_sk";
    private SkClient skClient;

    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Обращение к сервису СК для генерации суррогатных ключей
     *
     * @param skEnabled SK включен
     * @param tableName физическое название таблицы, для которой запрашивается генерация
     * @param df        набор данных
     * @return набор данных с заполненными суррогатными ключами
     */
    public Dataset<Row> runSkGen(boolean skEnabled,
                                 String tableName,
                                 Dataset<Row> df,
                                 List<String> attributeCodes) throws SkClientException {

        Dataset<Row> resDataSet = df;

        // Пропускаем генерацию SK, если SK_ENABLED в значении false и если был запущен IDL isCorpCgcIdlIn в значении false
        // В этом случае все _sk поля будут иметь тип данных Long
        if (skEnabled) {
            // Обращаемся к сервису, если датасет не пустой
            if (!resDataSet.isEmpty()) {

                resDataSet = getSkClient().map(resDataSet, attributeCodes)
                        .drop(getColumnsFromAttributes(attributeCodes).toArray(new String[0]));

            } else {
                log.warn("EmptyTable - {}", tableName);
                // mapping _sid back to _sk
                resDataSet = remapSkToSid(resDataSet, getColumnsFromAttributes(attributeCodes));
            }
        } else {
            log.info("sk disabled - {}", tableName);
            // mapping _sid back to _sk
            resDataSet = remapSkToSid(resDataSet, getColumnsFromAttributes(attributeCodes));
        }

        return resDataSet;
    }

    public List<String> getColumnsFromAttributes(List<String> attributeCodes) {
        return attributeCodes.stream().map(code -> {
            String replacement = code.startsWith("@") ? "" : "_";
            return format("%s%s", code.replace("@", replacement), SOURCE_SK_SUFFIX);
        }).collect(Collectors.toList());
    }

    Dataset<Row> remapSkToSid(Dataset<Row> df, List<String> skColList) {
        Dataset<Row> resDataSet = df;
        for (String name : skColList) {
            resDataSet = resDataSet.withColumnRenamed(name, format("%ssk", name.substring(0, name.length() - 3)));
        }
        return resDataSet;
    }

    private SkClient getSkClient() {
        if (skClient == null) {
            skClient = SkClient.getInstance();
        }
        return skClient;
    }

}
