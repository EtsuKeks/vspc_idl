package ru.sberbank.bigdata.dffess.vspc.impl.loader;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import ru.sberbank.bigdata.essa.common.exception.LoadingException;
import ru.sberbank.bigdata.essa.common.logger.LoggerWrapper;
import ru.sberbank.bigdata.essa.common.slave.app.SparkWrapper;
import ru.sberbank.bigdata.essa.common.slave.hdfs.HdfsService;
import ru.sberbank.bigdata.essa.common.slave.loading.SnpDataLoader;
import ru.sberbank.bigdata.essa.common.slave.templates.TransformConfiguration;
import ru.sberbank.bigdata.essa.common.structure.TableConfiguration;
import ru.sberbank.bigdata.essa.common.utils.EntityStaticService;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.essa.common.constant.CommonColumnName.*;
import static ru.sberbank.bigdata.essa.common.constant.CommonConst.*;
import static ru.sberbank.bigdata.essa.common.utils.Expression.*;

/**
 * Лоадер для классических h таблиц НДЛ - когда инкремент только добавляет записи (в приемнике нет deleted_flag и ctl_action)
 * а у существующих только пересчитываются дейт_енд по этим добавлениям.
 * Обязательно нужно заполнить на источнике  ctl_validfrom, даже если ее нет в источнике, то хотя бы сегодняшней датой
 * Так как при инкременте отбирает только три партиции, два последних месяца (потому что на стыке в них могут быть изменения) и максимальную,
 * требует использования соотвествующего мувинг генератора, чтобы при инкременте удалялсь только эти партиции, а не полностью таргет таблица.
 * Сейчас настроен только на партиционирование по месяцам.
 * Эта модификация лоадера ожидает на входе дифф с источника, то есть только добавленные, обновленные, удаленые записи (обязательно с deleted_flag)
 */
public class DataLoaderLinkHistDiff extends SnpDataLoader {

    private static final String CTL_VALIDFROM_TO_START_DT = "ctl_validfrom_to_start_dt";

    @Override
    public void fillTargetDatasets(Dataset<Row> diffFromSrc, HdfsService hdfsService, EntityStaticService entityStaticService, SparkWrapper sparkWrapper,
                                   TransformConfiguration transformConfiguration, LoggerWrapper loggerWrapper, TableConfiguration tableConfiguration) {

        if (!Arrays.asList(diffFromSrc.schema().fieldNames()).contains(DELETED_FLAG)) {
            throw new LoadingException("DELETED_FLAG отсутствует в датасете diffFromSrc!");
        }
        Column startDt = to_date(lit(MIN_DATE));
        if (transformConfiguration.getLoadMode() == LoadMode.INCR) {
            diffFromSrc = diffFromSrc.withColumn(CTL_VALIDFROM_TO_START_DT, col(CTL_VALIDFROM));
            startDt = date_sub(to_date(col(CTL_VALIDFROM_TO_START_DT)), N1);
            //кастим валид фром к таймстамп если он строка, потому что он должен быть таймстамп
            if (DataTypes.StringType.equals(getFieldType(diffFromSrc, CTL_VALIDFROM))) {
                diffFromSrc = diffFromSrc.withColumn(CTL_VALIDFROM, col(CTL_VALIDFROM).cast(DataTypes.TimestampType));
            }
        }
        if (transformConfiguration.getInstanceCodePartition() != null) {
            diffFromSrc = diffFromSrc.withColumn(PARTITION_COLUMN_INSTANCE, lit(transformConfiguration.getInstanceCodePartition()));
        }
        diffFromSrc = diffFromSrc.withColumn(START_DT, startDt)
                .withColumn(END_DT, to_date(lit(MAX_DATE)));
        Dataset<Row> emptyPrototype = sparkWrapper.getDataset(transformConfiguration.getTgtSchemaSnp(), transformConfiguration.getTgtTable());
        //не теряем deleted_flag с источника
        emptyPrototype = emptyPrototype.withColumn(DELETED_FLAG, lit(false));
        diffFromSrc = sparkWrapper.matchSchemas(emptyPrototype, diffFromSrc);

        diffFromSrc = diffFromSrc
                .withColumn(CTL_LOADING, lit(transformConfiguration.getLoadingId()));

        if (transformConfiguration.getLoadMode() == LoadMode.INCR) {
            Dataset<Row> dstSnp = sparkWrapper.getDataset(transformConfiguration.getTgtSchemaSnp(), transformConfiguration.getTgtTable());
            if (transformConfiguration.getInstanceCodePartition() != null) {
                dstSnp = dstSnp.where(col(PARTITION_COLUMN_INSTANCE).equalTo(transformConfiguration.getInstanceCodePartition()));
            }
            //изменяться могут только записи с end_date=MAX_DATE
            Dataset<Row> modifiableData = dstSnp.where(col(transformConfiguration.getPartitionColumnDst()).equalTo(generatePartitionMonthValue(to_date(lit(MAX_DATE)))));
            //текущий и предыдущий месяцы придется полностью переписать, поэтому здесь выбираем все что было
            Dataset<Row> rewritableData = dstSnp.where(col(transformConfiguration.getPartitionColumnDst()).isin(generatePartitionMonthValue(to_date(current_date())),
                    generatePartitionMonthValue(add_months(to_date(current_date()), -1))));
            // мерджим и приземляем чтоб не считать два раза, для снп и дифф
            Dataset<Row> snpWithDiff = mergeSnpAndDiffWithLanding(hdfsService, sparkWrapper, diffFromSrc,
                    modifiableData.withColumn(DELETED_FLAG, lit(false)),
                    transformConfiguration);

            targetSnp = snpWithDiff.where(
                            NEW_CONDITION.and(NOT_DEL_CONDITION)
                                    .or(NOT_EQUALS_CONDITION.and(col(RN).equalTo(lit(N1))).and(NOT_DEL_CONDITION))
                                    .or(EQUALS_CONDITION.and(col(RN).equalTo(lit(N2))))
                                    .or(OLD_CONDITION)
                    ).withColumn(START_DT, when(NEW_CONDITION, to_date(lit(MIN_DATE))).otherwise(col(START_DT)))
                    .withColumn(CTL_VALIDFROM, when(EQUALS_CONDITION.or(OLD_CONDITION), col(CTL_VALIDFROM)).otherwise(lit(transformConfiguration.getCtlValidfrom())));

            //смещаем дату закрытия у записей, которые обновились либо удалились
            targetSnp = targetSnp.union(snpWithDiff.where(NOT_EQUALS_CONDITION.and(col(RN).equalTo(lit(N2))))
                    .withColumn(END_DT, date_sub(to_date(col(ANOTHER_CTL_VALIDFROM)), N2))
                    .withColumn(CTL_VALIDFROM, lit(transformConfiguration.getCtlValidfrom()))
                    .withColumn(CTL_LOADING, lit(transformConfiguration.getLoadingId()))
                    .where(ANOTHER_DEL_CONDITION.or(col(END_DT).geq(col(START_DT)))) //это потому что если START_DT>END_DT это второе обновление за день и его надо просто похерить
            );

//            targetSnp = targetSnp.drop(DELETED_FLAG);
            prepareTargetDiff(targetSnp.where(col(CTL_LOADING).equalTo(lit(transformConfiguration.getLoadingId()))), transformConfiguration);
            targetSnp = dropTechColumns(targetSnp);
            targetSnp = targetSnp.union(rewritableData);
        } else {
            targetSnp = diffFromSrc.withColumn(CTL_VALIDFROM, lit(transformConfiguration.getCtlValidfrom()));
//            targetSnp = targetSnp.drop(DELETED_FLAG);
        }
        targetSnp = addPartitionMonthColumn(targetSnp, transformConfiguration.getPartitionColumnDst(), transformConfiguration.getPartitionColumnSrc());
    }

    @Override
    protected void clearAfterMe(SparkWrapper sparkWrapper, TransformConfiguration transformConfiguration, LoggerWrapper loggerWrapper) {
//        if (transformConfiguration.getLoadMode() == LoadMode.INCR) {
//            clearStageTable(TMP_PREFIX + transformConfiguration.getStgTable(), dmBuilder, transformConfiguration, logger);
//        }
    }

    private void prepareTargetDiff(Dataset<Row> snpWithDiff, TransformConfiguration transformConfiguration) {

        targetDiff = snpWithDiff.withColumn(CTL_DO, lit(CTL_DO_ACTION.R.toString()))
                .withColumn(CTL_ACTION, when(NEW_CONDITION.or(NOT_EQUALS_CONDITION.and(col(RN).equalTo(lit(N1)))), lit(ACTION.I.toString())).otherwise(lit(ACTION.U.toString())));

        targetDiff = targetDiff
                .union(
                        targetDiff.withColumn(CTL_DO, lit(CTL_DO_ACTION.U.toString()))
                                .withColumn(CTL_LOADING, lit(transformConfiguration.getLoadingId()))
                                .withColumn(CTL_ACTION, when(NEW_CONDITION.or(NOT_EQUALS_CONDITION.and(col(RN).equalTo(lit(N1)))), lit(ACTION.D.toString())).otherwise(lit(ACTION.U.toString())))
                                .withColumn(END_DT,
                                        when(NOT_EQUALS_CONDITION.and(col(RN).equalTo(lit(N2))), to_date(lit(MAX_DATE)))
                                                .otherwise(col(END_DT)))
                );
        targetDiff = addPartitionMonthColumn(targetDiff, transformConfiguration.getPartitionColumnDst(), transformConfiguration.getPartitionColumnSrc());
        targetDiff = dropTechColumns(targetDiff);
    }

    @Override
    public void clearStageTable(String tableName, SparkWrapper sparkWrapper, TransformConfiguration transformConfiguration) {
        super.clearStageTable(tableName, sparkWrapper, transformConfiguration);
    }
}
