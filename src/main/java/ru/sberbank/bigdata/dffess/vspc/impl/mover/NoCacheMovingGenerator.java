package ru.sberbank.bigdata.dffess.vspc.impl.mover;


import ru.sberbank.bigdata.essa.common.slave.app.SparkWrapper;
import ru.sberbank.bigdata.essa.common.slave.hdfs.HdfsService;
import ru.sberbank.bigdata.essa.common.slave.move.MovingTablesGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.TransformConfiguration;
import ru.sberbank.bigdata.essa.common.structure.MovingTable;
import ru.sberbank.bigdata.essa.common.structure.TableMeta;
import ru.sberbank.bigdata.essa.common.structure.part.Partitioning;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static ru.sberbank.bigdata.essa.common.constant.CommonConst.PARTITIONS_STATISTIC;
import static ru.sberbank.bigdata.essa.common.constant.CommonConst.STATISTIC_TABLE;
import static ru.sberbank.bigdata.essa.common.structure.MovingTable.newInstance;
import static ru.sberbank.bigdata.essa.common.structure.TableMeta.newTable;
import static ru.sberbank.bigdata.essa.common.structure.part.provide.change.PartitionStatisticService.getStgPartitionsStatisticTableName;
import static ru.sberbank.bigdata.essa.common.utils.EntityStaticService.getStgEntityStatisticTableName;

/**
 * генератор списка таблиц, данные по которым нужно переместить на шаге movingTableService при обработке EtlKit.
 */
public interface NoCacheMovingGenerator extends MovingTablesGenerator {

    List<MovingTable> generate(SparkWrapper sparkWrapper, HdfsService hdfsService,
                               TransformConfiguration transformConfiguration, Partitioning partitioning);

    /**
     * Всегда можно попытаться добавить таблицу сттатистики если есть - хуже не будет))
     */
    default List<MovingTable> addStatisticTableIfNeed(HdfsService hdfsService, TransformConfiguration transformConfiguration) {
        String dstTableName = transformConfiguration.getTgtTable();
        String stgSchema = transformConfiguration.getStgSchema();
        String instanceCodePartition = transformConfiguration.getInstanceCodePartition();
        List<MovingTable> addTables = new ArrayList<>();
        String tableName = getStgEntityStatisticTableName(instanceCodePartition, dstTableName);
        TableMeta statisticTable = newTable(stgSchema, tableName);
        if (hdfsService.isExistTable(statisticTable)) {
            MovingTable statTable = newInstance(statisticTable, newTable(stgSchema, STATISTIC_TABLE))
                    .notTruncOnIncr();
            statTable.setInstanceCodePartition(instanceCodePartition);
            statTable.setInitTruncPartitions(singletonList(dstTableName));
            addTables.add(statTable);
        }

        String partitionsStatisticTableName = getStgPartitionsStatisticTableName(instanceCodePartition, dstTableName);
        TableMeta partitionsStatisticTable = newTable(stgSchema, partitionsStatisticTableName);
        if (hdfsService.isExistTable(partitionsStatisticTable)) {
            MovingTable partsTable = newInstance(partitionsStatisticTable, newTable(stgSchema, PARTITIONS_STATISTIC))
                    .notTruncOnIncr();
            partsTable.setInstanceCodePartition(instanceCodePartition);
            partsTable.setInitTruncPartitions(singletonList(dstTableName));
            addTables.add(partsTable);
        }

        return addTables;
    }
}
