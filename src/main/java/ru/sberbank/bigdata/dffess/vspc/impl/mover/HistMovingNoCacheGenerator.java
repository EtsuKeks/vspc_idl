package ru.sberbank.bigdata.dffess.vspc.impl.mover;


import ru.sberbank.bigdata.essa.common.slave.app.SparkWrapper;
import ru.sberbank.bigdata.essa.common.slave.hdfs.HdfsService;
import ru.sberbank.bigdata.essa.common.slave.move.ClassicMovingGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.TransformConfiguration;
import ru.sberbank.bigdata.essa.common.structure.MovingTable;
import ru.sberbank.bigdata.essa.common.structure.part.Partitioning;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static ru.sberbank.bigdata.essa.common.constant.CommonConst.PARTITION_MONTH_FORMAT;
import static ru.sberbank.bigdata.essa.common.slave.hdfs.HdfsService.AFTER_OR_EQUALS_CUT_POINT_CONDITION;

/**
 * Реализация для hist таблиц, когда в снп удаляются перед мувом только партиции начиная с предыдущего месяца, в дифф как всегда только дописываются.
 * При наличии так же мувятся данные по статистикам источника, они не очищаются при ините, так как очистка в CtlStatisticHelper.
 * Использовать в паре с DataLoaderHist.
 */
public class HistMovingNoCacheGenerator extends ClassicMovingGenerator implements NoCacheMovingGenerator {

    @Override
    public List<MovingTable> generate(SparkWrapper sparkWrapper, HdfsService hdfsService,
                                      TransformConfiguration transformConfiguration, Partitioning partitioning) {

        List<MovingTable> movingTables = new ArrayList<>();

        MovingTable snpTable = generateSnpTable(transformConfiguration);
        DateTimeFormatter formatterToStr = DateTimeFormatter.ofPattern(PARTITION_MONTH_FORMAT);
        String cutPoint = LocalDate.now().minusMonths(1).format(formatterToStr);
        snpTable.setIncrTruncPartitions(hdfsService.preparePartitionsOnCondition(snpTable.getTo(), transformConfiguration.getInstanceCodePartition(), cutPoint, AFTER_OR_EQUALS_CUT_POINT_CONDITION));
        movingTables.add(snpTable);

        MovingTable diffTable = generateDiffTable(transformConfiguration);
        movingTables.add(diffTable);

        return movingTables;
    }

    public List<MovingTable> addCacheTableIfNeeded(HdfsService hdfsService, TransformConfiguration transformConfiguration) {
        return new ArrayList<>();
    }
}
