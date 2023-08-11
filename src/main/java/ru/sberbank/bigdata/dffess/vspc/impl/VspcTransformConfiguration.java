package ru.sberbank.bigdata.dffess.vspc.impl;

import org.apache.spark.sql.Column;
import ru.sberbank.bigdata.essa.common.structure.MetadataKeeper;
import ru.sberbank.bigdata.essa.common.templates.TransformConfiguration;
import ru.sberbank.hermes.core.dmbuilder.config.ModuleConfiguration;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.essa.common.constant.CommonConst.*;
import static ru.sberbank.bigdata.essa.common.constant.CommonConst.DATE;
import static ru.sberbank.bigdata.essa.common.utils.Expression.generatePartitionMonthValue;
import static ru.sberbank.bigdata.dffess.vspc.constant.VpscConstant.*;

public class VspcTransformConfiguration extends TransformConfiguration {

    private String stgPath;

    public  VspcTransformConfiguration(ModuleConfiguration moduleConfiguration) {
        super(moduleConfiguration);
        stgPath = moduleConfiguration.getValue(STG_PATH);
    }

    @Override
    protected MetadataKeeper getMetadataKeeper() {
        return new VspcMetadataKeeper();
    }

    @Override
    public String getDataProduct() { return DATA_PRODUCT; }

    @Override
    public String getIncrCutOffDstDateString() {
        DateTimeFormatter formatterToStr = DateTimeFormatter.ofPattern(PARTITION_FORMAT);
        return LocalDate.now().minusDays(DEPTH_DELTA).format(formatterToStr);
    }

    @Override
    public Column getIncrCutOffPartitionMonth() {
        return generatePartitionMonthValue(getIncrCutOffSrcDate());
    }

    @Override
    public String getIncrCutOffPartitionMonthString() {
        DateTimeFormatter formatterToStr = DateTimeFormatter.ofPattern(PARTITION_MONTH_FORMAT);
        return LocalDate.now().minusDays(DEPTH_DELTA).format(formatterToStr);
    }

    @Override
    public Column getIncrCutOffSrcDate() {
        return date_sub(current_date(), DEPTH_DELTA);
    }

    @Override
    public Column getInitCutOffDate() {
        return lit(VSPC_IDL_CUTOFF_DATE).cast(DATE);
    }

    @Override
    public boolean isNeedToRecalcStartDate() {
        return false;
    }

    @Override
    public String getStagePath() {
        return stgPath;
    }
}
