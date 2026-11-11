package ru.sberbank.bigdata.essa.common.ctl;

import ru.sberbank.bigdata.essa.common.ctl.dto.*;
import ru.sberbank.bigdata.essa.common.logger.LoggerWrapper;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

/** Local stand: CTL client without network. Statistics are only logged; nothing affects data. */
public class CtlHelper {
    private final LoggerWrapper logger;

    public CtlHelper(String ctlUrl, LoggerWrapper logger) {
        this.logger = logger;
    }

    private void log(String s) {
        if (logger != null) logger.info("[stand-ctl] " + s);
    }

    public Long getMaxLoadingIdFromSource(String fullWfName, String tableName) {
        return Long.MAX_VALUE;
    }

    public void setStatVal(Long loadingId, Long entityId, Long statId, String value) {
        setStatVal(loadingId, entityId, statId, value, null);
    }

    public void setStatVal(Long loadingId, Long entityId, Long statId, String value, String profile) {
        log("stat loading=" + loadingId + " entity=" + entityId + " stat=" + statId + " value=" + value);
    }

    public CtlLoadingParams getLoadingParamsById(Long loadingId) {
        throw new UnsupportedOperationException("stand: CtlAdapter is stubbed");
    }

    public CtlLoading getLoadingById(Long loadingId) {
        return new CtlLoading(null, null);
    }

    public CtlWf getCtlWfByLoadingId(Long loadingId) {
        throw new UnsupportedOperationException("stand: CtlAdapter is stubbed");
    }

    public CtlLoadingStatus getLastLoadingStatusByName(String wfName, LocalDate lowerBound) {
        return null;
    }

    public CtlLoadingStatus getLastLoadingStatusByName(String wfName, LocalDate lowerBound, Long currentLoadingId) {
        return null;
    }

    public CtlEntity createEntity(Long id, String name, String path, String storage, Long parentId) {
        return new CtlEntity(null, id, 1L, name, id, parentId, path, storage);
    }

    public CtlEntity getOrCreateEntity(String mnemonicCode, Long parentEntityId, String path, String storage,
                                       Long availableRangeStartInclusive, Long availableRangeEndInclusive) {
        return createEntity(availableRangeStartInclusive, mnemonicCode, path, storage, parentEntityId);
    }

    public List<CtlParentEntity> getChildEntities(Long parentId) {
        return Collections.emptyList();
    }

    public Long startWorkflowByName(String workflowName) {
        return 1L;
    }

    public boolean completeLoading(Long loadingId) {
        return true;
    }

    public void delay(int delayMs) {
    }

    public String getTableStatisticsPath() {
        return "\\\\data\\\\usl";
    }

    public String getTableStatisticsStorage() {
        return "HDFS";
    }

    public String getStatisticsDummyWfName() {
        return "USL__DATASET";
    }
}
