package ru.sberbank.bigdata.dffess.rt.transformations.src;

import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.dffess.rt.transformations.BaseTransformation;

import static ru.sberbank.bigdata.sl.rt.reader.SourceTable.*;

/**
 * Трансформация для таблиц-источников
 */
public class SourceTableTransformation extends BaseTransformation {
    private final String sourceTable;
    private final App.LoadMode loadMode;
    private final String sourceSchema;
    private final SourceType sourceType;

    public SourceTableTransformation(App app, String sourceTable, App.LoadMode loadMode, String sourceSchema, SourceType sourceType) {
        super(app, sourceTable + "_" + loadMode.name().toLowerCase());
        this.sourceTable = sourceTable;
        this.loadMode = loadMode;
        this.sourceSchema = sourceSchema;
        this.sourceType = sourceType;
    }

    public SourceTableTransformation(App app, String sourceTable, String SourceSchema, SourceType sourceType) {
        this(app, sourceTable, app.ctx().getLoadMode(), SourceSchema, sourceType);
    }

    @Override
    public void run() {
        if (loadMode == App.LoadMode.INITIAL) {
            selectMirror(sourceSchema, sourceTable, sourceType);
        } else {
            if (sourceSchema.endsWith(HIST_POSTFIX)) {
                selectHistDiff(sourceSchema, sourceTable, sourceType);
            } else {
                selectDeltaDiff(sourceSchema, sourceTable, sourceType);
            }
        }
    }
}
