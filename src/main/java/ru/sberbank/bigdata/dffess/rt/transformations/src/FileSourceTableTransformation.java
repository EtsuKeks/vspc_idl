package ru.sberbank.bigdata.dffess.rt.transformations.src;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.dffess.rt.transformations.BaseTransformation;

import java.util.List;

import static java.lang.String.format;
import static ru.sberbank.bigdata.sl.rt.logger.audit.AuditLogMessageBuilder.AUDIT_001;

/**
 * Трансформация для таблиц-источников считываемых в виде *.parquet файлов, расположенных в корневой папке
 * <p>
 * Используется как workaround для вычитывания большой таблицы со "сломанной" мета информацией
 * игнорируя вложенные history файлы
 * <p>
 * На примере Z_RECORDS
 * hdfs dfs -du -h /data/core/internal/EKS/pa/IBS/ | grep Z_RECORDS
 * 15.4 T    30.9 T   /data/core/internal/EKS/pa/IBS/Z_RECORDS
 * <p>
 * Данные хранятся в виде паркетов по ~500мб
 * <p>
 * дополнительно есть директория с историческими данными, разбитыми по месяцам
 * <p>
 * /data/core/internal/EKS/pa/IBS/Z_RECORDS/0_20171210000000/history/ctl_validto_mm=2019-10
 **/

public class FileSourceTableTransformation extends BaseTransformation {
    private final String sourceTable;


    public FileSourceTableTransformation(App app, String sourceTable) {
        super(app, sourceTable + "_" + App.LoadMode.INITIAL.name().toLowerCase());
        this.sourceTable = sourceTable;
    }

    @Override
    public void run() {
        selectParquetMirror(getInternalEksIbsFile(), sourceTable);
    }

    protected void selectParquetMirror(String tableSchema, String tableName) {
        String objectName = format("%1$s.%2$s", tableSchema, tableName);
        CatalogTable tableInfo = getTableInfo(tableSchema, tableName)
                .orElseThrow(() -> new RuntimeException(String.format("Неизвестное имя таблицы %s", objectName)));
        logger.log(AUDIT_001(objectName, objectName), true);
        // мы используем takeAsList вместо limit, тк spark для расчёта limit вычитывает все файлы,
        // в то время как limit останавливается на набранном кол-ве строк
        List<Row> tableRows = app.getSparkSession().read()
                .schema(tableInfo.schema())
                .option("pathGlobFilter", "*.parquet")
                .parquet(tableInfo.location().toString())
                .takeAsList(SRC_LOAD_LIMIT);

        Dataset<Row> df = app.getSparkSession().createDataFrame(tableRows, tableRows.isEmpty() ? tableInfo.schema() : tableRows.get(0).schema());
        saveIntermediateSourceTableSnp(df, STGTMP_SCHEMA, tableName);
    }
}
