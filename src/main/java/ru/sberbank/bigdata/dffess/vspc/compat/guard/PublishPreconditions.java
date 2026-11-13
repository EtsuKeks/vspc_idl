package ru.sberbank.bigdata.dffess.vspc.compat.guard;

import org.apache.hadoop.fs.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.EvalMode;
import org.apache.spark.sql.types.*;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherDao;
import ru.sberbank.bigdata.sl.rt.publisher.dto.TableInfo;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import scala.Option;

import java.io.*;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.*;
import static ru.sberbank.bigdata.sl.rt.core.Constants.CTL_LOADING;
import static ru.sberbank.bigdata.sl.rt.publisher.util.Constants.SUFFIX_DIFF;
import static ru.sberbank.bigdata.sl.rt.publisher.util.PublisherUtils.getDatabaseLocationPath;

/** Проверки до publish (BaseTransformation.publishChecked). */
public final class PublishPreconditions {

    private PublishPreconditions() {
    }

    // DV-08 (E/slave/move/MovingTableService.java:44): повтор с тем же L дописывает diff L, вендор заменяет секцию (V/PublisherDao.java:892)
    public static void keepLegs(String diffSchema, String table, String stgSchema, AppContext app, SparkSession spark, Runnable publish) {
        CatalogTable diff = new PublisherDao(spark).getCatalogTableOptional(new TableInfo(diffSchema, table, "")).get();
        // секция L целиком в одном каталоге, только если ctl_loading — первый уровень секций diff (порядок задаёт DDL, в meta его нет)
        List<String> levels = scala.collection.JavaConverters.seqAsJavaList(diff.partitionColumnNames());
        if (levels.isEmpty() || !CTL_LOADING.equals(levels.get(0))) {
            throw fail(diffSchema + "." + table + ": первый уровень секций " + levels + ", ожидается " + CTL_LOADING);
        }
        Path legs = new Path(new Path(diff.location()), CTL_LOADING + "=" + app.getLoadingId());
        Path aside = new Path(getDatabaseLocationPath(spark, stgSchema), table + SUFFIX_DIFF + "_" + app.getLoadingId());
        try {
            FileSystem fs = legs.getFileSystem(spark.sparkContext().hadoopConfiguration());
            restore(fs, aside, legs, diffSchema + "." + table, spark);
            if (app.getLoadMode() == App.LoadMode.INCREMENT && fs.exists(legs)) {
                fs.rename(legs, aside);
            }
            try {
                publish.run();
            } finally {
                restore(fs, aside, legs, diffSchema + "." + table, spark);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void restore(FileSystem fs, Path aside, Path legs, String diff, SparkSession spark) throws IOException {
        if (!fs.exists(aside)) {
            return;
        }
        for (FileStatus part : fs.listStatus(aside)) {
            Path to = new Path(legs, part.getPath().getName());
            fs.mkdirs(to);
            for (FileStatus f : fs.listStatus(part.getPath())) {
                fs.rename(f.getPath(), to);
            }
        }
        fs.delete(aside, true);
        spark.catalog().recoverPartitions(diff);
    }

    /** GUARD_SCHEMA_TYPES, DV-20 (E/slave/app/HiveWriter.java:10): тип входа != цели — ANSI-cast, как insertInto ESSA, иначе отказ (вендор дал бы NULL). */
    public static Dataset<Row> alignTypes(Dataset<Row> ds, String snpTable, SparkSession spark) {
        spark.catalog().refreshTable(snpTable);
        return alignTypes(ds, spark.table(snpTable).schema(), snpTable);
    }

    private static Dataset<Row> alignTypes(Dataset<Row> ds, StructType target, String snpTable) {
        List<String> names = Arrays.asList(target.fieldNames());
        List<Column> out = new ArrayList<>();
        List<String> cast = new ArrayList<>();
        List<String> bad = new ArrayList<>();
        for (StructField f : ds.schema().fields()) {
            Column c = col(f.name());
            if (names.contains(f.name())) {
                DataType to = target.apply(f.name()).dataType();
                String diff = f.name() + ": " + f.dataType().simpleString() + " -> " + to.simpleString();
                if (!to.simpleString().equalsIgnoreCase(f.dataType().simpleString())) {
                    if (Cast.canANSIStoreAssign(f.dataType(), to)) {
                        c = new Column(new Cast(c.expr(), to, Option.empty(), EvalMode.ANSI())).as(f.name());
                        cast.add(diff);
                    } else {
                        bad.add(diff);
                    }
                }
            }
            out.add(c);
        }
        if (!bad.isEmpty()) {
            throw fail(snpTable + ": типы колонок входа несовместимы с целью (ESSA падала на insertInto), без отказа значения стали бы NULL: " + bad);
        }
        if (cast.isEmpty()) {
            return ds;
        }
        info(snpTable + ": типы входа приведены к цели (ANSI store assignment, как insertInto ESSA): " + cast);
        return ds.select(out.toArray(new Column[0]));
    }
}
