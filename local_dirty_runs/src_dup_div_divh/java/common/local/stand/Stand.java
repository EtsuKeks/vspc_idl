package local.stand;

import org.apache.spark.sql.SparkSession;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * Local single-JVM stand driver (same class for 3.0 and 4.0; the classpath decides which product runs).
 * <pre>
 *   Stand &lt;root&gt; hdfsd                               MiniDFSCluster under &lt;root&gt;/hdfs on -Dstand.hdfs=PORT (keep running)
 *   Stand &lt;root&gt; sql &lt;file.sql&gt;...                    run Spark SQL scripts (statements end with ';' at line end)
 *   Stand &lt;root&gt; load &lt;30|40&gt; INIT|INCR &lt;loadingId&gt; &lt;faketime|-&gt; &lt;exportDir|-&gt; [file.sql...]
 *                                                     ONE load in ONE JVM: sql files (source state of the load),
 *                                                     then the product with the business clock set to faketime,
 *                                                     then export of the 16 target tables to exportDir (parquet)
 * </pre>
 * The business clock is the patched java.time.Clock (built from the JDK sources by stand.sh build): its offset is
 * switched on only for the product run, so apply/export use real time.
 */
public class Stand {

    static final String[] TABLES = {"addr", "addr_h", "div_", "div_h", "div_type", "div_div_relation", "terr_div_type", "time_zone_rf"};

    static String fs() {
        return "hdfs://localhost:" + System.getProperty("stand.hdfs");
    }

    /** Session settings. For 3.0 also the settings of ESSA SparkAppEnvConfigurationCreator, so that 3.0's own
     *  getOrCreate() gets an identical session. */
    static SparkSession spark(String root, boolean v30) {
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));
        SparkSession.Builder b = SparkSession.builder()
                .master(System.getProperty("spark.master", "local[2]")).appName("stand" + (v30 ? "30" : "40"))
                .config("spark.hadoop.fs.defaultFS", fs())
                .config("spark.sql.warehouse.dir", "/warehouse")
                .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + root + "/metastore_db;create=true")
                .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
                .config("spark.sql.session.timeZone", "Europe/Moscow")
                .config("spark.sql.shuffle.partitions", System.getProperty("spark.sql.shuffle.partitions", "4"))
                .config("spark.default.parallelism", "4")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
                .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
                .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
                .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
                .config("spark.sql.parquet.writeLegacyFormat", "true");
        if (v30) {  // essa-common-lib slave/app/SparkAppEnvConfigurationCreator.java:17-30
            b = b.config("spark.hadoop.metastore.catalog.default", "spark")
                    .config("hive.exec.dynamic.partition", "true")
                    .config("hive.exec.dynamic.partition.mode", "nonstrict")
                    .config("hive.exec.max.dynamic.partitions", "1000000")
                    .config("spark.sql.parquet.compression.codec", "gzip")
                    .config("hive.mapred.supports.subdirectories", "true")
                    .config("mapred.input.dir.recursive", "true")
                    .config("hive.supports.subdirectories", "true")
                    .config("hive.input.dir.recursive", "true")
                    .config("spark.sql.hive.convertMetastoreParquet", "false")
                    .config("spark.shuffle.service.enabled", "true")
                    .config("spark.sql.crossJoin.enabled", "true");
        } else {
            b = b.config("spark.sql.adaptive.enabled", "true");
        }
        SparkSession s = b.enableHiveSupport().getOrCreate();
        s.sparkContext().setLogLevel(System.getProperty("stand.log", "WARN"));
        return s;
    }

    /** Patched java.time.Clock$SystemClock.STAND_OFFSET (ms added to the wall clock of java.time.*). */
    static void setClock(String faketime) throws Exception {
        java.lang.reflect.Field f = Class.forName("java.time.Clock$SystemClock").getDeclaredField("STAND_OFFSET");
        f.setAccessible(true);
        long off = 0L;
        if (faketime != null && !faketime.equals("-")) {
            off = java.time.LocalDateTime.parse(faketime.replace(' ', 'T')).atZone(java.time.ZoneId.systemDefault())
                    .toInstant().toEpochMilli() - System.currentTimeMillis();
        }
        f.setLong(null, off);
        System.out.println("[stand] business clock " + (off == 0 ? "real" : "= " + faketime) + " -> " + java.time.LocalDateTime.now());
    }

    static void sql(SparkSession spark, String file) throws Exception {
        String text = new String(Files.readAllBytes(Paths.get(file)), StandardCharsets.UTF_8).replace("{{ROOT}}", "");
        for (String st : statements(text)) {
            String head = st.replaceAll("\\s+", " ");
            System.out.println("[sql] " + head.substring(0, Math.min(140, head.length())));
            String low = st.trim().toLowerCase();
            if (st.startsWith("/*opt*/")) {   // optional statement (e.g. a table only one version has): errors are skipped
                try { spark.sql(st.substring(7)); } catch (Exception e) { System.out.println("[sql] skip: " + e.getMessage().split("\n")[0]); }
            } else if (low.startsWith("select") || low.startsWith("show") || low.startsWith("describe")) spark.sql(st).show(1000, false);
            else if (low.startsWith("msck")) {   // the 3.0 DDL repairs unpartitioned tables too: Spark 3.5 refuses that
                try { spark.sql(st); } catch (Exception e) { System.out.println("[sql] skip: " + e.getMessage().split("\n")[0]); }
            } else spark.sql(st);
        }
    }

    static void export(SparkSession spark, String dir) {
        for (String layer : new String[]{"snp", "diff"}) {
            for (String t : TABLES) {
                spark.table("usl_vspc_idl_" + layer + "." + t).write().mode("overwrite").parquet("file://" + dir + "/" + layer + "_" + t);
            }
        }
        try {   // MARK_SD: rows of stg.chg_div_h per load
            spark.table("usl_vspc_idl_stg.chg_div_h").write().mode("overwrite").option("header", "true")
                    .csv("file://" + dir + "/stg_chg_div_h_csv");
        } catch (Exception e) {
            System.out.println("[export] no stg.chg_div_h rows: " + e.getMessage().split("\n")[0]);
        }
        try { spark.table("usl_vspc_idl_stg.chg_div_h").write().mode("overwrite").parquet("file://" + dir + "/../" + new java.io.File(dir).getName() + ".stg_chg_div_h"); }
        catch (Exception e) { System.out.println("[export] chg_div_h rows: " + e.getMessage().split("\n")[0]); }
        // schema of the shared preselect table (D-16: 3.0 keeps the DDL types, e.g. start_dt DATE)
        try {
            StringBuilder sb = new StringBuilder();
            for (org.apache.spark.sql.types.StructField f : spark.table("usl_vspc_idl_stg.chg_div_h").schema().fields())
                sb.append(f.name()).append(':').append(f.dataType().simpleString()).append('\n');
            Files.write(Paths.get(dir, "stg.chg_div_h.schema"), sb.toString().getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            System.out.println("[export] no stg.chg_div_h: " + e.getMessage().split("\n")[0]);
        }
        System.out.println("[export] done -> " + dir);
    }

    static List<String> statements(String text) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        for (String line : text.split("\n")) {
            String t = line.trim();
            if (t.startsWith("--") || t.isEmpty()) continue;
            cur.append(line).append('\n');
            if (t.endsWith(";")) {
                String s = cur.toString().trim();
                out.add(s.substring(0, s.length() - 1));
                cur.setLength(0);
            }
        }
        if (cur.toString().trim().length() > 0) out.add(cur.toString().trim());
        return out;
    }

    static long t0 = System.currentTimeMillis();

    static void phase(String what) {
        System.out.println(String.format("[stand] %-7s at %5.1fs", what, (System.currentTimeMillis() - t0) / 1000.0));
    }

    public static void main(String[] args) throws Exception {
        String root = args[0], cmd = args[1];
        if ("hdfsd".equals(cmd)) {
            java.io.File base = new java.io.File(root, "hdfs");
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set(org.apache.hadoop.hdfs.MiniDFSCluster.HDFS_MINIDFS_BASEDIR, base.getAbsolutePath());
            conf.set("dfs.permissions.enabled", "false");
            org.apache.hadoop.hdfs.MiniDFSCluster dfs = new org.apache.hadoop.hdfs.MiniDFSCluster.Builder(conf)
                    .nameNodePort(Integer.parseInt(System.getProperty("stand.hdfs"))).numDataNodes(1)
                    .format(!base.exists()).manageNameDfsDirs(true).manageDataDfsDirs(true).build();
            dfs.waitActive();
            System.out.println("[hdfsd] up at " + fs());
            Thread.sleep(Long.MAX_VALUE);
        }
        if ("sql".equals(cmd)) {
            SparkSession spark = spark(root, false);
            for (int i = 2; i < args.length; i++) sql(spark, args[i]);
            spark.stop();
            return;
        }
        if (!"load".equals(cmd)) throw new IllegalArgumentException("unknown command " + cmd);
        boolean v30 = "30".equals(args[2]);
        String mode = args[3], loadingId = args[4], faketime = args[5], exportDir = args[6];
        SparkSession spark = spark(root, v30);
        phase("apply");
        for (int i = 7; i < args.length; i++) sql(spark, args[i]);
        phase("run");
        setClock(faketime);
        if (v30) {
            Class.forName("local.stand.Run30").getMethod("run", String.class, long.class).invoke(null, mode, Long.parseLong(loadingId));
        } else {
            System.setProperty("stand.mode", mode);
            System.setProperty("stand.loadingId", loadingId);
            Class.forName("ru.sberbank.bigdata.dffess.vspc.MainClass").getMethod("main", String[].class)
                    .invoke(null, (Object) new String[]{loadingId, "http://ctl.stub"});
        }
        setClock("-");
        phase("export");
        if (!"-".equals(exportDir)) {
            if (spark.sparkContext().isStopped()) spark = spark(root, v30);
            export(spark, exportDir);
        }
        phase("done");
        spark.stop();
    }
}
