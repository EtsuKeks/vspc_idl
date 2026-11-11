package local.stand;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.dffess.vspc.impl.VspcDataProductGenerator;
import ru.sberbank.bigdata.essa.common.constant.CommonConst;
import ru.sberbank.bigdata.essa.common.logger.LoggerWrapper;
import ru.sberbank.bigdata.essa.common.master.AppRunner;
import ru.sberbank.bigdata.essa.common.master.AppRunnerGenerator;
import ru.sberbank.bigdata.essa.common.master.DataProductRunner;
import ru.sberbank.bigdata.essa.common.master.app.*;
import ru.sberbank.bigdata.essa.common.master.app.forApp.*;
import ru.sberbank.bigdata.essa.common.master.app.recover.DataProductRecovery;
import ru.sberbank.bigdata.essa.common.slave.InPoint;

import java.util.*;

/**
 * concurrent_init_incr scenario: two 3.0 loadings (INIT and INCR, separate CTL workflows, same stg/stgtmp schemas)
 * in ONE JVM, each with its own DataProductRunner/AppConfiguration, interleaved app by app by a global turnstile.
 * <pre>
 *   RunPair &lt;root&gt; &lt;schedule&gt; &lt;initId&gt; &lt;incrId&gt; &lt;faketime&gt; &lt;exportDir&gt; [file.sql...]
 *   schedule: comma list of I:&lt;app&gt; / N:&lt;app&gt; (I = INIT loading, N = INCR loading); "SEQ" = INIT then INCR.
 * </pre>
 * Only one app runs at any time (the ESSA app closes the SparkSession; the next getOrCreate builds a new one).
 * A head token whose loading is finished, or whose loading waits at another app (its app failed and the runner
 * skipped/retried), is skipped. Apps not in the schedule run after the schedule is exhausted.
 */
public class RunPair {

    static final Object LOCK = new Object();
    static List<String> sched = new ArrayList<>();
    static int pos = 0;
    static String running = null;
    static final Map<String, String> waitingAt = new HashMap<>();
    static final Set<String> finished = new HashSet<>();
    static final List<String> trace = Collections.synchronizedList(new ArrayList<String>());

    static void enter(String who, String tok) throws InterruptedException {
        synchronized (LOCK) {
            waitingAt.put(who, tok);
            while (true) {
                while (pos < sched.size()) {   // drop head tokens that can no longer run
                    String h = sched.get(pos), hw = h.substring(0, 1);
                    if (finished.contains(hw)) {
                        System.out.println("[pair] skip " + h + " (loading " + hw + " finished)"); pos++;
                    } else break;
                }
                boolean headIsMe = pos < sched.size() && sched.get(pos).equals(tok);
                boolean exhausted = pos >= sched.size();
                // the other loading waits at a token that is not the head and the head belongs to me but is not my app:
                // my runner went another way (failure/retry): skip that head
                if (running == null && !headIsMe && !exhausted) {
                    String h = sched.get(pos), hw = h.substring(0, 1);
                    String other = who.equals("I") ? "N" : "I";
                    String wo = waitingAt.get(other);
                    boolean otherBlocked = finished.contains(other) || (wo != null && !wo.equals(h));
                    if (hw.equals(who) && otherBlocked) {
                        System.out.println("[pair] skip " + h + " (loading " + who + " is at " + tok + ")"); pos++; continue;
                    }
                }
                if (running == null && (headIsMe || exhausted)) {
                    if (headIsMe) pos++;
                    running = tok;
                    waitingAt.remove(who);
                    trace.add(tok);
                    System.out.println("[pair] >>> " + tok + (exhausted ? " (after schedule)" : ""));
                    return;
                }
                LOCK.wait(500);
            }
        }
    }

    static void exit(String tok, Throwable err) {
        synchronized (LOCK) {
            running = null;
            System.out.println("[pair] <<< " + tok + (err == null ? " OK" : " FAILED: " + err));
            if (err != null) trace.add(tok + "!FAILED");
            LOCK.notifyAll();
        }
    }

    static DataProductRunner runner(final String who, CommonConst.LoadMode mode, long loadingId) {
        final String wfName = mode == CommonConst.LoadMode.INIT ? "USL_VSPC_IDL_INIT" : "USL_VSPC_IDL";
        final SparkConfKeeper conf = new SparkConfKeeper("", "", "", "", "", "",
                "stand@LOCAL", "/tmp/none/", "default", "", "", "");
        final AppConfiguration app = new AppConfiguration(
                "ESSA", 10, "", false, "", "vspc_idl", null,
                "http://dev.ctl.stub", 904000000L, "904000000-906999999",
                loadingId, wfName, "", mode, 1,
                CommonConst.SkMode.ON, Collections.emptyList(), null,
                InitStep.resolveInitStep(null, mode),
                PartialInitMode.resolvepartialInitMode(mode, null, "", null),
                Collections.emptyMap(), null, null, null);
        CtlAdapter ctl = new CtlAdapter() {
            public SparkConfKeeper getSparkConfKeeper() { return conf; }
            public AppConfiguration getAppConfiguration() { return app; }
        };
        DataProductGenerator gen = new DataProductGenerator() {
            private final VspcDataProductGenerator real = new VspcDataProductGenerator();
            public DataProduct generate(String instanceCode) { return real.generate(instanceCode); }
            public DataProductRecovery getDataProductRecovery(AppConfiguration c) {
                return new DataProductRecovery() {
                    public DataProductState getLastLoadingState() { return null; }
                    public void dump(DataProductState s) { }
                };
            }
        };
        AppRunnerGenerator local = new AppRunnerGenerator() {
            public AppRunner generate(final SparkApp sparkApp, final LoggerWrapper log) {
                return new AppRunner() {
                    public SparkApp getSparkApp() { return sparkApp; }
                    public void run() {
                        String tok = who + ":" + sparkApp.getName();
                        try { enter(who, tok); } catch (InterruptedException e) { throw new RuntimeException(e); }
                        Throwable err = null;
                        try {
                            InPoint.main(sparkApp.downloadToStr());
                        } catch (RuntimeException | Error e) {
                            err = e; throw e;
                        } finally { exit(tok, err); }
                    }
                };
            }
        };
        return new DataProductRunner(gen, ctl, local);
    }

    static final String[] DAG = {"chg_div_h", "kit_compress_stage", "kit_move_stage", "addr", "addr_h", "terr_div_type",
            "time_zone_rf", "div_", "div_h", "div_type", "div_div_relation", "kit_compress_vspc_idl", "kit_move_vspc_idl",
            "kit_compress_finisher", "kit_move_finisher"};

    public static void main(String[] args) throws Exception {
        String root = args[0], schedule = args[1];
        final long initId = Long.parseLong(args[2]), incrId = Long.parseLong(args[3]);
        String faketime = args[4], exportDir = args[5];
        if ("SEQ".equals(schedule)) {
            for (String a : DAG) sched.add("I:" + a);
            for (String a : DAG) sched.add("N:" + a);
        } else for (String t : schedule.split(",")) if (!t.trim().isEmpty()) sched.add(t.trim());
        System.out.println("[pair] schedule " + sched);
        SparkSession spark = Stand.spark(root, true);
        Stand.phase("apply");
        for (int i = 6; i < args.length; i++) Stand.sql(spark, args[i]);
        Stand.phase("run");
        Stand.setClock(faketime);
        final Map<String, String> result = new TreeMap<>();
        Thread ti = new Thread(new Runnable() { public void run() { go("I", CommonConst.LoadMode.INIT, initId, result); } }, "INIT");
        Thread tn = new Thread(new Runnable() { public void run() { go("N", CommonConst.LoadMode.INCR, incrId, result); } }, "INCR");
        ti.start(); tn.start(); ti.join(); tn.join();
        Stand.setClock("-");
        System.out.println("[pair] trace " + trace);
        System.out.println("[pair] result " + result);
        Stand.phase("export");
        if (!"-".equals(exportDir)) {
            if (spark.sparkContext().isStopped()) spark = Stand.spark(root, true);
            Stand.export(spark, exportDir);
            try {
                spark.table("usl_vspc_idl_stg.chg_div_h").write().mode("overwrite").parquet("file://" + exportDir + ".stg_chg_div_h");
            } catch (Exception e) { System.out.println("[pair] no stg.chg_div_h: " + e.getMessage().split("\n")[0]); }
            spark.sql("show tables in usl_vspc_idl_stg").show(200, false);
        }
        java.nio.file.Files.write(java.nio.file.Paths.get(exportDir + ".pair.txt"),
                ("trace " + trace + "\nresult " + result + "\n").getBytes("UTF-8"));
        Stand.phase("done");
        spark.stop();
    }

    static void go(String who, CommonConst.LoadMode mode, long id, Map<String, String> result) {
        String r = "SUCCESS";
        try {
            runner(who, mode, id).run();
        } catch (Throwable e) {
            r = "FAILED " + e;
            e.printStackTrace(System.out);
        } finally {
            synchronized (LOCK) { finished.add(who); waitingAt.remove(who); LOCK.notifyAll(); }
            synchronized (result) { result.put(who + "(" + mode + "," + id + ")", r); }
            System.out.println("[pair] loading " + who + " " + r);
        }
    }
}
