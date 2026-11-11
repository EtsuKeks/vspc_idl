package local.stand;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDataProductGenerator;
import ru.sberbank.bigdata.essa.common.constant.CommonConst;
import ru.sberbank.bigdata.essa.common.logger.LoggerWrapper;
import ru.sberbank.bigdata.essa.common.master.AppRunner;
import ru.sberbank.bigdata.essa.common.master.AppRunnerGenerator;
import ru.sberbank.bigdata.essa.common.master.DataProductRunner;
import ru.sberbank.bigdata.essa.common.master.app.*;
import ru.sberbank.bigdata.essa.common.master.app.forApp.*;
import ru.sberbank.bigdata.essa.common.master.app.recover.DataProductRecovery;
import ru.sberbank.bigdata.essa.common.master.app.forApp.DataProductState;
import ru.sberbank.bigdata.essa.common.slave.InPoint;

import java.util.Collections;

/**
 * Local stand for ESSA 3.0: the real DataProductRunner + the real VspcDataProductGenerator,
 * but CTL params come from here and every spark app (ETL / kit compressor / kit mover / finisher)
 * runs in this JVM through the real slave entry point InPoint.main instead of spark-submit to YARN.
 *
 * Called by Stand load (the SparkSession of that JVM is already active, ESSA's getOrCreate() returns it).
 * Hadoop/Hive settings: core-site.xml/hive-site.xml of the root (first on the classpath).
 */
public class Run30 {
    static final java.util.Map<String, Integer> TRIES = new java.util.HashMap<>();

    public static void run(String modeArg, final long loadingId) {
        final CommonConst.LoadMode mode = "INIT".equalsIgnoreCase(modeArg) ? CommonConst.LoadMode.INIT : CommonConst.LoadMode.INCR;
        final String wfName = mode == CommonConst.LoadMode.INIT ? "USL_VSPC_IDL_INIT" : "USL_VSPC_IDL";
        final SparkConfKeeper conf = new SparkConfKeeper("", "", "", "", "", "",
                "stand@LOCAL", "/tmp/none/", "default", "", "", "");
        final AppConfiguration app = new AppConfiguration(
                "ESSA",                      // log level ESSA -> SoutLoggerWrapper + EssaMetaWrapper (no Kafka / KAP meta)
                10, "", false, "", "vspc_idl", null,
                "http://dev.ctl.stub", 904000000L, "904000000-906999999",
                loadingId, wfName, "", mode,
                Integer.parseInt(System.getProperty("stand.threads", "1")),
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
                        // ESSA_RETRY_RERUN stand patch: failure injection to exercise DataProductRunner MAX_TRY=2.
                        // -Dstand.fail=<exactAppName>:<before|after>:<tryNo>[:<loadingId>]
                        // -Dstand.drop=<exactAppName>:<tryNo>:<db.table>[:<loadingId>]  (drop a table right before the app runs)
                        final String name = sparkApp.getName();
                        final int tryNo;
                        synchronized (TRIES) { Integer t = TRIES.get(name); tryNo = t == null ? 1 : t + 1; TRIES.put(name, tryNo); }
                        String drop = System.getProperty("stand.drop", "");
                        if (!drop.isEmpty()) {
                            String[] d = drop.split(":");
                            if (d[0].equals(name) && Integer.parseInt(d[1]) == tryNo && (d.length < 4 || Long.parseLong(d[3]) == loadingId)) {
                                System.out.println("[stand30] !!! drop table " + d[2] + " before app " + name + " try " + tryNo);
                                org.apache.spark.sql.SparkSession.builder().getOrCreate().sql("drop table if exists " + d[2]);
                            }
                        }
                        String fail = System.getProperty("stand.fail", "");
                        String when = null;
                        if (!fail.isEmpty()) {
                            String[] f = fail.split(":");
                            if (f[0].equals(name) && Integer.parseInt(f[2]) == tryNo && (f.length < 4 || Long.parseLong(f[3]) == loadingId)) when = f[1];
                        }
                        System.out.println("[stand30] >>> app " + name + " (" + sparkApp.getType() + ") try " + tryNo);
                        if ("before".equals(when)) {
                            System.out.println("[stand30] !!! injected failure BEFORE app " + name + " try " + tryNo);
                            throw new RuntimeException("stand injected failure before " + name);
                        }
                        try {
                            InPoint.main(sparkApp.downloadToStr());
                        } catch (RuntimeException e) {
                            System.out.println("[stand30] !!! app " + name + " try " + tryNo + " FAILED: " + e);
                            throw e;
                        }
                        if ("after".equals(when)) {
                            System.out.println("[stand30] !!! injected failure AFTER app " + name + " try " + tryNo + " (work done, reported failed)");
                            throw new RuntimeException("stand injected failure after " + name);
                        }
                        System.out.println("[stand30] <<< app " + name);
                    }
                };
            }
        };
        try {
            new DataProductRunner(gen, ctl, local).run();
        } catch (RuntimeException e) {
            System.out.println("[stand30] LOADING FAILED (DataProductRunner gave up after MAX_TRY): " + e);
            // ESSA_RETRY_RERUN: -Dstand.continueOnFail=true -> export the state left by the failed loading, go on with the chain
            if (!Boolean.getBoolean("stand.continueOnFail")) throw e;
            System.out.println("[stand30] continueOnFail: state left by the FAILED loading is exported, chain goes on");
        }
        System.out.println("[stand30] DONE");
    }
}
