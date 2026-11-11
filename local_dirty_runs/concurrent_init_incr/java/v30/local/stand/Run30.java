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
                        System.out.println("[stand30] >>> app " + sparkApp.getName() + " (" + sparkApp.getType() + ")");
                        InPoint.main(sparkApp.downloadToStr());
                        System.out.println("[stand30] <<< app " + sparkApp.getName());
                    }
                };
            }
        };
        new DataProductRunner(gen, ctl, local).run();
        System.out.println("[stand30] DONE");
    }
}
