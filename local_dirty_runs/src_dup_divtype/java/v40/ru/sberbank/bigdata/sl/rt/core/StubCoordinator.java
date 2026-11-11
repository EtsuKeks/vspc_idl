package ru.sberbank.bigdata.sl.rt.core;

import ru.sberbank.bigdata.sl.rt.coordinator.Coordinator;
import ru.sberbank.bigdata.sl.rt.coordinator.CoordinatorInitResult;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.recover.accumulator.RtfwAccumulator;
import ru.sberbank.bigdata.sl.rt.recover.accumulator.RtfwFinalizator;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * CTL stub. Configured by system properties:
 *   stand.mode=INIT|INCR, stand.loadingId=<int>, stand.params=<path to .properties with CTL params>,
 *   stand.providerLoadingIds=<comma list> (answer of getLoadingIds).
 */
public class StubCoordinator extends Coordinator {
    public static final StubCoordinator INSTANCE = new StubCoordinator();

    @Override
    public AppContextLoadingParams loadParams(String... args) {
        final Properties p = new Properties();
        String file = System.getProperty("stand.params");
        if (file != null) {
            try (InputStreamReader r = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
                p.load(r);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        final App.LoadMode mode = "INIT".equalsIgnoreCase(System.getProperty("stand.mode", "INIT"))
                ? App.LoadMode.INITIAL : App.LoadMode.INCREMENT;
        final int loadingId = Integer.parseInt(System.getProperty("stand.loadingId", "1000"));
        return new AppContextLoadingParams() {
            public String getCtlUrl() { return "http://ctl.stub"; }
            public Integer getLoadingId() { return loadingId; }
            public String getInstance() { return p.getProperty("stand.instance", ""); }
            public String getProductCode() { return p.getProperty("stand.productCode", "usl_vspc_idl"); }
            protected Properties getParameters() { return p; }
            public App.LoadMode getMode() { return mode; }
            public String getLogicalEnv() { return p.getProperty("stand.logicalEnv", ""); }
            public String getShortProductCode() { return p.getProperty("stand.shortProductCode", "vspc_idl"); }
        };
    }

    @Override
    public CoordinatorInitResult init(AppContext ctx) {
        return new CoordinatorInitResult(false, Collections.emptySet());
    }

    @Override public void initTransformation(Transformation t) { }
    @Override public void startPublishing() { }
    @Override public void completeTransformation(Transformation t) { }
    @Override public void completeApplication(AppContext ctx) { }

    @Override
    public Set<Long> getLoadingIds(AppContext context, Recordset provider, Recordset... consumers) {
        Set<Long> ids = new HashSet<>();
        for (String s : System.getProperty("stand.providerLoadingIds", "1").split(",")) {
            if (!s.trim().isEmpty()) ids.add(Long.parseLong(s.trim()));
        }
        System.out.println("[stand] getLoadingIds(" + provider + ") -> " + ids);
        return ids;
    }

    @Override
    public Map<String, String> getSparkArgs(String code, Map<String, String> userOverrides) {
        return new HashMap<>();
    }

    @Override
    public void report(LogMessage message) {
        System.out.println("[stand] CTL report: " + message);
    }

    @Override
    public Map<RtfwAccumulator<?>, RtfwFinalizator> getApplicationAccumulatorsMap() {
        return new HashMap<>();
    }

    @Override
    public int startAndCompleteDummyWf(String logicalEnv) {
        return Integer.MAX_VALUE;
    }
}
