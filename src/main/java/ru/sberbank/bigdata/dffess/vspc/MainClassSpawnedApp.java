package ru.sberbank.bigdata.dffess.vspc;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import ru.sberbank.bigdata.dffess.vspc.transformations.AddrTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.AddrHTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.ChgDivHTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.DivDivRelationTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.DivTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.DivHTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.DivTypeTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.TerrDivTypeTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.TimeZoneTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.SpawnedApp;
import ru.sberbank.bigdata.sl.rt.core.exceptions.AppException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.util.RtfwLogger;

import java.util.HashMap;

import static ru.sberbank.bigdata.sl.rt.core.CoreLogMessageBuilder.CORE_021;

public class MainClassSpawnedApp {

    public static void main(String... args) {
        try {
            String RTFW_KERBEROS_TICKET_CACHE_FILE = System.getenv("RTFW_KERBEROS_TICKET_CACHE_FILE");
            HashMap<String, String> CUSTOM_SPARK_PARAMS = getCustomSparkParams(RTFW_KERBEROS_TICKET_CACHE_FILE);

            App app = App.createJvmApp("main", args)
                    .setUserSparkSessionParams(CUSTOM_SPARK_PARAMS)
                    .build();

            SpawnedApp addr = app.createSpawnedApp(AddrTransformation.CODE, AddrTransformation.class, AddrTransformation.SETS);
            SpawnedApp addrh = app.createSpawnedApp(AddrHTransformation.CODE, AddrHTransformation.class, AddrHTransformation.SETS);
            SpawnedApp chgdivh = app.createSpawnedApp(ChgDivHTransformation.CODE, ChgDivHTransformation.class);
            SpawnedApp div = app.createSpawnedApp(DivTransformation.CODE, DivTransformation.class, DivTransformation.SETS);
            SpawnedApp divh = app.createSpawnedApp(DivHTransformation.CODE, DivHTransformation.class, DivHTransformation.SETS);
            SpawnedApp divtype = app.createSpawnedApp(DivTypeTransformation.CODE, DivTypeTransformation.class, DivTypeTransformation.SETS);
            SpawnedApp divdivrelation =  app.createSpawnedApp(DivDivRelationTransformation.CODE, DivDivRelationTransformation.class, DivDivRelationTransformation.SETS);
            SpawnedApp terrdivtype = app.createSpawnedApp(TerrDivTypeTransformation.CODE, TerrDivTypeTransformation.class, TerrDivTypeTransformation.SETS);
            SpawnedApp timezone = app.createSpawnedApp(TimeZoneTransformation.PU_CODE, TimeZoneTransformation.class, TimeZoneTransformation.SETS);

            addr.addPredecessors(chgdivh);
            addrh.addPredecessors(addr);
            div.addPredecessors(timezone, addrh);
            divh.addPredecessors(div);
            divtype.addPredecessors(divh, terrdivtype);
            divdivrelation.addPredecessors(divh);

            app.run();

        } catch (Exception e) {
            LogMessage logMessage = CORE_021(ExceptionUtils.getStackTrace(e));
            RtfwLogger.log(logMessage);
            throw new AppException(logMessage);
        }
    }

    private static @NotNull HashMap<String, String> getCustomSparkParams(String RTFW_KERBEROS_TICKET_CACHE_FILE) {
        HashMap<String, String> CUSTOM_SPARK_PARAMS = new HashMap<>();
        // spark.yarn.access.hadoopFileSystems НЕ переопределяем — RtfwLauncher авто-рассчитывает его из metadata
        CUSTOM_SPARK_PARAMS.put("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=/etc/zookeeper/conf/zookeeper_client_jaas.conf");
        CUSTOM_SPARK_PARAMS.put("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=/etc/zookeeper/conf/zookeeper_client_jaas.conf");
        if (RTFW_KERBEROS_TICKET_CACHE_FILE != null && !RTFW_KERBEROS_TICKET_CACHE_FILE.isEmpty()) {
            CUSTOM_SPARK_PARAMS.put("spark.yarn.appMasterEnv.KRB5CCNAME", RTFW_KERBEROS_TICKET_CACHE_FILE);
            CUSTOM_SPARK_PARAMS.put("spark.executorEnv.KRB5CCNAME", RTFW_KERBEROS_TICKET_CACHE_FILE);
            CUSTOM_SPARK_PARAMS.put("spark.driverEnv.KRB5CCNAME", RTFW_KERBEROS_TICKET_CACHE_FILE);
        }
        return CUSTOM_SPARK_PARAMS;
    }
}