package ru.sberbank.bigdata.dffess.vspc;

import org.apache.commons.lang3.exception.ExceptionUtils;
import ru.sberbank.bigdata.dffess.vspc.compat.guard.StartupSelfCheck;
import ru.sberbank.bigdata.dffess.vspc.transformations.AddrHTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.AddrTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.ChgDivHTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.DivDivRelationTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.DivHTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.DivTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.DivTypeTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.TerrDivTypeTransformation;
import ru.sberbank.bigdata.dffess.vspc.transformations.TimeZoneTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.Transformation;
import ru.sberbank.bigdata.sl.rt.core.exceptions.AppException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;
import ru.sberbank.bigdata.sl.rt.util.RtfwLogger;
import ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters;

import static ru.sberbank.bigdata.sl.rt.core.CoreLogMessageBuilder.CORE_021;
import static ru.sberbank.bigdata.sl.rt.util.Constants.RTFW_SPAWNED_APP_CODE_ENV;

public class MainClass {

    public static void main(String... args) {
        try {
            // под RtfwLauncher (StartLauncher.sh) терминальное приложение — лаунчер
            App app = App.create("main", args)
                    .terminator(System.getenv(RTFW_SPAWNED_APP_CODE_ENV) == null)
                    .autoSparkSession(true).build();
            StartupSelfCheck.verifyOrDie(app);
            // 3.0 SparkAppEnvConfigurationCreator.java:20: parquet gzip
            app.getSparkSession().conf().set("spark.sql.parquet.compression.codec", "gzip");
            // не из 3.0 (нагрузка, на строки не влияет): fast stats не собирать по всем партициям цели при recoverPartitions
            app.getSparkSession().conf().set("spark.sql.hive.gatherFastStats", "false");

            String debugSkOff = app.ctx().getProperty(ApplicationParameters.DEBUG_SK_OFF.getParamName(), "");
            if (!debugSkOff.isEmpty() && !"false".equalsIgnoreCase(debugSkOff)) {
                RtfwLogger.log(new LogMessage("", LogType.TECH_WARN,
                                  "ССК отключён параметром debug_sk_off=" + debugSkOff));
            }

            Transformation addr = new AddrTransformation(app);
            Transformation addrh = new AddrHTransformation(app);
            Transformation chgdivh = new ChgDivHTransformation(app);
            Transformation divdivrelation = new DivDivRelationTransformation(app);
            Transformation divh = new DivHTransformation(app);
            Transformation div = new DivTransformation(app);
            Transformation divtype = new DivTypeTransformation(app);
            Transformation terrdivtype = new TerrDivTypeTransformation(app);
            Transformation timezone = new TimeZoneTransformation(app);

            addr.addPredecessors(chgdivh);
            // 3.0: все ETL ждут chg_div_h
            timezone.addPredecessors(chgdivh);
            terrdivtype.addPredecessors(chgdivh);
            addrh.addPredecessors(addr);
            div.addPredecessors(timezone, addrh);
            divh.addPredecessors(div);
            divtype.addPredecessors(divh, terrdivtype);
            divdivrelation.addPredecessors(divh);

            app.addTransformation(addr);
            app.addTransformation(addrh);
            app.addTransformation(chgdivh);
            app.addTransformation(div);
            app.addTransformation(divh);
            app.addTransformation(divtype);
            app.addTransformation(divdivrelation);
            app.addTransformation(terrdivtype);
            app.addTransformation(timezone);

            // 3.0: lock КАП.МЕТА до старта ETL (DataProductRunner.java:69-71)
            app.startPublishing();
            app.run();

        } catch (Exception e) {
            LogMessage logMessage = CORE_021(ExceptionUtils.getStackTrace(e));
            RtfwLogger.log(logMessage);
            throw new AppException(logMessage);
        }
    }
}