package ru.sberbank.bigdata.dffess.vspc;

import org.apache.commons.lang3.exception.ExceptionUtils;
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
import ru.sberbank.bigdata.sl.rt.util.RtfwLogger;
import ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters;

import static ru.sberbank.bigdata.sl.rt.core.CoreLogMessageBuilder.CORE_021;

public class MainClass {

    public static void main(String... args) {
        try {
            App app = App.create("main", args).autoSparkSession(true).build();

            if ("true".equals(app.ctx().getProperty(ApplicationParameters.DEBUG_SK_OFF.getParamName(), ""))) {
                throw new IllegalStateException("Запуск без ССК запрещён: debug_sk_off=true");
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

            app.run();

        } catch (Exception e) {
            LogMessage logMessage = CORE_021(ExceptionUtils.getStackTrace(e));
            RtfwLogger.log(logMessage);
            throw new AppException(logMessage);
        }
    }
}