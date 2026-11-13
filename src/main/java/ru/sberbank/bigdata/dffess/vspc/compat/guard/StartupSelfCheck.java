package ru.sberbank.bigdata.dffess.vspc.compat.guard;

import ru.sberbank.bigdata.dffess.vspc.compat.CompatHooks;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherImpl;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.ProcessingStage;

import java.io.InputStream;
import java.security.CodeSource;
import java.util.Properties;

import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.fail;
import static ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters.MAX_INCREMENT_DEPTH_DAYS;
import static ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters.STARTING_BUSINESS_DATE;

/**
 * GUARD_RTFW_VERSION: слой совместимости опирается на внутреннее поведение закреплённых версий RT-FW.
 * Перед сменой версий:
 * <ol>
 *   <li>ProcessingStage = новый вендорский текст + те же три блока VSPC-COMPAT;</li>
 *   <li>CalculatorFactory: те же классы калькуляторов и поля fullSlice/reInit/collapsePeriods/physicalDeletion/loadingId;</li>
 *   <li>PublisherImpl.runIncr/runInit: тот же порядок шагов; контракты ChangesCalculator, MergeCalculator, SnapshotChanges;</li>
 *   <li>вендорские строки, на которые ссылаются комментарии compat (V/...);</li>
 *   <li>локальный стенд паритета (local/stand.sh run all) зелёный.</li>
 * </ol>
 */
public final class StartupSelfCheck {

    private static final String PUBLISHER_VERSION = "2.16.1";
    private static final String CORE_VERSION = "3.16.0";

    private StartupSelfCheck() {
    }

    public static void verifyOrDie(App app) {
        ClassLoader cl = PublisherImpl.class.getClassLoader();
        check(cl, "rt-fw-publisher", PUBLISHER_VERSION);
        check(cl, "rt-fw-core", CORE_VERSION);
        checkParams(app.ctx());
        checkShadowStage();
    }

    // DV-35: поправки ESSA живут в теневом ProcessingStage нашего jar (вендорский исключён, pom.xml:155-161)
    private static void checkShadowStage() {
        String ps = location(ProcessingStage.class);
        if (ps == null || !ps.equals(location(CompatHooks.class))) {
            throw fail(String.format("ProcessingStage загружен из %s, а не из %s: вендорский класс без VSPC-COMPAT "
                    + "снимает слой совместимости с ESSA", ps, location(CompatHooks.class)));
        }
    }

    private static String location(Class<?> c) {
        CodeSource cs = c.getProtectionDomain().getCodeSource();
        return cs == null || cs.getLocation() == null ? null : cs.getLocation().toString();
    }

    /** DV-66, DV-16: параметры CTL, которые ESSA игнорировала, а RT-FW исполняет, — отказ до любой записи. */
    private static void checkParams(AppContext ctx) {
        if (ctx.getLoadMode() == App.LoadMode.INITIAL && !ctx.isDiffClearedByInitLoad()) {
            throw fail("init_diff_flag должен быть false: в ESSA INIT всегда очищает diff");
        }
        String sbd = ctx.getProperty(STARTING_BUSINESS_DATE.getParamName(), "");
        String midd = ctx.getProperty(MAX_INCREMENT_DEPTH_DAYS.getParamName(), "");
        if (!sbd.isEmpty() || (!midd.isEmpty() && Long.parseLong(midd) < 1)) {
            throw fail(String.format("starting_business_date='%s', max_increment_depth_days='%s': в ESSA не действовали, "
                    + "в RT-FW сдвигают даты версий. Нужно starting_business_date='' и max_increment_depth_days>=1", sbd, midd));
        }
    }

    private static void check(ClassLoader cl, String artifact, String expected) {
        String res = "META-INF/maven/ru.sberbank.bigdata.sl.rt/" + artifact + "/pom.properties";
        String actual;
        try (InputStream in = cl.getResourceAsStream(res)) {
            Properties p = new Properties();
            if (in != null) {
                p.load(in);
            }
            actual = p.getProperty("version", "не найдена");
        } catch (Exception e) {
            actual = "ошибка чтения: " + e;
        }
        if (!expected.equals(actual)) {
            throw fail(String.format("%s версии %s, а слой совместимости с ESSA проверен на %s: пройдите чек-лист "
                    + "compat/guard/StartupSelfCheck и только потом меняйте закреплённую версию", artifact, actual, expected));
        }
    }
}
