package ru.sberbank.bigdata.dffess.vspc.compat.guard;

import ru.sberbank.bigdata.dffess.vspc.compat.CompatHooks;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.publisher.PublisherImpl;
import ru.sberbank.bigdata.sl.rt.publisher.strategy.process.ProcessingStage;
import ru.sberbank.bigdata.sl.rt.core.exceptions.TransformationException;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;

import java.io.InputStream;
import java.security.CodeSource;
import java.util.Properties;

import static ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters.MAX_INCREMENT_DEPTH_DAYS;
import static ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters.STARTING_BUSINESS_DATE;

// Проверки перед запуском: окружение такое, на каком проверены поправки ESSA, — иначе отказ до любой записи.
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

    // Проверяем что наши compat правки живут в теневом ProcessingStage нашего jar.
    private static void checkShadowStage() {
        String ps = location(ProcessingStage.class);
        if (ps == null || !ps.equals(location(CompatHooks.class))) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, String.format(
                "ProcessingStage загружен из %s, а не из %s: вендорский класс без VSPC-COMPAT "
                + "снимает слой совместимости с ESSA", ps, location(CompatHooks.class))));
        }
    }

    private static String location(Class<?> c) {
        CodeSource cs = c.getProtectionDomain().getCodeSource();
        return cs == null || cs.getLocation() == null ? null : cs.getLocation().toString();
    }

    // Проверяем параметры CTL, которые ESSA игнорировала, а RT-FW исполняет.
    private static void checkParams(AppContext ctx) {
        if (ctx.getLoadMode() == App.LoadMode.INITIAL && !ctx.isDiffClearedByInitLoad()) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR,
                "init_diff_flag должен быть false: в ESSA INIT всегда очищает diff"));
        }
        String sbd = ctx.getProperty(STARTING_BUSINESS_DATE.getParamName(), "");
        String midd = ctx.getProperty(MAX_INCREMENT_DEPTH_DAYS.getParamName(), "");
        if (!sbd.isEmpty() || !midd.isEmpty()) {
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, String.format(
                "starting_business_date='%s', max_increment_depth_days='%s': витрина их не использует — оставьте "
                + "пустыми, иначе RT-FW калькулятор таблиц обрежет вход и сдвинет даты версий относительно ESSA",
                sbd, midd)));
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
            throw new TransformationException(new LogMessage("", LogType.TECH_ERROR, String.format(
                    "%s версии %s, а поправки ESSA проверены на %s. Перед сменой версии: перенести вставки "
                    + "VSPC-COMPAT в новый вендорский ProcessingStage, сверить поля калькуляторов, которые читает "
                    + "CompatHooks, прогнать локальный стенд и обновить версию в StartupSelfCheck",
                    artifact, actual, expected)));
        }
    }
}
