package ru.sberbank.bigdata.dffess.vspc.common;

import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.logger.core.LogMessage;
import ru.sberbank.bigdata.sl.rt.logger.core.LogType;

public class Logger {
    org.apache.log4j.Logger log4jLogger;
    App app;

    public Logger(App app, Class clazz) {
        this.log4jLogger = org.apache.log4j.Logger.getLogger(clazz);
        this.app = app;
    }

    public void setLog4jLogger(org.apache.log4j.Logger log4jLogger) {
        this.log4jLogger = log4jLogger;
    }

    public void setApp(App app) {
        this.app = app;
    }

    public static LogMessage info(String title) {
        return new LogMessage("", LogType.TECH_INFO, title);
    }

    public static LogMessage error(String title) {
        return new LogMessage("", LogType.TECH_ERROR, title);
    }

    public static LogMessage debug(String title) {
        return new LogMessage("", LogType.TECH_DEBUG, title);
    }

    public void log(LogMessage message, boolean kibanaPosting) {
        if (kibanaPosting && app != null) {
            app.log(message);
        }
        if (log4jLogger != null) {
            String title = message.getTitle();
            String messageSubtype = message.getSubTypeId();
            if (LogType.TECH_INFO.getSubTypeId().equals(messageSubtype)) {
                log4jLogger.info(title);
            } else if (LogType.TECH_ERROR.getSubTypeId().equals(messageSubtype)) {
                log4jLogger.error(title);
            } else if (LogType.TECH_DEBUG.getSubTypeId().equals(messageSubtype)) {
                log4jLogger.debug(title);
            } else {
                System.out.println(title);
            }
        }
    }

    public void log(LogMessage message) {
        log(message, false);
    }

    public void logInfo(String title) {
        logInfo(title, false);
    }

    public void logInfo(String title, boolean kibanaPosting) {
        log(info(title), kibanaPosting);
    }

    public void logError(String title) {
        logError(title, false);
    }

    public void logError(String title, boolean kibanaPosting) {
        log(error(title), kibanaPosting);
    }

    public void logDebug(String title) {
        logDebug(title, false);
    }

    public void logDebug(String title, boolean kibanaPosting) {
        log(debug(title));
    }

}
