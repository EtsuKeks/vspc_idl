package ru.sberbank.bigdata.dffess.vspc.tables.div;

import ru.sberbank.bigdata.dffess.vspc.constant.TablesName;
import ru.sberbank.bigdata.essa.common.templates.ThreadConfigProvider;
import ru.sberbank.hermes.core.dmbuilder.config.ThreadConfiguration;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static ru.sberbank.bigdata.essa.common.constant.CommonConst.PARTITION_MONTH_FORMAT;

public class DIVThreadConfigProvider extends ThreadConfigProvider {
    @Override
    public ThreadConfiguration provide(Map<String, String> cmdArgs) {
        return getConfigProvider(TablesName.DIV);
    }
}