package ru.sberbank.bigdata.dffess.vspc.tables.divh;

import ru.sberbank.bigdata.dffess.vspc.constant.TablesName;
import ru.sberbank.bigdata.essa.common.templates.ThreadConfigProvider;
import ru.sberbank.hermes.core.dmbuilder.config.ThreadConfiguration;

import java.util.Map;

public class DivhThreadConfigProvider extends ThreadConfigProvider {
    @Override
    public ThreadConfiguration provide(Map<String, String> cmdArgs) {
        return getConfigProvider(TablesName.DIV_H);
    }
}