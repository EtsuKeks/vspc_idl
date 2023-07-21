package ru.sberbank.bigdata.dffess.vspc.tables.addrh;

import ru.sberbank.bigdata.dffess.vspc.constant.TablesName;
import ru.sberbank.bigdata.essa.common.templates.ThreadConfigProvider;
import ru.sberbank.hermes.core.dmbuilder.config.ThreadConfiguration;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static ru.sberbank.bigdata.essa.common.constant.CommonConst.PARTITION_MONTH_FORMAT;

public class ADDRHThreadConfigProvider extends ThreadConfigProvider {
    @Override
    public ThreadConfiguration provide(Map<String, String> cmdArgs) {
        ThreadConfiguration threadConfiguration=  getConfigProvider(TablesName.ADDRH);
        DateTimeFormatter formatterToStr = DateTimeFormatter.ofPattern(PARTITION_MONTH_FORMAT);
        String cutMonth = LocalDate.now().format(formatterToStr);
        threadConfiguration.setValue("mtp_Default_truncateIncFilterList", String.format("%s->p1month>=%s", TablesName.ADDRH, cutMonth));
        return threadConfiguration;
    }
}