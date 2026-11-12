package ru.sberbank.bigdata.dffess.vspc.constant;

import ru.sberbank.bigdata.dffess.vspc.compat.CompatTables.EssaLoader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;

/**
 * Загрузчик ESSA-версии витрины у каждой публикуемой таблицы.
 * По нему слой совместимости повторяет поведение ESSA при публикации. Ключ — код трансформации.
 */
public final class EssaLoaders {

    public static final Map<String, EssaLoader> BY_TRANSFORMATION;

    static {
        Map<String, EssaLoader> m = new HashMap<>();
        m.put(ADDR, EssaLoader.HUB_DIFF);
        m.put(DIV, EssaLoader.HUB_DIFF);
        m.put(TERRDIVTYPE, EssaLoader.HUB_DIFF);
        m.put(TIMEZONE, EssaLoader.HUB_DIFF);
        m.put(ADDRH, EssaLoader.HIST_DIFF);
        m.put(DIVH, EssaLoader.HIST_MIX);
        m.put(DIVTYPE, EssaLoader.HIST_SNP);
        m.put(DIVDIVRELATION, EssaLoader.LINK_HIST_DIFF);
        BY_TRANSFORMATION = Collections.unmodifiableMap(m);
    }

    private EssaLoaders() {
    }
}
