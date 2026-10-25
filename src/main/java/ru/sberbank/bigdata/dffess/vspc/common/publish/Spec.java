package ru.sberbank.bigdata.dffess.vspc.common.publish;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class Spec {

    public enum Strategy {HUB, HIST_SNP, HIST_DIFF, HIST_MIX, LINK_HIST_DIFF, EMPTY}

    public final String code;
    public final String mask;
    public final Strategy strategy;
    public final String[] pk;
    public final boolean closeOld;

    private Spec(String code, String mask, Strategy strategy, String[] pk, boolean closeOld) {
        this.code = code;
        this.mask = mask;
        this.strategy = strategy;
        this.pk = pk;
        this.closeOld = closeOld;
    }

    /**
     * Ключ реестра = t.code (код трансформации, TablesName.*), а не имя hive-таблицы:
     * у div-хаба трансформер имеет код "div", хотя таблица называется "div_"
     */
    public static final Map<String, Spec> SPECS;

    static {
        Map<String, Spec> specs = new HashMap<>();
        specs.put("addr", new Spec("addr", "usl_vspc_idl.addr.hub", Strategy.HUB, new String[]{"addr_sk"}, false));
        specs.put("addr_h", new Spec("addr_h", "usl_vspc_idl.addr.sat=h", Strategy.HIST_DIFF, new String[]{"addr_sk"},
                false));
        specs.put("div", new Spec("div_", "usl_vspc_idl.div.hub", Strategy.HUB, new String[]{"div_sk"}, false));
        specs.put("div_div_relation", new Spec("div_div_relation", "usl_vspc_idl.div&div_relation.link",
                Strategy.LINK_HIST_DIFF, new String[]{"div_div_relation_sk"}, false));
        specs.put("div_h", new Spec("div_h", "usl_vspc_idl.div.sat=h", Strategy.HIST_MIX, new String[]{"div_sk"},
                false));
        specs.put("div_type", new Spec("div_type", "usl_vspc_idl.div.sat=type", Strategy.HIST_SNP,
                new String[]{"div_sk"}, true));
        specs.put("terr_div_type", new Spec("terr_div_type", "usl_vspc_idl.terr_div_type.hub", Strategy.HUB,
                new String[]{"terr_div_type_sk"}, false));
        specs.put("time_zone_rf", new Spec("time_zone_rf", "usl_vspc_idl.time_zone_rf.hub", Strategy.HUB,
                new String[]{"time_zone_rf_sk"}, false));
        SPECS = Collections.unmodifiableMap(specs);
    }
}