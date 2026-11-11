package ru.sberbank.bigdata.dffess.vspc.compat;

import org.apache.spark.sql.Column;

import java.util.*;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.dffess.vspc.compat.CompatSupport.fail;

/** Таблицы витрины: ключи и колонки в порядке DDL 3.0 (порядок задаёт хэш 3.0). */
public final class CompatTables {

    public static final String START_DT = "start_dt";
    public static final String END_DT = "end_dt";
    public static final String P1MONTH = "p1month";
    /** Партиция открытых версий (end_dt = 9999-12-31). */
    public static final String OPEN_PARTITION = "999912";

    public enum Kind { PERIOD, HUB }

    public static final class Table {
        private final String code;
        private final Kind kind;
        private final List<String> pk;
        private final List<String> columns;

        Table(String code, Kind kind, List<String> pk, List<String> columns) {
            this.code = code;
            this.kind = kind;
            this.pk = pk;
            this.columns = columns;
        }

        public String code() { return code; }
        public Kind kind() { return kind; }
        public boolean isPeriod() { return kind == Kind.PERIOD; }
        public List<String> pk() { return pk; }

        /** PK без start_dt (для хабов = PK). */
        public List<String> bk() {
            List<String> bk = new ArrayList<>(pk);
            bk.remove(START_DT);
            return bk;
        }

        public Column bkNotNull() {
            return bk().stream().map(c -> col(c).isNotNull()).reduce(Column::and).get();
        }

        /** div_type — в 3.0 DataLoaderHistSnp: VANISH, правило возвращенца, маркер пустого INCR. */
        public boolean histSnp() { return "div_type".equals(code); }
        /** div_h: INIT ctl_validfrom := start_dt (DataLoaderHistMix). */
        public boolean initVfFromStart() { return "div_h".equals(code); }

        /** HUB_HASH_ESSA30 (E/slave/loading/SnpDataLoader.java:116-133, O/impl/VspcMetadataKeeper.java:19-25): xxhash64(concat) колонок DDL 3.0, NULL -> '', у period без start_dt/end_dt. */
        public Column hash30() {
            return xxhash64(concat(columns.stream()
                    .filter(c -> kind == Kind.HUB || !(START_DT.equals(c) || END_DT.equals(c)))
                    .map(c -> when(col(c).isNotNull(), col(c).cast(StringType)).otherwise(lit("")))
                    .toArray(Column[]::new)));
        }
    }

    private static final Map<String, Table> BY_CODE = new HashMap<>();
    private static final Map<String, Table> BY_MASK = new HashMap<>();

    static {
        add("addr", "usl_vspc_idl.addr.hub", Kind.HUB, l("addr_sk"),
                "addr_dp_code", "addr_sk", "city_name", "cntry_dp_code", "cntry_sk", "deleted_flag", "dirty_flag",
                "district_name", "fias_code", "fias_code_addr_dp_code", "fias_code_addr_sk", "house_num", "idx_num",
                "locty_name", "okato2_dp_code", "okato2_sk", "reg_addr_name", "rus_fed_subj_dp_code", "rus_fed_subj_sk",
                "sid", "src_txt", "street_name", "type_code");
        add("addr_h", "usl_vspc_idl.addr.sat=h", Kind.PERIOD, l("addr_sk", START_DT),
                "addr_dp_code", "addr_sk", "address_okato2_dp_code", "address_okato2_sk", "address_oktmo2_dp_code",
                "address_oktmo2_sk", END_DT, "standard_addr_txt", START_DT, "type_code");
        add("div_", "usl_vspc_idl.div.hub", Kind.HUB, l("div_sk"),
                "code", "deleted_flag", "dirty_flag", "div_close_dt", "div_dp_code", "div_open_dt", "div_sk",
                "div_time_zone_time_zone_rf_dp_code", "div_time_zone_time_zone_rf_sk", "full_reg_num",
                "legal_addr_dp_code", "legal_addr_sk", "parent_div_dp_code", "parent_div_sk", "reg_ord",
                "short_div_code", "sid", "subbranch_code");
        add("div_div_relation", "usl_vspc_idl.div&div_relation.link", Kind.PERIOD, l(START_DT, "div_div_relation_sk"),
                "deleted_flag", "div_div_relation_dp_code", "div_div_relation_sk", END_DT, "sid", "source_div_dp_code",
                "source_div_sk", START_DT, "successor_div_dp_code", "successor_div_sk");
        add("div_h", "usl_vspc_idl.div.sat=h", Kind.PERIOD, l("div_sk", START_DT),
                "disabled_access_equipment_flag", "div_dp_code", "div_name", "div_sk", END_DT, "fact_addr_dp_code",
                "fact_addr_sk", "pay_sys_prtcpnt_dp_code", "pay_sys_prtcpnt_sk", START_DT);
        add("div_type", "usl_vspc_idl.div.sat=type", Kind.PERIOD, l("div_sk", START_DT),
                "div_dp_code", "div_sk", "div_type_terr_div_type_dp_code", "div_type_terr_div_type_sk", END_DT, START_DT);
        add("terr_div_type", "usl_vspc_idl.terr_div_type.hub", Kind.HUB, l("terr_div_type_sk"),
                "deleted_flag", "dirty_flag", "div_type_code", "sid", "terr_div_type_dp_code", "terr_div_type_sk");
        add("time_zone_rf", "usl_vspc_idl.time_zone_rf.hub", Kind.HUB, l("time_zone_rf_sk"),
                "deleted_flag", "dirty_flag", "sid", "time_zone_abbr_code", "time_zone_code", "time_zone_msk_shift_val",
                "time_zone_name", "time_zone_rf_dp_code", "time_zone_rf_sk", "time_zone_wrld_shift_val");
    }

    private CompatTables() {
    }

    private static void add(String code, String mask, Kind kind, List<String> pk, String... columns) {
        Table t = new Table(code, kind, pk, l(columns));
        BY_CODE.put(code, t);
        BY_MASK.put(mask, t);
    }

    private static List<String> l(String... s) {
        return Arrays.asList(s);
    }

    /** Неизвестная маска — отказ: публикация мимо слоя совместимости. */
    public static Table byMask(String mask) {
        return require(BY_MASK.get(mask), mask);
    }

    public static Table byCode(String code) {
        return require(BY_CODE.get(code), code);
    }

    private static Table require(Table t, String name) {
        if (t == null) {
            throw fail(name + " не объявлена в CompatTables");
        }
        return t;
    }
}
