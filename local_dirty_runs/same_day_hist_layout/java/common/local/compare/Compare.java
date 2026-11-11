package local.compare;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 3.0 vs 4.0 comparison of the 16 vspc_idl target tables, per load, with case attribution.
 *
 * <pre>
 *   Compare &lt;runDir&gt; &lt;chain&gt; &lt;load,load,...&gt; [--mask table.col,...]
 * </pre>
 * Reads   runDir/exp30/&lt;load&gt;, runDir/exp40/&lt;load&gt; (parquet exports of stand.sh export; a missing side is
 *         reported, the present side is still dumped), runDir/synth/&lt;chain&gt;/timeline.tsv and keymap.tsv.
 * Writes  runDir/cmp/&lt;load&gt;.txt          human report: table verdicts, every mismatching row with its case
 *         runDir/cmp/mismatches.tsv       one line per mismatching row (all loads)
 *         runDir/cmp/tables.tsv           per load and table: rows30, rows40, only30, only40, verdict
 *         runDir/dump/&lt;30|40&gt;/&lt;load&gt;/&lt;layer&gt;.&lt;table&gt;.tsv   normalized rows with _case/_key (input of synth/check.py)
 *
 * Normalization (both sides identically, before exceptAll):
 *   ctl_validfrom -> class: NULL | START (= start_dt 00:00) | RUN@Lk (within 3h after the fake time of load Lk)
 *                    | TS:&lt;value&gt; (anything else). 3.0 and 4.0 stamp different instants inside a run, the class is
 *                    what has to match.
 *   ctl_loading   -> compared exactly (both versions get the same loadingId per load); shown as the load label.
 *   masked columns are dropped. Everything else is compared exactly, as strings (NULL = \N), multiset semantics
 *   (Dataset.exceptAll both ways).
 * Attribution: *_sk values are translated to natural keys (sid) using the hubs' (sk, sid) pairs of BOTH exports;
 *   the case is the first [CEX]NN id in the natural key of the row's PK, else keymap.tsv (keys like KALT, NULL).
 */
public class Compare {

    static final String[] TABLES = {"addr", "addr_h", "div_", "div_h", "div_type", "div_div_relation", "terr_div_type", "time_zone_rf"};
    static final Map<String, String> PK = new HashMap<>();
    static final Map<String, String> ENTITY_OF_COL = new HashMap<>();
    static final Map<String, String[]> HUB_OF_ENTITY = new LinkedHashMap<>();

    static {
        PK.put("addr", "addr_sk");
        PK.put("addr_h", "addr_sk");
        PK.put("div_", "div_sk");
        PK.put("div_h", "div_sk");
        PK.put("div_type", "div_sk");
        PK.put("div_div_relation", "div_div_relation_sk");
        PK.put("terr_div_type", "terr_div_type_sk");
        PK.put("time_zone_rf", "time_zone_rf_sk");
        for (String c : new String[]{"div_sk", "parent_div_sk", "source_div_sk", "successor_div_sk"}) ENTITY_OF_COL.put(c, "div");
        for (String c : new String[]{"addr_sk", "legal_addr_sk", "fact_addr_sk"}) ENTITY_OF_COL.put(c, "addr");
        for (String c : new String[]{"terr_div_type_sk", "div_type_terr_div_type_sk"}) ENTITY_OF_COL.put(c, "terr");
        for (String c : new String[]{"time_zone_rf_sk", "div_time_zone_time_zone_rf_sk"}) ENTITY_OF_COL.put(c, "tz");
        ENTITY_OF_COL.put("div_div_relation_sk", "link");
        HUB_OF_ENTITY.put("div", new String[]{"div_", "div_sk"});
        HUB_OF_ENTITY.put("addr", new String[]{"addr", "addr_sk"});
        HUB_OF_ENTITY.put("terr", new String[]{"terr_div_type", "terr_div_type_sk"});
        HUB_OF_ENTITY.put("tz", new String[]{"time_zone_rf", "time_zone_rf_sk"});
        HUB_OF_ENTITY.put("link", new String[]{"div_div_relation", "div_div_relation_sk"});
    }

    static final String NUL = "\\N";
    static final Pattern CASE_ID = Pattern.compile("([A-Z]\\d{2})");
    static final DateTimeFormatter FT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    final SparkSession spark;
    final File run;
    final List<String[]> timeline = new ArrayList<>();          // label, mode, faketime, loadingId
    final Map<String, String> keyOwner = new HashMap<>();        // sid (or \N) -> case
    final Set<String> masks = new HashSet<>();
    PrintWriter mismatches, tables;

    Compare(SparkSession spark, File run, String chain, String maskArg) throws IOException {
        this.spark = spark;
        this.run = run;
        for (String l : Files.readAllLines(new File(run, "synth/" + chain + "/timeline.tsv").toPath(), StandardCharsets.UTF_8)) {
            if (!l.startsWith("label\t") && !l.trim().isEmpty()) timeline.add(l.split("\t"));
        }
        for (String l : Files.readAllLines(new File(run, "synth/" + chain + "/keymap.tsv").toPath(), StandardCharsets.UTF_8)) {
            String[] p = l.split("\t");
            if (!l.startsWith("sid\t") && p.length >= 2) keyOwner.put(p[0], p[1]);
        }
        if (maskArg != null) for (String m : maskArg.split(",")) if (!m.trim().isEmpty()) masks.add(m.trim());
    }

    // ------------------------------------------------------------------------------------------ normalization

    String loadLabel(Object ctlLoading) {
        if (ctlLoading == null) return NUL;
        for (String[] t : timeline) if (t[3].equals(String.valueOf(ctlLoading))) return t[0];
        return String.valueOf(ctlLoading);
    }

    String vfClass(Object vf, Object startDt) {
        if (vf == null) return "NULL";
        LocalDateTime ts = ((Timestamp) vf).toLocalDateTime();
        if (startDt != null && ts.equals(((java.sql.Date) startDt).toLocalDate().atStartOfDay())) return "START";
        for (String[] t : timeline) {
            LocalDateTime f = LocalDateTime.parse(t[2], FT);
            if (!ts.isBefore(f) && ts.isBefore(f.plusHours(3))) return "RUN@" + t[0];
        }
        return "TS:" + ts;
    }

    static String str(Object v) {
        if (v == null) return NUL;
        if (v instanceof Timestamp) return ((Timestamp) v).toLocalDateTime().toString().replace('T', ' ');
        return String.valueOf(v);
    }

    static String tsvEsc(String s) {
        return s.replace("\t", "\\t").replace("\n", "\\n");
    }

    /** One export side of one table, normalized: raw string values for comparison + display values. */
    static class Side {
        List<String> cols = new ArrayList<>();       // compared columns (sorted)
        List<String[]> raw = new ArrayList<>();      // compared values
        List<String[]> shown = new ArrayList<>();    // display values (sk -> natural key, ctl_loading -> label)
        List<String> keys = new ArrayList<>();       // natural key of PK
        List<String> cases = new ArrayList<>();
        boolean present;
    }

    /** Parquet export of one table, read once per load (tables are small: collected to the driver). */
    static class Raw {
        String[] columns;
        List<Row> rows;
    }

    final Map<String, Raw> cache = new HashMap<>();

    Raw read(String dir, String layer, String table) {
        if (dir == null) return null;
        String k = dir + "/" + layer + "_" + table;
        if (!cache.containsKey(k)) {
            File f = new File(k);
            Raw r = null;
            if (f.exists()) {
                Dataset<Row> d = spark.read().parquet("file://" + f.getAbsolutePath());
                r = new Raw();
                r.columns = d.columns();
                r.rows = d.collectAsList();
            }
            cache.put(k, r);
        }
        return cache.get(k);
    }

    Side load(String dir, String layer, String table, Map<String, Map<Long, String>> sid) {
        Side s = new Side();
        Raw d = read(dir, layer, table);
        if (d == null) return s;
        s.present = true;
        List<String> all = new ArrayList<>(Arrays.asList(d.columns));
        Collections.sort(all);
        for (String c : all) if (!masks.contains(table + "." + c) && !masks.contains(c)) s.cols.add(c);
        String pk = PK.get(table);
        for (Row r : d.rows) {
            String[] raw = new String[s.cols.size()], shown = new String[s.cols.size()];
            Object start = all.contains("start_dt") ? r.getAs("start_dt") : null;
            for (int i = 0; i < s.cols.size(); i++) {
                String c = s.cols.get(i);
                Object v = r.getAs(c);
                if (c.equals("ctl_validfrom")) {
                    raw[i] = shown[i] = vfClass(v, start);
                } else if (c.equals("ctl_loading")) {
                    raw[i] = str(v);
                    shown[i] = loadLabel(v);
                } else if (ENTITY_OF_COL.containsKey(c) && v != null) {
                    raw[i] = str(v);
                    String n = sid.get(ENTITY_OF_COL.get(c)).get(((Number) v).longValue());
                    shown[i] = n != null ? n : "#" + v;
                } else {
                    raw[i] = shown[i] = str(v);
                }
            }
            Object pkv = r.getAs(pk);
            String key = pkv == null ? NUL : sid.get(ENTITY_OF_COL.get(pk)).getOrDefault(((Number) pkv).longValue(), "#" + pkv);
            s.raw.add(raw);
            s.shown.add(shown);
            s.keys.add(key);
            s.cases.add(caseOf(key));
        }
        return s;
    }

    String caseOf(String key) {
        if (key.equals(NUL)) return keyOwner.getOrDefault(NUL, "?");
        Matcher m = CASE_ID.matcher(key);
        if (m.find()) return m.group(1);
        return keyOwner.getOrDefault(key, "?");
    }

    /** sk -> natural key for every entity, from the hubs (snp and diff) of both exports. */
    Map<String, Map<Long, String>> sidMaps(String... dirs) {
        Map<String, Map<Long, String>> res = new HashMap<>();
        for (Map.Entry<String, String[]> e : HUB_OF_ENTITY.entrySet()) {
            Map<Long, String> m = new HashMap<>();
            for (String dir : dirs) {
                if (dir == null) continue;
                for (String layer : new String[]{"snp", "diff"}) {
                    Raw d = read(dir, layer, e.getValue()[0]);
                    if (d == null) continue;
                    for (Row r : d.rows) {
                        Object sk = r.getAs(e.getValue()[1]), sd = r.getAs("sid");
                        if (sk != null) m.put(((Number) sk).longValue(), sd == null ? NUL : sd.toString());
                    }
                }
            }
            res.put(e.getKey(), m);
        }
        return res;
    }

    // ------------------------------------------------------------------------------------------ comparison

    static String rowKey(String[] v, List<String> cols) {
        return String.join("\u0001", v);
    }

    /** Multiset difference a - b with Spark exceptAll on all compared columns (as strings). */
    List<String> exceptAll(List<String[]> a, List<String[]> b, List<String> cols) {
        List<StructField> fs = new ArrayList<>();
        for (String c : cols) fs.add(DataTypes.createStructField(c, DataTypes.StringType, false));
        StructType st = DataTypes.createStructType(fs);
        List<Row> ra = new ArrayList<>(), rb = new ArrayList<>();
        for (String[] v : a) ra.add(RowFactory.create((Object[]) v));
        for (String[] v : b) rb.add(RowFactory.create((Object[]) v));
        List<String> out = new ArrayList<>();
        for (Row r : spark.createDataFrame(ra, st).exceptAll(spark.createDataFrame(rb, st)).collectAsList()) {
            String[] v = new String[cols.size()];
            for (int i = 0; i < v.length; i++) v[i] = r.getString(i);
            out.add(rowKey(v, cols));
        }
        return out;
    }

    /** Rows of side s whose value tuple is in the multiset 'want' (consumes it). */
    static List<Integer> pick(Side s, List<String> want) {
        Map<String, Integer> cnt = new HashMap<>();
        for (String w : want) cnt.merge(w, 1, Integer::sum);
        List<Integer> idx = new ArrayList<>();
        for (int i = 0; i < s.raw.size(); i++) {
            String k = rowKey(s.raw.get(i), s.cols);
            Integer c = cnt.get(k);
            if (c != null && c > 0) {
                cnt.put(k, c - 1);
                idx.add(i);
            }
        }
        return idx;
    }

    String identity(Side s, int i, String layer, String table) {
        List<String> parts = new ArrayList<>();
        String[] want = layer.equals("diff")
                ? new String[]{PK.get(table), "start_dt", "ctl_loading", "ctl_do", "ctl_action"}
                : new String[]{PK.get(table), "start_dt"};
        for (String c : want) {
            int j = s.cols.indexOf(c);
            if (j >= 0) parts.add(s.raw.get(i)[j]);
        }
        return String.join("|", parts);
    }

    String describe(Side s, int i, boolean full) {
        StringBuilder b = new StringBuilder();
        String[] id = {"start_dt", "end_dt", "ctl_do", "ctl_action", "ctl_loading", "ctl_validfrom", "p1month"};
        for (String c : id) {
            int j = s.cols.indexOf(c);
            if (j >= 0) b.append(c).append('=').append(s.shown.get(i)[j]).append(' ');
        }
        if (full) {
            for (int j = 0; j < s.cols.size(); j++) {
                String c = s.cols.get(j);
                if (Arrays.asList(id).contains(c) || c.equals(PK.get(currentTable)) || c.endsWith("dp_code")
                        || c.equals("sid") || c.equals("deleted_flag") || c.equals("dirty_flag")) continue;
                b.append(c).append('=').append(s.shown.get(i)[j]).append(' ');
            }
        }
        return b.toString().trim();
    }

    String currentTable;
    final Map<String, TreeMap<String, Set<String>>> caseMatrix = new TreeMap<>();   // case -> load -> {table:cols}
    final List<String> loadsDone = new ArrayList<>();
    final List<String> loadsNotCompared = new ArrayList<>();

    int compareLoad(String load) throws IOException {
        cache.clear();
        File d30 = new File(run, "exp30/" + load), d40 = new File(run, "exp40/" + load);
        boolean has30 = d30.exists(), has40 = d40.exists();
        File rep = new File(run, "cmp/" + load + ".txt");
        rep.getParentFile().mkdirs();
        int bad = 0;
        try (PrintWriter out = new PrintWriter(rep, "UTF-8")) {
            out.printf("load %s: 3.0 export %s, 4.0 export %s%s%n", load, has30 ? "present" : "MISSING", has40 ? "present" : "MISSING",
                    masks.isEmpty() ? "" : ", masked: " + masks);
            if (!has30 || !has40) loadsNotCompared.add(load);
            if (!has30 && !has40) return 0;
            Map<String, Map<Long, String>> sid = sidMaps(has30 ? d30.getPath() : null, has40 ? d40.getPath() : null);
            out.printf("%-26s %7s %7s %7s %7s  %s%n", "table", "rows30", "rows40", "only30", "only40", "verdict");
            StringBuilder details = new StringBuilder();
            for (String layer : new String[]{"snp", "diff"}) {
                for (String t : TABLES) {
                    currentTable = t;
                    String tn = layer + "." + t;
                    Side a = load(d30.getPath(), layer, t, sid), b = load(d40.getPath(), layer, t, sid);
                    if (a.present) dump(a, "30", load, tn);
                    if (b.present) dump(b, "40", load, tn);
                    if (!a.present || !b.present) {
                        out.printf("%-26s %7s %7s %7s %7s  %s%n", tn, a.present ? a.raw.size() : "-", b.present ? b.raw.size() : "-", "-", "-", "NOT COMPARED");
                        tables.printf("%s\t%s\t%s\t%s\t\t\tNOT_COMPARED%n", load, tn, a.present ? a.raw.size() : "", b.present ? b.raw.size() : "");
                        continue;
                    }
                    String schema = "";
                    if (!a.cols.equals(b.cols)) {
                        Set<String> x = new TreeSet<>(a.cols), y = new TreeSet<>(b.cols);
                        x.removeAll(b.cols);
                        y.removeAll(a.cols);
                        schema = " SCHEMA-DIFF only30=" + x + " only40=" + y;
                        List<String> common = new ArrayList<>(a.cols);
                        common.retainAll(b.cols);
                        a = restrict(a, common);
                        b = restrict(b, common);
                    }
                    List<String> onlyA = exceptAll(a.raw, b.raw, a.cols), onlyB = exceptAll(b.raw, a.raw, a.cols);
                    boolean ok = onlyA.isEmpty() && onlyB.isEmpty() && schema.isEmpty();
                    if (!ok) bad++;
                    out.printf("%-26s %7d %7d %7d %7d  %s%s%n", tn, a.raw.size(), b.raw.size(), onlyA.size(), onlyB.size(), ok ? "OK" : "DIFF", schema);
                    tables.printf("%s\t%s\t%d\t%d\t%d\t%d\t%s%n", load, tn, a.raw.size(), b.raw.size(), onlyA.size(), onlyB.size(), ok ? "OK" : "DIFF");
                    if (!onlyA.isEmpty() || !onlyB.isEmpty()) details.append(explain(load, layer, t, a, b, onlyA, onlyB));
                }
            }
            out.println(bad == 0 ? "ALL 16 TABLES MATCH" : "TABLES WITH DIFFERENCES: " + bad);
            out.println();
            out.print(details);
        }
        loadsDone.add(load);
        return bad;
    }

    static Side restrict(Side s, List<String> common) {
        Side r = new Side();
        r.present = s.present;
        r.cols = common;
        r.keys = s.keys;
        r.cases = s.cases;
        for (int i = 0; i < s.raw.size(); i++) {
            String[] v = new String[common.size()], w = new String[common.size()];
            for (int j = 0; j < common.size(); j++) {
                int k = s.cols.indexOf(common.get(j));
                v[j] = s.raw.get(i)[k];
                w[j] = s.shown.get(i)[k];
            }
            r.raw.add(v);
            r.shown.add(w);
        }
        return r;
    }

    /** Pairs rows that are only on one side by identity (PK, start_dt, ctl_loading/do/action) and names the differing columns. */
    String explain(String load, String layer, String t, Side a, Side b, List<String> onlyA, List<String> onlyB) {
        String tn = layer + "." + t;
        List<Integer> ia = pick(a, onlyA), ib = pick(b, onlyB);
        Map<String, Deque<Integer>> byId = new LinkedHashMap<>();
        for (int i : ib) byId.computeIfAbsent(identity(b, i, layer, t), k -> new ArrayDeque<>()).add(i);
        TreeMap<String, List<String>> perCase = new TreeMap<>();
        for (int i : ia) {
            Deque<Integer> q = byId.get(identity(a, i, layer, t));
            String cs = a.cases.get(i), key = a.keys.get(i);
            if (q != null && !q.isEmpty()) {
                int j = q.poll();
                List<String> diffs = new ArrayList<>(), names = new ArrayList<>();
                for (int c = 0; c < a.cols.size(); c++) {
                    if (!a.raw.get(i)[c].equals(b.raw.get(j)[c])) {
                        diffs.add(a.cols.get(c) + ": 3.0=" + a.shown.get(i)[c] + " 4.0=" + b.shown.get(j)[c]);
                        names.add(a.cols.get(c));
                    }
                }
                perCase.computeIfAbsent(cs, k -> new ArrayList<>()).add("  CHANGED  key=" + key + "  " + describe(a, i, false) + "\n      " + String.join("\n      ", diffs));
                record(load, tn, cs, key, "changed", String.join(",", names), String.join("; ", diffs));
            } else {
                perCase.computeIfAbsent(cs, k -> new ArrayList<>()).add("  ONLY 3.0 key=" + key + "  " + describe(a, i, true));
                record(load, tn, cs, key, "only30", "", describe(a, i, true));
            }
        }
        for (Deque<Integer> q : byId.values()) {
            for (int j : q) {
                String cs = b.cases.get(j), key = b.keys.get(j);
                perCase.computeIfAbsent(cs, k -> new ArrayList<>()).add("  ONLY 4.0 key=" + key + "  " + describe(b, j, true));
                record(load, tn, cs, key, "only40", "", describe(b, j, true));
            }
        }
        StringBuilder sb = new StringBuilder("== " + tn + "\n");
        for (Map.Entry<String, List<String>> e : perCase.entrySet()) {
            sb.append(" case ").append(e.getKey()).append('\n');
            for (String l : e.getValue()) sb.append(l).append('\n');
        }
        return sb.toString();
    }

    void record(String load, String tn, String cs, String key, String kind, String cols, String detail) {
        mismatches.printf("%s\t%s\t%s\t%s\t%s\t%s\t%s%n", load, tn, cs, tsvEsc(key), kind, cols, tsvEsc(detail));
        String what = kind.equals("changed") ? tn + "(" + cols + ")" : tn + "(" + kind + ")";
        caseMatrix.computeIfAbsent(cs, k -> new TreeMap<>()).computeIfAbsent(load, k -> new TreeSet<>()).add(what);
    }

    void dump(Side s, String ver, String load, String tn) throws IOException {
        File f = new File(run, "dump/" + ver + "/" + load + "/" + tn + ".tsv");
        f.getParentFile().mkdirs();
        try (PrintWriter w = new PrintWriter(f, "UTF-8")) {
            w.print("_case\t_key");
            for (String c : s.cols) w.print("\t" + c);
            w.println();
            for (int i = 0; i < s.raw.size(); i++) {
                w.print(s.cases.get(i) + "\t" + tsvEsc(s.keys.get(i)));
                for (String v : s.shown.get(i)) w.print("\t" + tsvEsc(v));
                w.println();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: Compare <runDir> <chain> <load,load,...> [--mask table.col,...]");
            System.exit(2);
        }
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));
        String mask = null;
        for (int i = 3; i < args.length; i++) if (args[i].equals("--mask") && i + 1 < args.length) mask = args[++i];
        SparkSession sp = SparkSession.builder().master("local[2]").appName("compare").config("spark.ui.enabled", "false")
                .config("spark.sql.session.timeZone", "Europe/Moscow").config("spark.sql.shuffle.partitions", "2").getOrCreate();
        sp.sparkContext().setLogLevel("ERROR");
        File run = new File(args[0]).getAbsoluteFile();
        Compare c = new Compare(sp, run, args[1], mask);
        new File(run, "cmp").mkdirs();
        int bad = 0;
        try (PrintWriter mm = new PrintWriter(new File(run, "cmp/mismatches.tsv"), "UTF-8");
             PrintWriter tb = new PrintWriter(new File(run, "cmp/tables.tsv"), "UTF-8")) {
            c.mismatches = mm;
            c.tables = tb;
            mm.println("load\ttable\tcase\tkey\tkind\tcolumns\tdetail");
            tb.println("load\ttable\trows30\trows40\tonly30\tonly40\tverdict");
            for (String load : args[2].split(",")) {
                int b = c.compareLoad(load);
                bad += b;
                System.out.println("[compare] " + load + ": " + (b == 0 ? "16/16 tables match" : b + " tables differ"));
            }
        }
        try (PrintWriter w = new PrintWriter(new File(run, "cmp/cases.txt"), "UTF-8")) {
            w.println("Per case: loads and tables where 3.0 and 4.0 differ (table(columns) = same row, other values;"
                    + " only30/only40 = row on one side only).");
            List<String> cmpd = new ArrayList<>(c.loadsDone);
            cmpd.removeAll(c.loadsNotCompared);
            w.println("Loads compared: " + (cmpd.isEmpty() ? "none" : String.join(",", cmpd))
                    + (c.loadsNotCompared.isEmpty() ? "" : "; NOT compared (one version has no export): "
                    + String.join(",", c.loadsNotCompared) + " - 'same' covers only the compared loads"));
            List<String[]> known = new ArrayList<>();
            File ct = new File(run, "synth/" + args[1] + "/cases.tsv");
            if (ct.exists()) for (String l : Files.readAllLines(ct.toPath(), StandardCharsets.UTF_8))
                if (!l.startsWith("id\t") && !l.trim().isEmpty()) known.add(l.split("\t", 4));
            Set<String> listed = new HashSet<>();
            for (String[] k : known) {
                listed.add(k[0]);
                TreeMap<String, Set<String>> m = c.caseMatrix.get(k[0]);
                boolean acc = k.length > 3 && !k[3].trim().isEmpty();
                w.printf("%-4s %-12s %s%n", k[0], m == null ? "same" : acc ? "ACCEPTED" : "DIFFERENT", k.length > 1 ? k[1] : "");
                if (m != null && acc) w.println("       accepted deviation (CASES.md): " + k[3]);
                if (m != null) for (Map.Entry<String, Set<String>> l : m.entrySet()) w.println("       " + l.getKey() + "  " + String.join("  ", l.getValue()));
                if (m != null && k.length > 2 && !k[2].trim().isEmpty()) w.println("       known 4.0 behaviour (CASES.md): " + k[2]);
            }
            for (Map.Entry<String, TreeMap<String, Set<String>>> e : c.caseMatrix.entrySet()) {
                if (listed.contains(e.getKey())) continue;
                w.printf("%-4s %-12s %s%n", e.getKey(), "DIFFERENT", "(rows not attributable to a catalog case)");
                for (Map.Entry<String, Set<String>> l : e.getValue().entrySet()) w.println("       " + l.getKey() + "  " + String.join("  ", l.getValue()));
            }
        }
        sp.stop();
        System.exit(bad == 0 ? 0 : 3);
    }
}
