package ru.sberbank.bigdata.dffess.rt;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.dffess.rt.transformations.BaseTransformation;
import ru.sberbank.bigdata.dffess.rt.transformations.dest.AddrObjTransformation;
import ru.sberbank.bigdata.dffess.rt.transformations.dest.DivTransformation;
import ru.sberbank.bigdata.dffess.rt.transformations.src.SourceTableTransformation;
import ru.sberbank.bigdata.sl.rt.core.App;
import ru.sberbank.bigdata.sl.rt.core.AppContext;
import ru.sberbank.bigdata.sl.rt.core.Transformation;
import ru.sberbank.bigdata.sl.rt.core.exceptions.AppException;
import ru.sberbank.bigdata.sl.rt.feature.FeatureToggle;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.sberbank.bigdata.dffess.rt.transformations.BaseTransformation.INTERNAL_VSPC_IDL;
import static ru.sberbank.bigdata.sl.rt.core.App.LoadMode.INCREMENT;
import static ru.sberbank.bigdata.sl.rt.core.App.LoadMode.INITIAL;
import static ru.sberbank.bigdata.sl.rt.reader.SourceTable.SourceType.KAP802_STORK;
import static ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters.SPARKMASK;
import static ru.sberbank.bigdata.sl.rt.validation.ApplicationParameters.SPARK_PARAMETERS;

public class Main {

    private static final Logger logger = new Logger(null, Main.class);

    private static final String MONITORING_TEST_ENABLED_FLAG = "monitoring_test_enabled";

    public static void main(String... args) {
        logger.logInfo("Приложение запущено с параметрами: " + Arrays.asList(args));

        App app = App.create("root", args)
                .autoSparkSession(false)
                .build();
        logger.setApp(app);
        AppContext context = app.ctx();

        FeatureToggle featureToggle = new FeatureToggle(context.getProperties(), MONITORING_TEST_ENABLED_FLAG);
        if (featureToggle.isOn()) {
            //new MonitoringPerformanceTest(context).run();
            return;
        }

        SparkSession.Builder builder = SparkSession.builder()
                .config("hive.exec.dynamic.partition", "true")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("hive.exec.max.dynamic.partitions", "1000000")
                .config("spark.sql.parquet.compression.codec", "gzip")
                .config("hive.mapred.supports.subdirectories", "true")
                .config("mapred.input.dir.recursive", "true")
                .config("hive.supports.subdirectories", "true")
                .config("hive.input.dir.recursive", "true")
                .config("spark.sql.hive.convertMetastoreParquet", "false")
                .config("spark.hadoop.metastore.catalog.default", "spark")
                .config("spark.shuffle.service.enabled", "true")
                .config("spark.sql.parquet.writeLegacyFormat", "true")
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .enableHiveSupport();

        // добавляем спарк параметры из CTL, полученные в App.ctx
        getSparkParameters(context.getProperties()).forEach(builder::config);
        SparkSession session = builder.getOrCreate();
        session.sparkContext().setLogLevel("WARN");
        app.initSparkSession(session);

        // настройки ССК
        System.setProperty("service_data_product_code", context.getShortProductCode());
        System.setProperty("service_init_greenfield", context.getProperty("sk_greenfield", "false"));
        if (!context.getLogEnv().isEmpty()) {
            System.setProperty("app_log_env", context.getLogEnv());
        }
        BaseTransformation.initStaticEnv(context);

        // Создаём набор mirror трансформаций, которые загружают из источника в STGTMP промежуточные _mirror сущности
        Map<String, Transformation> srcMap = Stream.of(
                        "ebx_rep_m_sfs_0002_sfsdivision")
                .collect(Collectors.toMap(s -> s, s -> new SourceTableTransformation(app, s, INITIAL, INTERNAL_VSPC_IDL, KAP802_STORK)));





        //  Создаётся трансформация, которая в результате своей работы публикует набор данных:
        //  'usl_vspc_idl.div.hub' (должен быть указан в файле metadata.json)
        Transformation div = new DivTransformation(app, "usl_vspc_idl.div_.hub");
        if (context.getLoadMode() == INCREMENT) {
            //srcMap.put(z_ft_money_delta, new SourceTableTransformation(app, EBX_REP_M_SFS_0002_SFSDIVISION, INCREMENT, INTERNAL_VSPC_IDL, KAP802_STORK));
            //srcMap.put(as_addr_obj_delta, new SourceTableTransformation(app, EBX_REP_M_SFS_0002_SFSDIVISION, INCREMENT, INTERNAL_VSPC_IDL, KAP802_XSTREAM));
        } else {

        }


        // Все cозданные трансформации необходимо добавить в app
        // src сущности
        srcMap.values().forEach(app::addTransformation);

        // для инициализирующей загрузки добавляем первую трансформацию
        if (app.ctx().getLoadMode() == INITIAL) {
            //чистим таблицу src_log
            BaseTransformation initTransformation = new BaseTransformation(app, "clean_src_log") {
                @Override
                public void run() {
                    clearTable(STG_SCHEMA, SRC_LOG_TABLE);
                }
            };
            app.addTransformation(initTransformation);
            // от данной трансформации зависят все src трансформации, поэтому она выполнится первой
            srcMap.values().forEach(src -> src.addPredecessors(initTransformation));
        }

        app.addTransformation(div);

        //
        //  Старт работы приложения.
        //  При этом исполняются все определенные в рамках данного приложения трансформации (объекты
        //  Transformation) в порядке с учётом зависимостей
        //
        try {
            app.run();
        } catch (AppException e) {
            app.log(e.getLogMessage());
            throw e;
        }
    }

    private static Map<String, String> getSparkParameters(Properties parameters) {
        Map<String, String> sparkParameters = new HashMap<>();
        sparkParameters.putAll(parseSparkParameters(parameters.getProperty(SPARK_PARAMETERS.getParamName())));
        sparkParameters.putAll(getSparkPropertiesByPrefix(parameters));
        return sparkParameters;
    }

    private static Map<String, String> parseSparkParameters(String value) {
        if (value == null || value.isEmpty()) {
            return Collections.emptyMap();
        }

        Pattern sparkParameterPattern = Pattern.compile("([^ =]+)\\s*=\\s*(.+)");

        return Stream.of(value.split("--conf ")).
                filter(conf -> !conf.equals("")).
                filter(conf -> conf.startsWith("spark.")).
                filter(conf -> !conf.matches(".+ --.+")).
                filter(conf -> sparkParameterPattern.matcher(conf.trim()).matches()).
                map(conf -> {
                    String[] param = conf.split("=");
                    return new String[]{param[0], param[1].trim()};
                }).collect(Collectors.toMap(k -> k[0], v -> v[1]));
    }

    private static Map<String, String> getSparkPropertiesByPrefix(Properties parameters) {
        return parameters.stringPropertyNames().stream().
                filter(name -> name.startsWith(SPARKMASK.getParamName())).
                collect(Collectors.toMap(name -> name, parameters::getProperty));
    }

}
