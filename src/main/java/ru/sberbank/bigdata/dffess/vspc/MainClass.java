package ru.sberbank.bigdata.dffess.vspc;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcEntityProvider;
import ru.sberbank.bigdata.dffess.vspc.tables.addr.AddrConfiguration;
import ru.sberbank.bigdata.essa.common.logger.LoggerWrapper;
import ru.sberbank.bigdata.essa.common.logger.SoutLoggerWrapper;
import ru.sberbank.bigdata.essa.common.master.AppRunnerGenerator;
import ru.sberbank.bigdata.essa.common.master.DataProductRunner;
import ru.sberbank.bigdata.essa.common.master.app.*;
import ru.sberbank.bigdata.essa.common.master.parser.CmdArgsParser;
import ru.sberbank.bigdata.essa.common.master.spark.SparkAppRunnerGenerator;
import ru.sberbank.bigdata.essa.common.slave.templates.FinalConfiguration;

import java.util.HashMap;
import java.util.Map;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.ADDR;
import static ru.sberbank.bigdata.dffess.vspc.constant.VpscConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.essa.common.master.app.forApp.SparkConfKeeper.ResourceConfig.HARD;
import static ru.sberbank.bigdata.essa.common.master.app.forApp.SparkConfKeeper.ResourceConfig.LITE;

public class MainClass {

    public static void main(String[] args) throws Exception {
        System.out.println("ssdsssdsssd=========================================");
        CmdArgsParser cmdArgsParser = new CmdArgsParser();
        Map<String, String> cmdArgs = cmdArgsParser.parse(args);

        EtlKit preloadKit = preloadKit();
        EtlKit etlKit = getEtlKit(preloadKit);

        LoggerWrapper loggerWrapper = new SoutLoggerWrapper();


        Map<String, String> statMap = new HashMap<>();
        statMap.put("2", "");
        statMap.put("12", "");

        EtlKit finisher = EtlKit.newInstance("finisher", LITE)
                .addDependency(preloadKit)
                .addDependency(etlKit)
                .needToPostStatistic(statMap)
                .addConfiguration(FinalConfiguration.class);

        DataProduct dp = DataProduct.newInstance(DATA_PRODUCT, new VspcEntityProvider(),
                        cmdArgs.get("ctl_url"), cmdArgs.get("loading_id"))
                .addEtlKit(preloadKit)
                .addEtlKit(etlKit)
                .addEtlKit(finisher);
        CtlAdapter ctlAdapter = new CtlAdapterImpl();
        AppRunnerGenerator appRunnerGenerator = new SparkAppRunnerGenerator();
        DataProductRunner dpr = new DataProductRunner(dp, ctlAdapter, appRunnerGenerator);
        dpr.run();
        System.out.println("++++++++++++++++++++++++++++");

    }

    private static EtlKit preloadKit() {
        //Etl stage = Etl.newInstance(TablesName.CHG_TRANS_HEAD, ChgTransHeadConfiguration.class, LITE);

        return EtlKit.newInstance("stage", HARD);
                //.addEtl(stage);
    }

    private static EtlKit getEtlKit(EtlKit preload) {
        Etl addr = Etl.newInstance(ADDR, AddrConfiguration.class, LITE).addDependency(preload);
        //Etl crncy = Etl.newInstance(TablesName.CRNCY, CrncyConfiguration.class, LITE).addDependency(preload);
        //Etl trans = Etl.newInstance(TablesName.TRANS, TransConfiguration.class, LITE).addDependency(preload);
        return EtlKit.newInstance("ssd", HARD)
                //.addEtl(selfServiceDevice)
                //.addEtl(crncy)
                .addEtl(addr);
    }
}