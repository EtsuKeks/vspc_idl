package ru.sberbank.bigdata.dffess.vspc;

import ru.sberbank.bigdata.dffess.vspc.constant.TablesName;
import ru.sberbank.bigdata.dffess.vspc.impl.VspcEntityProvider;
import ru.sberbank.bigdata.dffess.vspc.tables.addr.AddrConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.addrh.AddrHConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.div.DivConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.divh.DivHConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.divtype.DivTypeConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.terrdivtype.TerrDivTypeConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.timezone.TimeZoneConfiguration;

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

import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.DATA_PRODUCT;
import static ru.sberbank.bigdata.essa.common.master.app.forApp.SparkConfKeeper.ResourceConfig.HARD;
import static ru.sberbank.bigdata.essa.common.master.app.forApp.SparkConfKeeper.ResourceConfig.LITE;

public class MainClass {

    public static void main(String[] args) throws Exception {
        System.out.println("START MAIN VSPC_IDL");
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
        System.out.println("FINISHED MAIN VSPC_IDL");

    }

    private static EtlKit preloadKit() {
        //Etl stage = Etl.newInstance(TablesName.CHG_TRANS_HEAD, ChgTransHeadConfiguration.class, LITE);

        return EtlKit.newInstance("stage", HARD);
                //.addEtl(stage);
    }

    private static EtlKit getEtlKit(EtlKit preload) {
        Etl addr = Etl.newInstance(TablesName.ADDR, AddrConfiguration.class, LITE).addDependency(preload);
        Etl addrh = Etl.newInstance(TablesName.ADDRH, AddrHConfiguration.class, LITE).addDependency(preload);
        Etl div = Etl.newInstance(TablesName.DIV, DivConfiguration.class, LITE).addDependency(preload);
        Etl divh = Etl.newInstance(TablesName.DIVH, DivHConfiguration.class, LITE).addDependency(preload);
        Etl divtype = Etl.newInstance(TablesName.DIVTYPE, DivTypeConfiguration.class, LITE).addDependency(preload);
        Etl terrdivtype = Etl.newInstance(TablesName.TERRDIVTYPE, TerrDivTypeConfiguration.class, LITE).addDependency(preload);
        Etl timezone = Etl.newInstance(TablesName.TIMEZONE, TimeZoneConfiguration.class, LITE).addDependency(preload);

        addrh = addrh.addDependency(addr);
        divh = divh.addDependency(div).addDependency(addr);
        divtype = divtype.addDependency(div);

        return EtlKit.newInstance("vspc_idl", HARD)
                .addEtl(addr)
                .addEtl(addrh)
                .addEtl(div)
                .addEtl(divh)
                .addEtl(divtype)
                .addEtl(terrdivtype)
                .addEtl(timezone);
    }
}