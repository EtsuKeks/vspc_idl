package ru.sberbank.bigdata.dffess.vspc;

import ru.sberbank.bigdata.essa.common.master.AppRunnerGenerator;
import ru.sberbank.bigdata.essa.common.master.DataProductRunner;
import ru.sberbank.bigdata.essa.common.master.app.CtlAdapter;
import ru.sberbank.bigdata.essa.common.master.app.CtlAdapterImpl;
import ru.sberbank.bigdata.essa.common.master.app.DataProductGenerator;
import ru.sberbank.bigdata.essa.common.master.parser.CmdArgsParser;
import ru.sberbank.bigdata.essa.common.master.spark.SparkAppRunnerGenerator;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDataProductGenerator;

import java.util.Map;

public class MainClass {

    public static void main(String[] args) throws Exception {

        System.out.println("START MAIN VSPC_IDL");
        CmdArgsParser cmdArgsParser = new CmdArgsParser();
        Map<String, String> cmdArgs = cmdArgsParser.parse(args);

        EtlKit preloadKit = preloadKit();
        EtlKit etlKit = getEtlKit(preloadKit);

        LoggerWrapper loggerWrapper = new SoutLoggerWrapper();

        DataProductGenerator dataProductGenerator = new VspcDataProductGenerator();
        CtlAdapter ctlAdapter = new CtlAdapterImpl(cmdArgs.get("ctl_url"), cmdArgs.get("loading_id"));
        AppRunnerGenerator appRunnerGenerator = new SparkAppRunnerGenerator();
        DataProductRunner dpr = new DataProductRunner(dataProductGenerator, ctlAdapter, appRunnerGenerator);
        dpr.run();

        System.out.println("FINISHED MAIN VSPC_IDL");

    }

    
}