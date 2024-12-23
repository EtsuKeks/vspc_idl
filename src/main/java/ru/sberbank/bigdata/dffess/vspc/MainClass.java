package ru.sberbank.bigdata.dffess.vspc;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDataProductGenerator;
import ru.sberbank.bigdata.essa.common.master.DataProductRunner;
import ru.sberbank.bigdata.essa.common.master.app.*;
import ru.sberbank.bigdata.essa.common.master.spark.SparkAppRunnerGenerator;
import ru.sberbank.bigdata.essa.common.master.cmdargs.CmdArgs;

public class MainClass {

    public static void main(String[] args) throws Exception {

        System.out.println("START MAIN VSPC_IDL");
        CmdArgs cmdArgs = CmdArgs.parse(args);
        CtlAdapter ctlAdapter = new CtlAdapterImpl(cmdArgs);
        DataProductRunner dpr = new DataProductRunner(
                new VspcDataProductGenerator(),
                ctlAdapter,
                new SparkAppRunnerGenerator()
        );
        dpr.run();

        System.out.println("FINISHED MAIN VSPC_IDL");

    }

    
}