package ru.sberbank.bigdata.dffess.vspc;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDataProductGenerator;
import ru.sberbank.bigdata.essa.common.master.DataProductRunner;
import ru.sberbank.bigdata.essa.common.master.app.CtlAdapter;
import ru.sberbank.bigdata.essa.common.master.app.CtlAdapterImpl;
import ru.sberbank.bigdata.essa.common.master.cmdargs.CmdArgs;
import ru.sberbank.bigdata.essa.common.master.spark.SparkAppRunnerGenerator;

public class MainClass {

    public static void main(String[] args) {
        System.out.println("START MAIN VSPC_IDL");

        CmdArgs cmdArgs = CmdArgs.parse(args);
        CtlAdapter ctlAdapter = new CtlAdapterImpl(cmdArgs);
        DataProductRunner dpr = new DataProductRunner(
                new VspcDataProductGenerator(),
                ctlAdapter,
                new SparkAppRunnerGenerator()
        ).withKiller("USL_VSPC_IDL","MAIN_ACTION");
        dpr.run();

        System.out.println("FINISHED MAIN VSPC_IDL");
    }
}
