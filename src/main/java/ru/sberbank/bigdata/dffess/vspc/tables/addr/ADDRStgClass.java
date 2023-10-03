package ru.sberbank.bigdata.dffess.vspc.tables.addr;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDmStgClass;

public class ADDRStgClass extends VspcDmStgClass {
    public ADDRStgClass() {
        moduleConfiguration = new ADDRStgConfigClass(this.moduleConfiguration);
    }

    public ADDRStgClass(String moduleName) {
        moduleConfiguration = new ADDRStgConfigClass(this.moduleConfiguration);
        this.moduleName = moduleName;
        this.entityTransformer = new AddrTransformer();
    }

    public static void main(String[] args) throws Exception {
        ADDRStgClass stgClass = new ADDRStgClass("");
        stgClass.run(args);
    }
}
