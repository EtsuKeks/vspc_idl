package ru.sberbank.bigdata.dffess.vspc.tables.addrh;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDmStgClass;

public class ADDRHStgClass extends VspcDmStgClass {
    public ADDRHStgClass() {
        moduleConfiguration = new ADDRHStgConfigClass(this.moduleConfiguration);
    }

    public ADDRHStgClass(String moduleName) {
        moduleConfiguration = new ADDRHStgConfigClass(this.moduleConfiguration);
        this.moduleName = moduleName;
        this.entityTransformer = new AddrHTransformer();
    }

    public static void main(String[] args) throws Exception {
        ADDRHStgClass stgClass = new ADDRHStgClass("");
        stgClass.run(args);
    }
}
