package ru.sberbank.bigdata.dffess.vspc.tables.terrdivtype;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDmStgClass;

public class TERRDIVTYPEStgClass extends VspcDmStgClass {
    public TERRDIVTYPEStgClass() {
        moduleConfiguration = new TERRDIVTYPEStgConfigClass(this.moduleConfiguration);
    }

    public TERRDIVTYPEStgClass(String moduleName) {
        moduleConfiguration = new TERRDIVTYPEStgConfigClass(this.moduleConfiguration);
        this.moduleName = moduleName;
        this.entityTransformer = new TerrDivTypeTransformer();
    }

    public static void main(String[] args) throws Exception {
        TERRDIVTYPEStgClass stgClass = new TERRDIVTYPEStgClass("");
        stgClass.run(args);
    }
}
