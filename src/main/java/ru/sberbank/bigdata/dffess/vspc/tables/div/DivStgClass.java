package ru.sberbank.bigdata.dffess.vspc.tables.div;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDmStgClass;

public class DivStgClass extends VspcDmStgClass {
    public DivStgClass() {
        moduleConfiguration = new DivStgConfigClass(this.moduleConfiguration);
    }

    public DivStgClass(String moduleName) {
        moduleConfiguration = new DivStgConfigClass(this.moduleConfiguration);
        this.moduleName = moduleName;
        this.entityTransformer = new DivTransformer();
    }

    public static void main(String[] args) throws Exception {
        DivStgClass stgClass = new DivStgClass("");
        stgClass.run(args);
    }
}
