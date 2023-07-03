package ru.sberbank.bigdata.dffess.vspc.tables.divh;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDmStgClass;

public class DivhStgClass extends VspcDmStgClass {
    public DivhStgClass() {
        moduleConfiguration = new DivhStgConfigClass(this.moduleConfiguration);
    }

    public DivhStgClass(String moduleName) {
        moduleConfiguration = new DivhStgConfigClass(this.moduleConfiguration);
        this.moduleName = moduleName;
        this.entityTransformer = new DivhTransformer();
    }

    public static void main(String[] args) throws Exception {
        DivhStgClass stgClass = new DivhStgClass("");
        stgClass.run(args);
    }
}
