package ru.sberbank.bigdata.dffess.vspc.tables.divh;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDmStgClass;

public class DIVHStgClass extends VspcDmStgClass {
    public DIVHStgClass() {
        moduleConfiguration = new DIVHStgConfigClass(this.moduleConfiguration);
    }

    public DIVHStgClass(String moduleName) {
        moduleConfiguration = new DIVHStgConfigClass(this.moduleConfiguration);
        this.moduleName = moduleName;
        this.entityTransformer = new DivHTransformer();
    }

    public static void main(String[] args) throws Exception {
        DIVHStgClass stgClass = new DIVHStgClass("");
        stgClass.run(args);
    }
}
