package ru.sberbank.bigdata.dffess.vspc.tables.addr;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDmStgClass;

public class AddrtStgClass extends VspcDmStgClass {
    public AddrtStgClass() {
        moduleConfiguration = new AddrStgConfigClass(this.moduleConfiguration);
    }

    public AddrtStgClass(String moduleName) {
        moduleConfiguration = new AddrStgConfigClass(this.moduleConfiguration);
        this.moduleName = moduleName;
        this.entityTransformer = new AddrTransformer();
    }

    public static void main(String[] args) throws Exception {
        AddrtStgClass stgClass = new AddrtStgClass("");
        stgClass.run(args);
    }
}
