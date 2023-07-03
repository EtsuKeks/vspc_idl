package ru.sberbank.bigdata.dffess.vspc.tables.addrh;

import ru.sberbank.bigdata.dffess.vspc.impl.VspcDmStgClass;

public class AddrhStgClass extends VspcDmStgClass {
    public AddrhStgClass() {
        moduleConfiguration = new AddrhStgConfigClass(this.moduleConfiguration);
    }

    public AddrhStgClass(String moduleName) {
        moduleConfiguration = new AddrhStgConfigClass(this.moduleConfiguration);
        this.moduleName = moduleName;
        this.entityTransformer = new AddrhTransformer();
    }

    public static void main(String[] args) throws Exception {
        AddrhStgClass stgClass = new AddrhStgClass("");
        stgClass.run(args);
    }
}
