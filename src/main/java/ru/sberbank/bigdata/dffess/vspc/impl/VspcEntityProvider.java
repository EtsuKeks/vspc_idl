package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.essa.common.master.app.CtlEntityProvider;


public  class VspcEntityProvider implements CtlEntityProvider {
    @Override
    public String getCtlEntity(String instanceCode) {
        String ctlEntity = "801005314";
        return ctlEntity;
    }
}
