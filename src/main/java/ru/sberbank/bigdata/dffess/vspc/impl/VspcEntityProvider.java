package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.essa.common.master.app.CtlEntityProvider;

import static ru.sberbank.bigdata.dffess.vspc.constant.VspcConstant.CTL_ENTITY_ID;

public class VspcEntityProvider implements CtlEntityProvider {

    @Override
    public String getCtlEntity(String instanceCode) {
        return CTL_ENTITY_ID;
    }
}
