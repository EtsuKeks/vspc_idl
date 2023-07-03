package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.essa.common.templates.EntityProxyRelation;

import java.util.HashMap;
import java.util.Map;

public class VspcEntityProxyRelation implements EntityProxyRelation {
    private Map<String, String> relationMap;

    public VspcEntityProxyRelation() {
        this.relationMap = new HashMap<>();
    }

    @Override
    public String getProxyBySrcTable(String tableName) {
        return relationMap.getOrDefault(tableName, null);
    }

    @Override
    public String getSrcTableByProxy(String tableName) {
        for (Map.Entry<String,String> entry : relationMap.entrySet()){
            if(entry.getValue().equals(tableName)){
                return entry.getKey();
            }
        }
        return null;
    }
}