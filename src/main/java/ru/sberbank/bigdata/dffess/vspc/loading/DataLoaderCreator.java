package ru.sberbank.bigdata.dffess.vspc.loading;

import ru.sberbank.bigdata.essa.common.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.loading.DataLoaderHubSnp;

public class DataLoaderCreator {
    public static DataLoader getDataLoader(String tableName){
        return new DataLoaderHubSnp();
    }
}

