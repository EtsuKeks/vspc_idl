package ru.sberbank.bigdata.dffess.vspc.loading;

import ru.sberbank.bigdata.dffess.vspc.constant.TablesName;
import ru.sberbank.bigdata.essa.common.loading.DataLoader;
import ru.sberbank.bigdata.essa.common.loading.DataLoaderHist;
import ru.sberbank.bigdata.essa.common.loading.DataLoaderHubDiff;
import ru.sberbank.bigdata.essa.common.loading.DataLoaderHubSnp;

public class DataLoaderCreator {
    public static DataLoader getDataLoader(String tableName) {
        if (tableName.equals(TablesName.DIVH) || tableName.equals(TablesName.ADDRH) || tableName.equals(TablesName.DIVTYPE)) {
            return new DataLoaderHist();
        }
        if (tableName.equals(TablesName.DIV) || tableName.equals(TablesName.ADDR) || tableName.equals(TablesName.TERRDIVTYPE) ) {
            return new DataLoaderHubSnp();
        }
        return new DataLoaderHubDiff();
    }
}

