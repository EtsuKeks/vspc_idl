package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.essa.common.constant.CommonConst;
import ru.sberbank.bigdata.essa.common.master.app.EtlSeqService;

import java.util.List;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.essa.common.utils.Expression.stringList;

public class VspcEtlSeqServiceImpl implements EtlSeqService {

    @Override
    public List<String> getSeq(String instanceCode, CommonConst.LoadMode loadMode) {
        return stringList(
                ADDRH, ADDR, DIVH, DIV, DIVTYPE, TERRDIVTYPE, TIMEZONE, CHG_DIVH, DIVDIVRELATION
        );
    }
}
