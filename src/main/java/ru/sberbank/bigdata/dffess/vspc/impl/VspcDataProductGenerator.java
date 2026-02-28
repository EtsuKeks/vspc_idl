package ru.sberbank.bigdata.dffess.vspc.impl;

import ru.sberbank.bigdata.dffess.vspc.tables.addr.AddrConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.addrh.AddrHConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.chgdivh.ChgDivHConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.div.DivConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.divdivrelation.DivDivRelationConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.divh.DivHConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.divtype.DivTypeConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.terrdivtype.TerrDivTypeConfiguration;
import ru.sberbank.bigdata.dffess.vspc.tables.timezone.TimeZoneConfiguration;
import ru.sberbank.bigdata.essa.common.master.app.DataProduct;
import ru.sberbank.bigdata.essa.common.master.app.DataProductGenerator;
import ru.sberbank.bigdata.essa.common.master.app.Etl;
import ru.sberbank.bigdata.essa.common.master.app.EtlKit;
import ru.sberbank.bigdata.essa.common.slave.templates.fin.FinalConfiguration;

import java.util.HashMap;
import java.util.Map;

import static ru.sberbank.bigdata.dffess.vspc.constant.TablesName.*;
import static ru.sberbank.bigdata.essa.common.master.app.forApp.SparkConfKeeper.ResourceConfig.HARD;
import static ru.sberbank.bigdata.essa.common.master.app.forApp.SparkConfKeeper.ResourceConfig.LITE;
import static ru.sberbank.bigdata.essa.common.slave.app.CtlStatService.StatisticsType.DATA_PRODUCT;

public class VspcDataProductGenerator implements DataProductGenerator {

    @Override
    public DataProduct generate(String instanceCode) {

        EtlKit preloadKit = preloadKit();
        EtlKit snpKit = snpKit(preloadKit);

        Map<String, String> statMap = new HashMap<>();

        statMap.put("2", "");
        statMap.put("12", "");

        EtlKit finisher = EtlKit.newInstance("finisher", LITE)
                .addDependency(preloadKit)
                .addDependency(snpKit)
                .needToPostStatistic(statMap, DATA_PRODUCT)
                .addConfiguration(FinalConfiguration.class);

        return DataProduct.newInstance(new VspcEntityProvider(), new VspcEtlSeqServiceImpl())
                .addEtlKit(preloadKit)
                .addEtlKit(snpKit)
                .addEtlKit(finisher);
    }

    private static EtlKit preloadKit() {
        Etl chgdivh = Etl.newInstance(CHG_DIVH, ChgDivHConfiguration.class, LITE);

        return EtlKit.newInstance("stage", HARD)
            .addEtl(chgdivh);
    }

    private static EtlKit snpKit(EtlKit preload) {
        Etl addr = Etl.newInstance(ADDR, AddrConfiguration.class, LITE).addDependency(preload);
        Etl addrh = Etl.newInstance(ADDRH, AddrHConfiguration.class, LITE).addDependency(preload);
        Etl div = Etl.newInstance(DIV, DivConfiguration.class, LITE).addDependency(preload);
        Etl divdivrelation = Etl.newInstance(DIVDIVRELATION, DivDivRelationConfiguration.class, LITE).addDependency(preload);
        Etl divh = Etl.newInstance(DIVH, DivHConfiguration.class, LITE).addDependency(preload);
        Etl divtype = Etl.newInstance(DIVTYPE, DivTypeConfiguration.class, LITE).addDependency(preload);
        Etl terrdivtype = Etl.newInstance(TERRDIVTYPE, TerrDivTypeConfiguration.class, LITE).addDependency(preload);
        Etl timezone = Etl.newInstance(TIMEZONE, TimeZoneConfiguration.class, LITE).addDependency(preload);

        addrh = addrh.addDependency(addr);
        div = div.addDependency(timezone).addDependency(addr).addDependency(addrh);
        divh = divh.addDependency(div).addDependency(addr).addDependency(addrh);
        divtype = divtype.addDependency(div).addDependency(divh).addDependency(terrdivtype);
        divdivrelation = divdivrelation.addDependency(div).addDependency(divh);

        return EtlKit.newInstance("vspc_idl", HARD)
                .addEtl(timezone)
                .addEtl(addr)
                .addEtl(addrh)
                .addEtl(div)
                .addEtl(divh)
                .addEtl(divtype)
                .addEtl(terrdivtype)
                .addEtl(divdivrelation);
    }
}
