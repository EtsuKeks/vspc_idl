package ru.sberbank.bigdata.dffess.vspc;

import org.apache.commons.lang3.tuple.Triple;
import ru.sberbank.bigdata.dffess.vspc.constant.VpscConstant;
import ru.sberbank.bigdata.essa.common.germes.CmdArgsProvider;
import ru.sberbank.bigdata.essa.common.germes.WfConfBuilder;
import ru.sberbank.bigdata.essa.common.templates.ThreadConfigProvider;
import ru.sberbank.hermes.core.dmbuilder.config.ThreadConfiguration;
import ru.sberbank.hermes.core.dmbuilder.config.WfConfiguration;
import ru.sberbank.hermes.core.dmbuilder.provider.PatternProvider;
import ru.sberbank.hermes.core.dmbuilder.provider.PatternProvidingException;
import ru.sberbank.hermes.core.dmbuilder.wf.DefaultWorkflow;
import ru.sberbank.hermes.core.dmbuilder.wf.FrameworkWorkflow;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MainClass {

    public static void main(String[] args) throws Exception {

        final String launcherList = "launcher_list";

        CmdArgsProvider cmdArgsProvider = new CmdArgsProvider();
        Map<String, String> cmdArgs = cmdArgsProvider.provideArgs(args);
        cmdArgs.forEach((s1, s2) -> System.out.println(s1 + "=" + s2));

        WfConfBuilder wfConfBuilder = new WfConfBuilder(cmdArgs, VpscConstant.DATA_PRODUCT, VpscConstant.CTL_ENTITY_ID);
        WfConfiguration wfConfiguration = wfConfBuilder.build();

        List<String> tmp = Stream.of(cmdArgs.get(launcherList).split(",")).collect(Collectors.toCollection(ArrayList::new));
        Map<String, String> mainThreads = getRcrsMap();
        List<Triple<String, ThreadConfiguration, Class>> launcherTriplesRcrs = generateTriples(tmp, mainThreads, "ru.sberbank.bigdata.dffess.vspc.tables.", cmdArgs);

        List<Triple<String, ThreadConfiguration, Class>> launcherTriples = launcherTriplesRcrs;

        DefaultWorkflow.Builder frameworkWorkflowBuilder = DefaultWorkflow.builder()
                .setWfConfiguration(wfConfiguration);
        for (Object launcherTriple : launcherTriples) {
            Triple<String, ThreadConfiguration, Class> lt = (Triple<String, ThreadConfiguration, Class>) launcherTriple;
            frameworkWorkflowBuilder.setThreadConf(lt.getLeft(), lt.getMiddle(), lt.getRight());
        }
        FrameworkWorkflow frameworkWorkflow = frameworkWorkflowBuilder.create();
        frameworkWorkflow.launch();

    }

    private static List generateTriples(List<String> tmp, Map<String, String> threads, String packageName, Map<String, String> cmdArgs) {
        List<String> launcherList = new ArrayList<>();
        for (String s : tmp) {
            if (threads.containsKey(s)) {
                launcherList.add(s);
            }
        }

        if (!launcherList.isEmpty()) {
            String confProviderBaseClassName = "ThreadConfigProvider";
            Class<?>[] emptyParams = new Class[0];
            Object[] emptyValues = new Object[0];

            String stgClassBaseClassName = "StgClass";
            Class<?>[] emptySbParams = new Class[0];
            Object[] emptySbValues = new Object[0];
            return launcherList.stream()
                    .map(threadName -> {
                        try {
                            ThreadConfigProvider configProvider = (ThreadConfigProvider) PatternProvider.provide(packageName + threadName.toLowerCase(),
                                    confProviderBaseClassName, threadName.toUpperCase(), emptyParams, emptyValues);
                            return Triple.of(
                                    threadName,
                                    configProvider.provide(cmdArgs),
                                    PatternProvider.provide(packageName + threadName.toLowerCase(), stgClassBaseClassName,
                                            threadName.toUpperCase(), emptySbParams, emptySbValues).getClass()
                            );
                        } catch (PatternProvidingException e) {
                            String errorMsg = "Error while getting table triple";
                            throw new RuntimeException(errorMsg, e);
                        }
                    })
                    .collect(Collectors.toCollection(LinkedList::new));
        }
        return new LinkedList();

    }

    private static Map<String, String> getRcrsMap() {
        Map<String, String> mapRcrs = new HashMap<>();
        mapRcrs.put("addr", "");
        mapRcrs.put("addrh", "");
        mapRcrs.put("div", "");
        mapRcrs.put("divh", "");
        mapRcrs.put("divtype", "");
        mapRcrs.put("terrdivtype", "");
        return mapRcrs;
    }

}