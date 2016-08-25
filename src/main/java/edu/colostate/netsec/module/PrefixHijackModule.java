package edu.colostate.netsec.module;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.colostate.netsec.BgpmonOuterClass;
import edu.colostate.netsec.BgpmonOuterClass.IPPrefix;
import edu.colostate.netsec.BgpmonOuterClass.ModuleType;
import edu.colostate.netsec.BgpmonOuterClass.MonitorIPPrefix;

public class PrefixHijackModule extends Module {
    private BgpmonOuterClass.PrefixHijackModule protobufModule;
    private List<MonitorPrefix> monitorPrefixes;

    public PrefixHijackModule(String moduleId, BgpmonOuterClass.PrefixHijackModule protobufModule, Map<String, Module> modules) {
        super(moduleId, modules);
        this.protobufModule = protobufModule;

        this.monitorPrefixes = new LinkedList<MonitorPrefix>();
        for(MonitorIPPrefix monitorIPPrefix : protobufModule.getMonitorPrefixList()) {
            IPPrefix ipPrefix = monitorIPPrefix.getIpPrefix();

            this.monitorPrefixes.add(
                new MonitorPrefix(
                    monitorIPPrefix.getAsNumber(),
                    ipPrefix.getPrefixIpAddress(),
                    ipPrefix.getPrefixMask()
                )
            );
        }
    }

    @Override
    public void execute() {
        System.out.println("EXECUTING PREFIX HIJACK MODULE");
        for(MonitorPrefix monitorPrefix : this.monitorPrefixes) {
            System.out.println("\tTODO process " + monitorPrefix);
        }
    }

    @Override
    public BgpmonOuterClass.Module getProtobufModule() {
        BgpmonOuterClass.Module module = BgpmonOuterClass.Module.newBuilder()
                                                        .setModuleType(ModuleType.PREFIX_HIJACK)
                                                        .setPrefixHijackModule(this.protobufModule)
                                                        .setModuleId(this.moduleId)
                                                        .build();

        return module;
    }

    @Override
    public void destroy() {
        
    }

    private class MonitorPrefix {
        public int asNumber, prefixMask;
        public String inetAddress;

        public MonitorPrefix(int asNumber, String inetAddress, int prefixMask) {
            this.asNumber = asNumber;
            this.inetAddress = inetAddress;
            this.prefixMask = prefixMask;
        }

        @Override
        public String toString() {
            return asNumber + ":" + inetAddress + "/" + prefixMask;
        }
    }
}
