package edu.colostate.netsec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.docopt.Docopt;

import edu.colostate.netsec.BgpmonOuterClass.CassandraSession;
import edu.colostate.netsec.BgpmonOuterClass.CloseSessionRequest;
import edu.colostate.netsec.BgpmonOuterClass.CloseSessionReply;
import edu.colostate.netsec.BgpmonOuterClass.IPPrefix;
import edu.colostate.netsec.BgpmonOuterClass.ListModulesRequest;
import edu.colostate.netsec.BgpmonOuterClass.ListModulesReply;
import edu.colostate.netsec.BgpmonOuterClass.ListSessionsRequest;
import edu.colostate.netsec.BgpmonOuterClass.ListSessionsReply;
import edu.colostate.netsec.BgpmonOuterClass.Module;
import edu.colostate.netsec.BgpmonOuterClass.ModuleType;
import edu.colostate.netsec.BgpmonOuterClass.MonitorIPPrefix;
import edu.colostate.netsec.BgpmonOuterClass.PrefixHijackModule;
import edu.colostate.netsec.BgpmonOuterClass.RunModuleReply;
import edu.colostate.netsec.BgpmonOuterClass.RunModuleRequest;
import edu.colostate.netsec.BgpmonOuterClass.Session;
import edu.colostate.netsec.BgpmonOuterClass.SessionType;
import edu.colostate.netsec.BgpmonOuterClass.StartModuleReply;
import edu.colostate.netsec.BgpmonOuterClass.StartModuleRequest;
import edu.colostate.netsec.BgpmonOuterClass.StopModuleReply;
import edu.colostate.netsec.BgpmonOuterClass.StopModuleRequest;
import edu.colostate.netsec.BgpmonOuterClass.OpenSessionReply;
import edu.colostate.netsec.BgpmonOuterClass.OpenSessionRequest;
import edu.colostate.netsec.BgpmonGrpc.BgpmonStub;
import edu.colostate.netsec.BgpmonGrpc.BgpmonBlockingStub;

public class BgpmonClient {
    private static final String doc = 
        "BgpmonClient.\n"
        + "\n"
        + "Usage:\n"
        + "  BgpmonClient close <session_id>\n"
        + "  BgpmonClient list sessions [<session_ids>...]\n"
        + "  BgpmonClient list modules [<module_ids>...]\n"
        + "  BgpmonClient open cassandra <username> <password> <host>... [--session_id=<sid>]\n"
        + "  BgpmonClient run prefix-hijack <session_id> (--file=<filename> | --prefix=<prefix>)\n"
        + "  BgpmonClient start prefix-hijack <session_id> (--file=<filename> | --prefix=<prefix>) [--delay=<delay>]\n"
        + "  BgpmonClient stop <module_id>\n"
        + "  BgpmonClient write mrt-file <session_id> <filename>\n"
        + "  BgpmonClient (-h | --help)\n"
        + "  BgpmonClient --version\n"
        + "\n"
        + "Options:\n"
        + "  -h --help              Show this screen.\n"
        + "  --version              Show version.\n"
        + "  --session_id=<sid>     Set session ID.\n"
        + "  --delay=<delay>        Execution delay [default: 10].\n"
        + "\n";

    private final ManagedChannel channel;
    private final BgpmonBlockingStub blockingStub;
    private final BgpmonStub asyncStub;

    public static void main(String[] args) {
        Map<String, Object> opts = new Docopt(doc)
                                        .withVersion("BgpmonClient 0.1.0")
                                        .parse(args);

        BgpmonClient client = new BgpmonClient("127.0.0.1", 12289);

        if((Boolean)opts.get("close")) {
            client.close((String)opts.get("<session_id>"));
        } else if((Boolean)opts.get("list")) {
            if ((Boolean)opts.get("modules")) {
                client.listModules((List<String>)opts.get("<modules_ids>..."));
            } else if((Boolean)opts.get("sessions")) {
                client.listSessions((List<String>)opts.get("<session_ids>..."));
            }
        } else if((Boolean)opts.get("open")) {
            if((Boolean)opts.get("cassandra")) {
                client.openCassandraSession((String)opts.get("<username>"), (String)opts.get("<password>"), (List<String>)opts.get("<host>"), (String)opts.get("--session_id"));
            }
        } else if((Boolean)opts.get("run")) {
            if((Boolean)opts.get("prefix-hijack")) {
                client.runPrefixHijackModule((String)opts.get("<module_id>"), (String)opts.get("<session_id>"), (String)opts.get("--file"), (String)opts.get("--prefix"));
            }
        } else if((Boolean)opts.get("start")) {
            if((Boolean)opts.get("prefix-hijack")) {
                client.startPrefixHijackModule(
                    (String)opts.get("<module_id>"),
                    (String)opts.get("<session_id>"),
                    (String)opts.get("--file"),
                    (String)opts.get("--prefix"),
                    Integer.parseInt((String)opts.get("--delay"))
                );
            }
        } else if((Boolean)opts.get("stop")) {
            client.stop((String)opts.get("<module_id>"));
        } else if((Boolean)opts.get("write")) {
            if((Boolean)opts.get("mrt-file")) {
                client.writeMRTFile((String)opts.get("<filename>"));
            }
        }
    }

    public BgpmonClient(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                                    .usePlaintext(true)
                                    .build();

        blockingStub = BgpmonGrpc.newBlockingStub(channel);
        asyncStub = BgpmonGrpc.newStub(channel);
    }

    /**
     * Session Methods
     */
    public void close(String sessionId) {
        CloseSessionRequest request = CloseSessionRequest.newBuilder()
                                                    .setSessionId(sessionId)
                                                    .build();

        CloseSessionReply reply = this.blockingStub.closeSession(request);

        System.out.println("CLOSE SESSION SUCCESS:" + reply.getSuccess());
    }

    public void listSessions(List<String> sessionIds) {
        ListSessionsRequest.Builder requestBuilder = ListSessionsRequest.newBuilder();
        if(sessionIds != null) {
            requestBuilder.addAllSessionId(sessionIds);
        }

        ListSessionsReply reply = this.blockingStub.listSessions(requestBuilder.build());

        System.out.println("****SESSIONS****");
        for(Session session : reply.getSessionList()) {
            System.out.println(session);
        }
    }

    public void openCassandraSession(String username, String password, List<String> host, String sessionId) {
        CassandraSession session = CassandraSession.newBuilder()
                                                .setUsername(username)
                                                .setPassword(password)
                                                .addAllHost(host)
                                                .build();

        OpenSessionRequest request = OpenSessionRequest.newBuilder()
                                                    .setSessionType(SessionType.CASSANDRA)
                                                    .setCassandraSession(session)
                                                    .setSessionId(sessionId != null ? sessionId : UUID.randomUUID().toString())
                                                    .build();

        OpenSessionReply reply = this.blockingStub.openSession(request);

        System.out.println("OPENED SESSION SUCCESS:" + reply.getSuccess());
    }

    /**
     * Module Methods
     */

    public void listModules(List<String> moduleIds) {
        ListModulesRequest.Builder requestBuilder = ListModulesRequest.newBuilder();
        if(moduleIds != null) {
            requestBuilder.addAllModuleId(moduleIds);
        }

        ListModulesReply reply = this.blockingStub.listModules(requestBuilder.build());

        System.out.println("****RUNNING MODULES****");
        for(Module module : reply.getRunningModuleList()) {
            System.out.println(module);
        }

        System.out.println("****STARTED MODULES****");
        for(Module module : reply.getStartedModuleList()) {
            System.out.println(module);
        }
    }

    public void runPrefixHijackModule(String moduleId, String sessionId, String filename, String prefix) {
        PrefixHijackModule module = PrefixHijackModule.newBuilder()
                                                    .addAllMonitorPrefix(processIPPrefix(prefix, filename))
                                                    .setSessionId(sessionId)
                                                    .build();

        RunModuleRequest request = RunModuleRequest.newBuilder()
                                                    .setModuleType(ModuleType.PREFIX_HIJACK)
                                                    .setPrefixHijackModule(module)
                                                    .setModuleId(moduleId != null ? moduleId : UUID.randomUUID().toString())
                                                    .build();

        RunModuleReply reply = this.blockingStub.runModule(request);

        System.out.println("RUN MODULE SUCCESS:" + reply.getSuccess());
    }

    public void startPrefixHijackModule(String moduleId, String sessionId, String filename, String prefix, int executionDelay) {
        PrefixHijackModule module= PrefixHijackModule.newBuilder()
                                                    .addAllMonitorPrefix(processIPPrefix(prefix, filename))
                                                    .setSessionId(sessionId)
                                                    .build();

        StartModuleRequest request = StartModuleRequest.newBuilder()
                                                    .setModuleType(ModuleType.PREFIX_HIJACK)
                                                    .setPrefixHijackModule(module)
                                                    .setModuleId(moduleId != null ? moduleId : UUID.randomUUID().toString())
                                                    .setExecutionDelay(executionDelay)
                                                    .build();

        StartModuleReply reply = this.blockingStub.startModule(request);

        System.out.println("START MODULE SUCCESS:" + reply.getSuccess());
    }

    public void stop(String moduleId) {
        StopModuleRequest request = StopModuleRequest.newBuilder()
                                                    .setModuleId(moduleId)
                                                    .build();

        StopModuleReply reply = this.blockingStub.stopModule(request);

        System.out.println("STOP MODULE SUCCESS:" + reply.getSuccess());
    }

    /**
     * Write Methods
     */
    public void writeMRTFile(String filename) {
        System.out.println("WRITE MRT-FILE UNIMPLEMENTED!");
    }

    /**
     * Helper Methods
     */
    private List<MonitorIPPrefix> processIPPrefix(String prefix, String filename) {
        List<MonitorIPPrefix> list = new LinkedList<MonitorIPPrefix>();

        try {
            if(prefix != null) {
                list.add(parseIPPrefix(prefix));
            }

            if(filename != null) {
                BufferedReader in = new BufferedReader(new FileReader(filename));

                String line;
                while((line = in.readLine()) != null) {
                    list.add(parseIPPrefix(line));
                }

                in.close();
            }
        } catch(Exception e) {
            e.printStackTrace(); //TODO make more robust error handling
        }

        return list;
    }

    private MonitorIPPrefix parseIPPrefix(String line) throws Exception {
        String[] fields = line.split(",");

        IPPrefix ipPrefix = IPPrefix.newBuilder()
                                    .setPrefixIpAddress(fields[1].trim())
                                    .setPrefixMask(Integer.parseInt(fields[2]))
                                    .build();

        MonitorIPPrefix monitorIPPrefix = MonitorIPPrefix.newBuilder()
                                                        .setAsNumber(Integer.parseInt(fields[0]))
                                                        .setIpPrefix(ipPrefix)
                                                        .build();

        return monitorIPPrefix;
    }
}
