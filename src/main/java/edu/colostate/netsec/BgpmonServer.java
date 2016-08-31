package edu.colostate.netsec;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import edu.colostate.netsec.BgpmonGrpc.BgpmonImplBase;
import edu.colostate.netsec.BgpmonOuterClass;
import edu.colostate.netsec.BgpmonOuterClass.Empty;
import edu.colostate.netsec.BgpmonOuterClass.CloseSessionRequest;
import edu.colostate.netsec.BgpmonOuterClass.CloseSessionReply;
import edu.colostate.netsec.BgpmonOuterClass.ListModulesRequest;
import edu.colostate.netsec.BgpmonOuterClass.ListModulesReply;
import edu.colostate.netsec.BgpmonOuterClass.ListSessionsRequest;
import edu.colostate.netsec.BgpmonOuterClass.ListSessionsReply;
import edu.colostate.netsec.BgpmonOuterClass.WriteRequest;
import edu.colostate.netsec.BgpmonOuterClass.SessionType;;
import edu.colostate.netsec.BgpmonOuterClass.RunModuleReply;;
import edu.colostate.netsec.BgpmonOuterClass.RunModuleRequest;;
import edu.colostate.netsec.BgpmonOuterClass.StartModuleReply;;
import edu.colostate.netsec.BgpmonOuterClass.StartModuleRequest;;
import edu.colostate.netsec.BgpmonOuterClass.StopModuleReply;;
import edu.colostate.netsec.BgpmonOuterClass.StopModuleRequest;;
import edu.colostate.netsec.BgpmonOuterClass.OpenSessionReply;
import edu.colostate.netsec.BgpmonOuterClass.OpenSessionRequest;
import edu.colostate.netsec.module.Module;
import edu.colostate.netsec.module.PrefixHijackModule;
import edu.colostate.netsec.session.CassandraSession;
import edu.colostate.netsec.session.Session;

public class BgpmonServer {
    private final Server server;

    public static void main(String[] args) throws Exception {
        BgpmonServer server = new BgpmonServer(12289);
        server.start();
        server.blockUntilShutdown();
    }

    public BgpmonServer(int port) {
        this.server = ServerBuilder.forPort(port)
                                .addService(new BgpmonService())
                                .build();
    }

    public void start() throws IOException {
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("shutting down BgpmonServer since JVM is shutting down");
                BgpmonServer.this.stop();
            }
        });
    }

    public void stop() {
        if(server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if(server != null) {
            server.awaitTermination();
        }
    }

    private static class BgpmonService extends BgpmonImplBase {
        private Map<String, Session> sessions;
        private Map<String, Module> runningModules;
        private Map<String, Module> startedModules;

        public BgpmonService() {
            this.sessions = new HashMap<String, Session>();
            this.runningModules = new HashMap<String, Module>();
            this.startedModules = new HashMap<String, Module>();
        }

        /**
         * Session Methods
         */
        @Override
        public void closeSession(CloseSessionRequest request, StreamObserver<CloseSessionReply> responseObserver) {
            Session session = this.sessions.get(request.getSessionId());
            if(session != null) {
                session.selfDestruct();
            }

            CloseSessionReply reply = CloseSessionReply.newBuilder()
                                                    .setSuccess(session != null)
                                                    .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void listSessions(ListSessionsRequest request, StreamObserver<ListSessionsReply> responseObserver) {
            ListSessionsReply.Builder replyBuilder = ListSessionsReply.newBuilder();
            List<String> sessionIds = request.getSessionIdList();

            for(String sessionId : this.sessions.keySet()) {
                if(sessionIds.size() == 0 || sessionIds.contains(sessionId)) {
                    replyBuilder.addSession(this.sessions.get(sessionId).getProtobufSession());
                }
            }

            responseObserver.onNext(replyBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void openSession(OpenSessionRequest request, StreamObserver<OpenSessionReply> responseObserver) {
            Session session = null;
            switch(request.getSessionType()) {
                case CASSANDRA:
                    session = new CassandraSession(request.getSessionId(), request.getCassandraSession(), this.sessions);
                    break;
                default:
                    //TODO throw new Exception("Unknown Session Type");
            }

            sessions.put(request.getSessionId(), session);

            OpenSessionReply reply = OpenSessionReply.newBuilder()
                                                    .setSuccess(true)
                                                    .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        /**
         * Module Methods
         */

        @Override
        public void listModules(ListModulesRequest request, StreamObserver<ListModulesReply> responseObserver) {
            ListModulesReply.Builder replyBuilder = ListModulesReply.newBuilder();
            List<String> moduleIds = request.getModuleIdList();

            for(String moduleId : this.runningModules.keySet()) {
                if(moduleIds.size() == 0 || moduleIds.contains(moduleId)) {
                    replyBuilder.addRunningModule(this.runningModules.get(moduleId).getProtobufModule());
                }
            }

            for(String moduleId : this.startedModules.keySet()) {
                if(moduleIds.size() == 0 || moduleIds.contains(moduleId)) {
                    replyBuilder.addStartedModule(this.startedModules.get(moduleId).getProtobufModule());
                }
            }

            responseObserver.onNext(replyBuilder.build());
            responseObserver.onCompleted();           
        }

        @Override
        public void runModule(RunModuleRequest request, StreamObserver<RunModuleReply> responseObserver) {
            Module module = null;
            switch(request.getModuleType()) {
                case PREFIX_HIJACK:
                    Session session = sessions.get(request.getPrefixHijackModule().getSessionId());
                    if(session == null) {
                        //TODO throw new Exception("Unknown Session Id");
                    }

                    module = new PrefixHijackModule(request.getModuleId(), session, request.getPrefixHijackModule(), runningModules);
                    break;
                default:
                    //TODO throw new Exception("Uknown Module Type");
            }

            runningModules.put(request.getModuleId(), module);
            module.execute(); //TODO get return value from execute
            module.selfDestruct();

            RunModuleReply reply = RunModuleReply.newBuilder()
                                                .setSuccess(true)
                                                .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void startModule(StartModuleRequest request, StreamObserver<StartModuleReply> responseObserver) {
            Module module = null;
            switch(request.getModuleType()) {
                case PREFIX_HIJACK:
                    Session session = sessions.get(request.getPrefixHijackModule().getSessionId());
                    if(session == null) {
                        //TODO throw new Exception("Unknown Session Id");
                    }

                    module = new PrefixHijackModule(request.getModuleId(), session, request.getPrefixHijackModule(), startedModules);
                    break;
                default:
                    //TODO throw new Exception("Unknown Module Type");
            }

            startedModules.put(request.getModuleId(), module);
            module.schedule(request.getExecutionDelay());

            StartModuleReply reply = StartModuleReply.newBuilder()
                                                    .setSuccess(true)
                                                    .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void stopModule(StopModuleRequest request, StreamObserver<StopModuleReply> responseObserver) {
            Module module = this.startedModules.get(request.getModuleId());
            if(module != null) {
                module.selfDestruct();
            }

            StopModuleReply reply = StopModuleReply.newBuilder()
                                                .setSuccess(module != null)
                                                .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        /**
         * Write Methods
         */
        @Override
        public StreamObserver<WriteRequest> write(StreamObserver<Empty> responseObserver) {
            return new StreamObserver<WriteRequest>() {
                @Override
                public void onNext(WriteRequest request) {
                    Session session = sessions.get(request.getSessionId());
                    if(session == null) {
                        //TODO throw new Exception("Unknown Session Id");
                    }

                    session.write(request);
                    /*switch(request.getWriteType()) {
                        case BGP_UPDATE:
                            BGPUpdate bgpUpdate = request.getBgpUpdate();
                            break;
                        default:
                            //TODO throw new Exception("Unknown Write Type");
                    }*/
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            };
        }
    }
}
