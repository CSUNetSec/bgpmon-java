package edu.colostate.netsec.session;

import java.util.List;
import java.util.Map;

import edu.colostate.netsec.BgpmonOuterClass;
import edu.colostate.netsec.BgpmonOuterClass.SessionType;

import com.datastax.driver.core.Cluster;

public class CassandraSession extends Session {
    private final int PORT = 9042;
    private BgpmonOuterClass.CassandraSession protobufSession;
    protected com.datastax.driver.core.Session session;

    public CassandraSession(String sessionId, BgpmonOuterClass.CassandraSession protobufSession, Map<String, Session> sessions) {
        super(sessionId, sessions);
        this.protobufSession = protobufSession;

        Cluster cluster = Cluster.builder()
                                .addContactPoints(protobufSession.getHostList().toArray(new String[protobufSession.getHostList().size()]))
                                .withPort(PORT)
                                .withCredentials(protobufSession.getUsername(), protobufSession.getPassword())
                                .build();

        session = cluster.connect();
    }

    @Override
    public BgpmonOuterClass.Session getProtobufSession() {
        BgpmonOuterClass.Session session = BgpmonOuterClass.Session.newBuilder()
                                                        .setSessionType(SessionType.CASSANDRA)
                                                        .setCassandraSession(this.protobufSession)
                                                        .setSessionId(this.sessionId)
                                                        .build();

        return session;
    }

    @Override
    public void destroy() {
        session.close();
    }
}
