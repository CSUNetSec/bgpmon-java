package edu.colostate.netsec.session;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import edu.colostate.netsec.BgpmonOuterClass;
import edu.colostate.netsec.BgpmonOuterClass.BGPUpdate;
import edu.colostate.netsec.BgpmonOuterClass.SessionType;
import edu.colostate.netsec.BgpmonOuterClass.WriteRequest;
import edu.colostate.netsec.BgpmonOuterClass.WriteType;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

public class CassandraSession extends Session {
    private final int UPDATE_MESSAGES_BY_TIME = 1;
    private final String UPDATE_MESSAGES_BY_TIME_STMT = "INSERT INTO csu_bgp_core.update_messages_by_time(time_bucket, timestamp, advertised_prefixes, as_path, collector_ip_address, collector_mac_address, collector_port, next_hop, peer_ip_address, withdrawn_prefixes) VALUES(?,?,?,?,?,?,?,?,?,?)";
    private final int AS_NUMBER_BY_PREFIX_RANGE = 2;
    private final String AS_NUMBER_BY_PREFIX_RANGE_STMT = "INSERT INTO csu_bgp_derived.as_number_by_prefix_range(time_bucket, prefix_ip_address, prefix_mask, timestamp, as_number) VALUES(?,?,?,?,?)";

    private final int PORT = 9042;
    protected final com.datastax.driver.core.Session session;
    protected final Map<String, Map<Integer, PreparedStatement>> writeTokens = new HashMap<String, Map<Integer, PreparedStatement>>();
    private BgpmonOuterClass.CassandraSession protobufSession;

    public CassandraSession(String sessionId, BgpmonOuterClass.CassandraSession protobufSession, Map<String, Session> sessions) {
        super(sessionId, sessions);
        this.protobufSession = protobufSession;

        //connect to cassandra session
        Cluster cluster = Cluster.builder()
                                .addContactPoints(protobufSession.getHostList().toArray(new String[protobufSession.getHostList().size()]))
                                .withPort(PORT)
                                .withCredentials(protobufSession.getUsername(), protobufSession.getPassword())
                                .build();

        session = cluster.connect();
    }

    public com.datastax.driver.core.Session getDatastaxSession() {
        return this.session;
    }

    @Override
    public void write(String writeToken, WriteRequest request) {
        switch(request.getWriteType()) {
            case BGP_UPDATE:
                BGPUpdate bgpUpdate = request.getBgpUpdate();
                Map<String, PreparedStatement> statements = writeTokens.get(writeToken);
                if(statements == null) {
                    //TODO throw new Exception("Unintialized Write Token");
                }

                //loop over statements to insert data
                for(Map.Entry<Integer, PreparedStatement> entry : statements.entrySet()) {
                    BoundStatement bound = null;
                    switch(entry.getKey()) {
                        case UPDATE_MESSAGES_BY_TIME:
                            bound = entry.getValue().bind(
                                        //time_bucket
                                        //timestamp
                                        //advertised_prefixes
                                        //as_path
                                        //collector_ip_address
                                        //collector_mac_address
                                        //collector_port
                                        //next_hop
                                        //peer_ip_address
                                        //withdrawn_prefixes
                                    );
                            break;
                        case AS_NUMBER_BY_PREFIX_RANGE:
                            bound = entry.getValue().bind(
                                        //time_bucket
                                        //prefix_ip_address
                                        //prefix_mask
                                        //timestamp
                                        //as_number
                                    );

                            break;
                        default:
                            //TODO throw new Exception("Unknown Statement Type");
                    }

                    session.execute(bound);
                }
                break;
            default:
                //TODO throw new Exception("Unsupported Write Type");
        }
    }

    @Override
    public String generateWriteToken(WriteType writeType) {
        Map<String, PreparedStatement> statements = new HashMap<String, PreparedStatement>();

        //initialize prepared statements for write type
        switch(writeType) {
            case BGP_UPDATE:
                statements.put(UPDATE_MESSAGES_BY_TIME, session.prepare(UPDATE_MESSAGES_BY_TIME_STMT));
                statements.put(AS_NUMBER_BY_PREFIX_RANGE, session.prepare(AS_NUMBER_BY_PREFIX_RANGE_STMT));
                break;
            default:
                //TODO throw new Exception("Unsupported WriteType");
        }

        //add token to writeTokens
        writeTokens.put(UUID.randomUUID().toString(), statements);
        return writeToken;
    }

    @Override
    public void destroyWriteToken(String writeToken) {
        writeTokens.remove(writeToken);
        //doesn't currently need to do anything
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
