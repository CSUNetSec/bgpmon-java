package edu.colostate.netsec.session;

import java.util.List;
import java.util.Map;

import edu.colostate.netsec.BgpmonOuterClass;
import edu.colostate.netsec.BgpmonOuterClass.BGPUpdate;
import edu.colostate.netsec.BgpmonOuterClass.SessionType;
import edu.colostate.netsec.BgpmonOuterClass.WriteRequest;
import edu.colostate.netsec.BgpmonOuterClass.WriteType;

import com.datastax.driver.core.Cluster;

public class CassandraSession extends Session {
    private final String UPDATE_MESSAGES_BY_TIME = "INSERT INTO csu_bgp_core.update_messages_by_time(time_bucket, timestamp, advertised_prefixes, as_path, collector_ip_address, collector_mac_address, collector_port, next_hop, peer_ip_address, withdrawn_prefixes) VALUES(?,?,?,?,?,?,?,?,?,?)";
    private final String AS_NUMBER_BY_PREFIX_RANGE = "INSERT INTO csu_bgp_derived.as_number_by_prefix_range(time_bucket, prefix_ip_address, prefix_mask, timestamp, as_number) VALUES(?,?,?,?,?)";

    private final int PORT = 9042;
    protected final com.datastax.driver.core.Session session;
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
        System.err.println("write unimplemented!");

        switch(request.getWriteType()) {
            case BGP_UPDATE:
                break;
            default:
                //TODO throw new Exception("Unsupported Write Type");
        }
    }

    @Override
    public String generateWriteToken(WriteType writeType) {
        return "";
    }

    @Override
    public void destroyWriteToken(String writeToken) {

    }

    /*@Override
    public void writeBatch(WriteBatchRequest request) {
        switch(request.getWriteType()) {
            case BGP_UPDATE:
                //iniitalize prepared statement
                PreparedStatement preparedTime = this.session.prepare(UPDATE_MESSAGES_BY_TIME);
                PreparedStatement preparedPrefixRange = this.session.prepare(AS_NUMBER_BY_PREFIX_RANGE);

                //loop through messages
                for(BGPUpdate bgpUpdate : request.getBGPUpdateList()) {
                    BoundStatement boundTime = preparedTime.bind(
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

                    BoundStatement boundPrefixRange = preparedPrefixRange.bind(
                        //time_bucket
                        //prefix_ip_address
                        //prefix_mask
                        //timestamp
                        //as_number
                    );

                    this.session.execute(boundTime);
                    this.session.execute(boundPrefixRange);
                }

                break;
            default:
                //TODO throw new Exception("Unsupported Write Type");
        }
    }*/

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
