package edu.colostate.netsec.session;

import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import edu.colostate.netsec.BgpmonOuterClass;
import edu.colostate.netsec.BgpmonOuterClass.BGPUpdate;
import edu.colostate.netsec.BgpmonOuterClass.IPPrefix;
import edu.colostate.netsec.BgpmonOuterClass.SessionType;
import edu.colostate.netsec.BgpmonOuterClass.WriteRequest;
import edu.colostate.netsec.BgpmonOuterClass.WriteType;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.utils.UUIDs;

public class CassandraSession extends Session {
    private final int UPDATE_MESSAGES_BY_TIME = 1;
    private final String UPDATE_MESSAGES_BY_TIME_STMT = "INSERT INTO csu_bgp_core.update_messages_by_time(time_bucket, timestamp, advertised_prefixes, as_path, collector_ip_address, collector_mac_address, collector_port, next_hop, peer_ip_address, withdrawn_prefixes) VALUES(?,?,?,?,?,?,?,?,?,?)";
    private final int AS_NUMBER_BY_PREFIX_RANGE = 2;
    private final String AS_NUMBER_BY_PREFIX_RANGE_STMT = "INSERT INTO csu_bgp_derived.as_number_by_prefix_range(time_bucket, prefix_ip_address, prefix_mask, timestamp, as_number) VALUES(?,?,?,?,?)";

    private final int PORT = 9042;
    protected final com.datastax.driver.core.Session session;
    protected final Map<String, Map<Integer, PreparedStatement>> writeTokens = new HashMap<String, Map<Integer, PreparedStatement>>();
    protected Random random = new Random();
    protected BgpmonOuterClass.CassandraSession protobufSession;

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
                Map<Integer, PreparedStatement> statements = writeTokens.get(writeToken);
                if(statements == null) {
                    //TODO throw new Exception("Unintialized Write Token");
                }

                long timestampMillis = bgpUpdate.getTimestamp();
                Timestamp timeBucket = new Timestamp(timestampMillis - (timestampMillis % (86400 * 1000)));
                UUID timestamp = new UUID(UUIDs.startOf(timestampMillis).getMostSignificantBits(), random.nextLong());

                //loop over statements to insert data
                for(Map.Entry<Integer, PreparedStatement> entry : statements.entrySet()) {
                    BatchStatement batch = new BatchStatement();
                    switch(entry.getKey()) {
                        case UPDATE_MESSAGES_BY_TIME:
                            try {
                                batch.add(
                                    entry.getValue().bind(
                                        timeBucket, //time_bucket
                                        timestamp, //timestamp
                                        null, //TODO advertised_prefixes
                                        bgpUpdate.getAsPathList(), //as_path
                                        InetAddress.getByName(bgpUpdate.getCollectorIpAddress()), //collector_ip_address
                                        bgpUpdate.getCollectorMacAddress(), //collector_mac_address
                                        bgpUpdate.getCollectorPort(), //collector_port
                                        bgpUpdate.getNextHop() != null ? InetAddress.getByName(bgpUpdate.getNextHop()) : null, //next_hop
                                        InetAddress.getByName(bgpUpdate.getPeerIpAddress()), //peer_ip_address
                                        null //TODO withdrawn_prefixes
                                    )
                                );
                            } catch(Exception e) {
                                e.printStackTrace();
                                //TODO throw new Exception("");
                            }

                            break;
                        case AS_NUMBER_BY_PREFIX_RANGE:
                            List<Integer> asPath = bgpUpdate.getAsPathList();
                            int asNumber = asPath.get(asPath.size() - 1);

                            for(IPPrefix ipPrefix : bgpUpdate.getAdvertisedPrefixList()) {
                                try {
                                    batch.add(
                                        entry.getValue().bind(
                                            timeBucket, //time_bucket
                                            InetAddress.getByName(ipPrefix.getPrefixIpAddress()), //prefix_ip_address
                                            ipPrefix.getPrefixMask(), //prefix_mask
                                            timestamp, //timestamp
                                            asNumber //as_number
                                        )
                                    );
                                } catch(Exception e) {
                                    e.printStackTrace();
                                    //TODO throw new Exception("");
                                }
                            }

                            break;
                        default:
                            //TODO throw new Exception("Unknown Statement Type");
                    }

                    session.execute(batch);
                }
                break;
            default:
                //TODO throw new Exception("Unsupported Write Type");
        }
    }

    @Override
    public String generateWriteToken(WriteType writeType) {
        Map<Integer, PreparedStatement> statements = new HashMap<Integer, PreparedStatement>();

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
        String writeToken = UUID.randomUUID().toString();
        writeTokens.put(writeToken, statements);
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
