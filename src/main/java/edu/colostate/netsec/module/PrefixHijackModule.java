package edu.colostate.netsec.module;

import java.net.InetAddress;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.colostate.netsec.BgpmonOuterClass;
import edu.colostate.netsec.BgpmonOuterClass.IPPrefix;
import edu.colostate.netsec.BgpmonOuterClass.ModuleType;
import edu.colostate.netsec.BgpmonOuterClass.MonitorIPPrefix;
import edu.colostate.netsec.session.Session;
import edu.colostate.netsec.session.CassandraSession;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

public class PrefixHijackModule extends Module {
    private static final String QUERY = "SELECT prefix_ip_address, prefix_mask, as_number, dateOf(timestamp) FROM csu_bgp_derived.as_number_by_prefix_range WHERE time_bucket=? AND prefix_ip_address>=? AND prefix_ip_address<=?";

    private Session session;
    private BgpmonOuterClass.PrefixHijackModule protobufModule;
    private List<MonitorPrefix> monitorPrefixes;

    public PrefixHijackModule(String moduleId, Session session, BgpmonOuterClass.PrefixHijackModule protobufModule, Map<String, Module> modules) {
        super(moduleId, modules);
        this.session = session;
        this.protobufModule = protobufModule;

        //initialize monitored prefixes
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
        System.out.println("TODO process execute");
        com.datastax.driver.core.Session session = null;
        if(this.session instanceof CassandraSession) {
            session = ((CassandraSession)this.session).getDatastaxSession();
        } else {
            //TODO throw Exception("");
        }

        long currentTime = System.currentTimeMillis();
        Timestamp timeBucket = new Timestamp(currentTime - (currentTime % (86400 * 1000)));

        PreparedStatement prepared = session.prepare(QUERY);

        //loop over monitored prefixes
        for(MonitorPrefix monitorPrefix : this.monitorPrefixes) {
            //bind prepared statement
            BoundStatement bound = prepared.bind(timeBucket, monitorPrefix.minInetAddress, monitorPrefix.maxInetAddress);

            //execute bound statement and add callback for completion
            ResultSetFuture future = session.executeAsync(bound);
            Futures.addCallback(
                future,
                new FutureCallback<ResultSet>() {
                    @Override
                    public void onSuccess(ResultSet resultSet) {
                        System.out.println("TODO handle resultset for " + monitorPrefix + " : " + monitorPrefix.minInetAddress + "-" + monitorPrefix.maxInetAddress);
                    }

                    @Override
                    public void onFailure(Throwable t) {

                    }
                },
                MoreExecutors.sameThreadExecutor()
            );
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
        public InetAddress inetAddress, minInetAddress, maxInetAddress;

        public MonitorPrefix(int asNumber, String inetAddress, int prefixMask) {
            this.asNumber = asNumber;
            this.prefixMask = prefixMask;

            try {
                this.inetAddress = InetAddress.getByName(inetAddress.trim());

                //generate mask
                int minInt = 0xffffffff << (32 - prefixMask);
                byte[] mask = new byte[]{(byte)(minInt >>> 24), (byte)(minInt >> 16 & 0xff), (byte)(minInt >> 8 & 0xff), (byte)(minInt & 0xff)};

                //compute minimum ip address
                byte[] minBytes = this.inetAddress.getAddress();
                for(int i=0; i<minBytes.length; i++) {
                    minBytes[i] = (byte)(minBytes[i] & mask[i]);
                }
                this.minInetAddress = InetAddress.getByAddress(minBytes);

                //compute maximum ip address
                byte[] maxBytes = this.inetAddress.getAddress();
                for(int i=0; i<maxBytes.length; i++) {
                    maxBytes[i] = (byte)(maxBytes[i] | (mask[i] ^ 0xff));
                }
                this.maxInetAddress = InetAddress.getByAddress(maxBytes);
            } catch(Exception e) {
                //TODO handle exception on failure to parse inet address
            }
        }

        @Override
        public String toString() {
            return asNumber + ":" + inetAddress + "/" + prefixMask;
        }
    }
}
