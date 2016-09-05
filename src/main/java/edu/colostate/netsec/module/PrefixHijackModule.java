package edu.colostate.netsec.module;

import java.net.InetAddress;
import java.lang.StringBuilder;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;

import edu.colostate.netsec.BgpmonOuterClass;
import edu.colostate.netsec.BgpmonOuterClass.IPPrefix;
import edu.colostate.netsec.BgpmonOuterClass.ModuleType;
import edu.colostate.netsec.BgpmonOuterClass.MonitorIPPrefix;
import edu.colostate.netsec.session.Session;
import edu.colostate.netsec.session.CassandraSession;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

public class PrefixHijackModule extends Module {
    private static final String AS_NUMBER_BY_PREFIX_RANGE = "SELECT timestamp, dateOf(timestamp), prefix_ip_address, prefix_mask, as_number FROM csu_bgp_derived.as_number_by_prefix_range WHERE time_bucket=? AND prefix_ip_address>=? AND prefix_ip_address<=?";
    private static final String UPDATE_MESSAGES_BY_TIME = "SELECT collector_ip_address, as_path FROM csu_bgp_core.update_messages_by_time WHERE time_bucket=? and timestamp=?";

    private com.datastax.driver.core.Session session;
    private PreparedStatement asNumberByPrefixRange, updateMessagesByTime;
    private BgpmonOuterClass.PrefixHijackModule protobufModule;
    private Map<UUID, PrefixHijack> potentialHijacks = new HashMap<UUID, PrefixHijack>();
    private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private List<MonitorPrefix> monitorPrefixes = new LinkedList<MonitorPrefix>();

    public PrefixHijackModule(String moduleId, Session session, BgpmonOuterClass.PrefixHijackModule protobufModule, Map<String, Module> modules, String logDirectory) {
        super(moduleId, modules, logDirectory);
        this.protobufModule = protobufModule;

        //prepare queries
        if(session instanceof CassandraSession) {
            this.session = ((CassandraSession)session).getDatastaxSession();
            asNumberByPrefixRange = this.session.prepare(AS_NUMBER_BY_PREFIX_RANGE);
            updateMessagesByTime = this.session.prepare(UPDATE_MESSAGES_BY_TIME);
        } else {
            //TODO throw Exception("");
        }

        //initialize monitored prefixes
        this.monitorPrefixes = new LinkedList<MonitorPrefix>();
        for(MonitorIPPrefix monitorIPPrefix : protobufModule.getMonitorPrefixList()) {
            IPPrefix ipPrefix = monitorIPPrefix.getIpPrefix();

            InetAddress inetAddress = null;
            try {
                inetAddress = InetAddress.getByName(ipPrefix.getPrefixIpAddress().trim());
            } catch(Exception e) {
                System.err.println("Unable to parse IP Address");
                //TODO handle exception
            }

            //search for inetAddress and mask in existing prefixes
            MonitorPrefix monitorPrefix = null;
            for(MonitorPrefix monitorPrefixIter : this.monitorPrefixes) {
                if(monitorPrefixIter.inetAddress == inetAddress && monitorPrefixIter.mask == ipPrefix.getPrefixMask()) {
                    monitorPrefix = monitorPrefixIter;
                    break;
                }   
            }

            if(monitorPrefix == null) {
                this.monitorPrefixes.add(
                    new MonitorPrefix(
                        monitorIPPrefix.getAsNumber(),
                        inetAddress,
                        ipPrefix.getPrefixMask()
                    )
                );
            } else {
                monitorPrefix.addAsNumber(monitorIPPrefix.getAsNumber());
            }
        }
    }

    @Override
    public void execute() {
        long currentTime = System.currentTimeMillis();
        Timestamp timeBucket = new Timestamp(currentTime - (currentTime % (86400 * 1000)));

        //loop over monitored prefixes
        for(MonitorPrefix monitorPrefix : this.monitorPrefixes) {
            //bind prepared statement
            BoundStatement bound = asNumberByPrefixRange.bind(timeBucket, monitorPrefix.minInetAddress, monitorPrefix.maxInetAddress);

            //execute bound statement and add callback for completion
            ResultSetFuture future = this.session.executeAsync(bound);
            Futures.addCallback(
                future,
                new FutureCallback<ResultSet>() {
                    @Override
                    public void onSuccess(ResultSet resultSet) {
                        Iterator<Row> iterator = resultSet.iterator();
                        while(iterator.hasNext()) {
                            Row row = iterator.next();

                            //check if valid advertisement
                            long asNumber = row.getLong("as_number");
                            if(monitorPrefix.asNumbers.contains(asNumber)) {
                                continue;
                            }

                            //retrieve additional row eleemnts
                            UUID timeuuid = row.getUUID("timestamp");
                            Date timestamp = row.getTimestamp(1);
                            InetAddress inetAddress = row.getInet("prefix_ip_address");
                            int mask = row.getInt("prefix_mask");

                            //check for existance of potential hijack
                            rwl.readLock().lock();
                            try {
                                if(potentialHijacks.containsKey(timeuuid)) {
                                    continue;
                                }
                            } finally {
                                rwl.readLock().unlock();
                            }

                            //add hijack to potential hijacks
                            rwl.writeLock().lock();
                            try {
                                //query csu_bgp_core.update_messages_by_time for additional imformation
                                BoundStatement bound = updateMessagesByTime.bind(timeBucket, timeuuid);
                                Row coreRow = null;
                                try {
                                    ResultSet coreResultSet= session.executeAsync(bound).get();
                                    coreRow = coreResultSet.one();
                                } catch(Exception e) {
                                    //logger.log(Level.WARNING, "unable to complete csu_bgp_core.update_messages_by_time query");
                                    //TODO e.printStackTrace();
                                    continue;
                                }

                                if(coreRow == null) {
                                    continue; //TODO remove this once all data is inserted with this application (timeuuid's will match between update_messages_by_time and as_number_by_prefix_range
                                }

                                InetAddress collectorIp = coreRow.getInet("collector_ip_address");
                                List<Long> asPath = coreRow.getList("as_path", Long.class);

                                potentialHijacks.put(timeuuid, new PrefixHijack(timestamp, inetAddress, mask));
                                logger.log(Level.INFO, "\tHIJACK!:" + timeuuid + " -- " + asNumber + ":" + inetAddress + "/" + mask + " - " + timestamp + " FOR MONITORED " + monitorPrefix);
                            } finally {
                                rwl.writeLock().unlock();
                            }
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        //logger.log(Level.WARNING, "unable to complete csu_bgp_derived.as_number_by_prefix_range query");
                        //t.printStackTrace();
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
        public List<Long> asNumbers = new LinkedList<Long>();
        public int mask;
        public InetAddress inetAddress, minInetAddress, maxInetAddress;

        public MonitorPrefix(long asNumber, InetAddress inetAddress, int mask) {
            this.asNumbers.add(asNumber);
            this.inetAddress = inetAddress;
            this.mask = mask;

            try {
                //generate mask
                int minInt = 0xffffffff << (32 - mask);
                byte[] maskBytes = new byte[]{(byte)(minInt >>> 24), (byte)(minInt >> 16 & 0xff), (byte)(minInt >> 8 & 0xff), (byte)(minInt & 0xff)};

                //compute minimum ip address
                byte[] minBytes = this.inetAddress.getAddress();
                for(int i=0; i<minBytes.length; i++) {
                    minBytes[i] = (byte)(minBytes[i] & maskBytes[i]);
                }
                this.minInetAddress = InetAddress.getByAddress(minBytes);

                //compute maximum ip address
                byte[] maxBytes = this.inetAddress.getAddress();
                for(int i=0; i<maxBytes.length; i++) {
                    maxBytes[i] = (byte)(maxBytes[i] | (maskBytes[i] ^ 0xff));
                }
                this.maxInetAddress = InetAddress.getByAddress(maxBytes);
            } catch(Exception e) {
                //TODO handle exception on failure to parse inet address
            }
        }

        public void addAsNumber(long asNumber) {
            this.asNumbers.add(asNumber);
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            boolean first = true;
            for(Long asNumber : asNumbers) {
                stringBuilder.append((first ? "" : ",") + asNumber);
                first = false;
            }

            stringBuilder.append(":" + inetAddress + "/" + mask);
            return stringBuilder.toString();
        }
    }

    private class PrefixHijack {
        public Date timestamp;
        public InetAddress inetAddress;
        public int mask;

        public PrefixHijack(Date timestamp, InetAddress inetAddress, int mask) {
            this.timestamp = timestamp;
            this.inetAddress = inetAddress;
            this.mask = mask;
        }
    }
}
