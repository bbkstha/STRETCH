package edu.colostate.cs.fa2017.stretch.groups.X;


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteClosure;

import javax.cache.Cache;
import java.util.*;

public class ClusterMasterY {

    private static final String cacheName = "STRETCH-CACHE";
    private static final String dataRegionName = "150MB_Region";

    private static boolean alreadyRequested = false;




    public static void main(String[] args){

        if(args.length<2){
            return;
        }
        String groupName = args[0];
        Integer numberOfMastersExpected = Integer.parseInt(args[1]);

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName(cacheName);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);

        /*StretchAffinityFunctionXX stretchAffinityFunctionXX = new StretchAffinityFunctionXX(false, 1056);
        cacheConfiguration.setAffinity(stretchAffinityFunctionXX);*/
        cacheConfiguration.setRebalanceMode(CacheRebalanceMode.SYNC);



        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        // Region name.
        regionCfg.setName(dataRegionName);
        // Setting the size of the default memory region to 80MB to achieve this.
        regionCfg.setInitialSize(
                50L * 1024 * 1024);
        regionCfg.setMaxSize(500L * 1024 * 1024);
        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);
        storageCfg.setSystemRegionMaxSize(45L * 1024 * 1024);
        // Setting the data region configuration.
        storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        // Applying the new configuration.
        igniteConfiguration.setDataStorageConfiguration(storageCfg);

        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group",groupName);
            put("role", "master");
            put("donated","no");
            put("region-max", "500");
            put("split","no");
            put("keyToSplit","bbk");
            put("partitionToSplit","1042");
            put("map","./hashmap.ser");

        }};
        igniteConfiguration.setCacheConfiguration(cacheConfiguration);
        igniteConfiguration.setUserAttributes(userAtt);
        igniteConfiguration.setClientMode(true);
        igniteConfiguration.setRebalanceThreadPoolSize(4);


        // Start Ignite node.
        Ignite ignite = Ignition.start(igniteConfiguration);

            ClusterGroup masterGroup = ignite.cluster().forAttribute("role", "master");

            IgniteMessaging mastersMessanger = ignite.message(masterGroup);
            Map<UUID, Object> offerReceived = new HashMap<>();

        ignite.compute(ignite.cluster().forAttribute("role","master").forRemotes()).apply(
                new IgniteClosure<Integer, Integer>() {
                    @Override
                    public Integer apply(Integer x) {

                        String arg = "47.76976,-139.85048,1388538000000";

                        CacheConfiguration tmpCacheConfiguration = new CacheConfiguration("TMP-CACHE");
                        tmpCacheConfiguration.setCacheMode(CacheMode.LOCAL);
                        IgniteCache<DataLoader.GeoEntry, String> tmpCache = ignite.createCache(tmpCacheConfiguration);
                        DataLoader.GeoEntry testKey = new DataLoader.GeoEntry(Double.parseDouble("47.76976"), Double.parseDouble("-139.85048"),12,"1388538000000");

                        int prevPart = ignite.affinity(cacheName).partition(testKey);
                        System.out.println("Previous partition ID is: "+prevPart);

                        IgniteCache<DataLoader.GeoEntry, String> localCache = ignite.cache(cacheName);

                        Iterator<Cache.Entry<DataLoader.GeoEntry, String>> it = localCache.localEntries(CachePeekMode.OFFHEAP).iterator();
                        int i=0;
                        while(it.hasNext()){
                            i++;
                            Cache.Entry<DataLoader.GeoEntry, String> e = it.next();
                            System.out.println("FROM ANOTHER: "+i);
                            tmpCache.put(e.getKey(), e.getValue());
                            System.out.println(localCache.remove(e.getKey()));
                            System.out.println(""+i+". "+e.getKey()+" and value: "+e.getValue());
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                        }
                        boolean flag = true;

                        while(flag){
                            System.out.println("New partition ID is: "+ignite.affinity(cacheName).partition(testKey));
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            if(ignite.affinity(cacheName).partition(testKey) !=prevPart){
                                System.out.println("New partition ID is: "+ignite.affinity(cacheName).partition(testKey));
                                flag = false;
                                System.out.println(flag);
                            }
                        }

                        Iterator<Cache.Entry<DataLoader.GeoEntry, String>> itr = tmpCache.localEntries(CachePeekMode.OFFHEAP).iterator();
                        while(itr.hasNext()) {

                            Cache.Entry<DataLoader.GeoEntry, String> e = itr.next();
                            localCache.put(e.getKey(), e.getValue());
                            tmpCache.remove(e.getKey());
                        }


                        ScanQuery scanQuery = new ScanQuery();
                        scanQuery.setPartition(prevPart);
                        // Execute the query.
                        Iterator<Cache.Entry<DataLoader.GeoEntry, String>> iterator1 = localCache.query(scanQuery).iterator();
                        int c1 = 0;
                        while (iterator1.hasNext()) {
                            Cache.Entry<DataLoader.GeoEntry, String> remainder = iterator1.next();
                            //System.out.println("The remaining key in 330 is: "+x.getKey());
                            localCache.put(remainder.getKey(), remainder.getValue());
                            c1++;
                        }
                        System.out.println(c1);


                        return 1;
                    }
                },
                330
        );
/*
            //All other listeners here!!

            //4.Listen for offer grant
            mastersMessanger.remoteListen(OFFER_GRANTED, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {
                    if (msg.toString().split("::")[0].equals(ignite.cluster().localNode().id())) {

                        String[] param = msg.toString().split("::");
                        String hotPartitions = param[1];
                        String host = param[2];
                        String group = ignite.cluster().localNode().attribute("group");
                        String placeHolder = "_GROUP-NAME_" + "$$" + "_DONATED_" + "$$" + "_HOT-PARTITIONS_";
                        String replacement = group + "##" + "yes" + hotPartitions;
                        FileEditor fileEditor = new FileEditor(configTemplate, placeHolder, replacement, group);
                        String configPath = fileEditor.replace();


                        Collection<Map<String, Object>> hostNames = new ArrayList<>();
                        Map<String, Object> tmpMap = new HashMap<String, Object>() {{
                            put("host", host);
                            put("uname", "bbkstha");
                            put("passwd", "Bibek2753");
                            put("cfg", configPath);
                            put("nodes", 2);
                        }};
                        hostNames.add(tmpMap);
                        Map<String, Object> dflts = null;

                        ignite.cluster().startNodes(hostNames, dflts, false, 10000, 5);
                    }
                    return true;
                }
            });


            //3. Listen for acknowledgement
            mastersMessanger.remoteListen(OFFER_ACKNOWLEDGED, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {

                    if (msg.toString().split("::")[0].equals(ignite.cluster().localNode().id())) {
                        //Transfer data or persist it in the disk from the donated node
                        //Send the final permission
                        Object reply = nodeId + "::" + msg.toString().split("::")[1] + "::" + msg.toString().split("::")[2];
                        mastersMessanger.send(OFFER_GRANTED, reply);
                    }
                    return true;
                }
            });


            //2. Listen for resource offered.
            //Listen to service message. Expect receiving node that has been donated.
            mastersMessanger.remoteListen(OFFER_TOPIC, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {
                    if (!nodeId.equals(ignite.cluster().localNode().id())) {
                        int expectedNumberOfOffers = ignite.cluster().forAttribute("role", "master").nodes().size() - 1;
                        offerReceived.put(nodeId, msg);
                        if (offerReceived.size() < expectedNumberOfOffers) {
                            return true;
                        }

                        String hotPartitions = msg.toString().split("::")[2];
                        //Make selection of hostname to use
                        double usage = 1.0;
                        String host = "";
                        UUID sender = null;
                        for (Map.Entry<UUID, Object> e : offerReceived.entrySet()) {

                            String[] offer = e.getValue().toString().split("::");

                            if (Double.parseDouble(offer[0]) < usage) {
                                usage = Double.parseDouble(offer[0]);
                                host = offer[1];
                                sender = e.getKey();
                            }
                        }
                        Object reply = sender + "::" + hotPartitions + "::" + host;

                        mastersMessanger.send(OFFER_ACKNOWLEDGED, reply);
                    }
                    return true;
                }
            });


            //1. Listen to Resource Request
            mastersMessanger.remoteListen(REQUEST_TOPIC, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {

                    if (!nodeId.equals(ignite.cluster().localNode().id())) {

                        String groupName = ignite.cluster().localNode().attribute("group");
                        ClusterGroup workerCluster = ignite.cluster().forAttribute("group", groupName).forAttribute("role", "worker");
                        if (!workerCluster.nodes().isEmpty()) {
                            Map<String, Integer> nodesPerHost = new HashMap<>();
                            Map<Double, String> usagePerNode = new TreeMap<>();
                            Iterator<ClusterNode> iterator = workerCluster.nodes().iterator();
                            while (iterator.hasNext()) {
                                ClusterNode n = iterator.next();
                                String hostname = n.hostNames().iterator().next();
                                double usage = n.metrics().getNonHeapMemoryUsed() / (double) n.metrics().getNonHeapMemoryTotal();


                                if (!nodesPerHost.containsKey(hostname)) {
                                    nodesPerHost.put(hostname, 1);
                                    usagePerNode.put(usage, hostname);
                                } else {
                                    nodesPerHost.put(hostname, nodesPerHost.get(hostname) + 1);
                                }
                            }
                            //This is the host that can be used to spawn new JVM.
                            Double usage = ((TreeMap<Double, String>) usagePerNode).firstKey();
                            String idleHost = ((TreeMap<Double, String>) usagePerNode).firstEntry().getValue();
                            String hotPartitions = msg.toString();
                            Object offer = (Object) usage + "::" + idleHost + "::" + hotPartitions;
                            mastersMessanger.send(OFFER_TOPIC, offer);
                        }
                    }
                    return true;
                }
            });
*/

    }
}
