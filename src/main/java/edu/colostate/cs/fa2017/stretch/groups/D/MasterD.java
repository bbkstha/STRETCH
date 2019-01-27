package edu.colostate.cs.fa2017.stretch.groups.D;

import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunction;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;

import java.util.*;

public class MasterD {

    private static final String REQUEST_TOPIC = "Resource_Requested";
    private static final String OFFER_TOPIC = "Resource_Offered";
    private static final String OFFER_ACKNOWLEDGED = "Resource_Acknowledged";
    private static final String OFFER_GRANTED = "Resource_Granted";

    private static UUID myLocalID = null;

    private static final String cacheName = "Stretch-Cache";



    public static void main(String[] args) {

        IgniteConfiguration cfg = new IgniteConfiguration();
        Map<UUID, ClusterNode> nodeMap = new HashMap<>();
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(cacheName);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);


        StretchAffinityFunction stretchAffinityFunction = new StretchAffinityFunction();
        stretchAffinityFunction.setPartitions(2000);
        cacheCfg.setAffinity(stretchAffinityFunction);

        cacheCfg.setRebalanceMode(CacheRebalanceMode.ASYNC);

        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group", "D");
            put("role", "master");
            put("donated","no");
        }};



        cfg.setCacheConfiguration(cacheCfg);
        cfg.setUserAttributes(userAtt);
        cfg.setClientMode(false);
        // Start Ignite node.
        try (Ignite ignite = Ignition.start(cfg)) {


            ClusterGroup clusterGroupD = ignite.cluster().forAttribute("group","D");

            Affinity affinity = ignite.affinity(cacheName);

            //Monitoring partition growth in each workers in the cluster.
            Map<ClusterNode, Map<Integer, Long>> clusterToPartitionInfo = new HashMap<>();
            Iterator<ClusterNode> it = clusterGroupD.nodes().iterator();
            while (it.hasNext()){

                ClusterNode node = it.next();
                int[] partitions = affinity.primaryPartitions(node);
                Map<Integer, Long> partitionToKeyCountMap = new HashMap<>();
                for (int i=0; i< partitions.length; i++){
                    partitionToKeyCountMap.put(partitions[i], stretchAffinityFunction.getPartitionToKeyCount(partitions[i]));
                }
                clusterToPartitionInfo.put(node, partitionToKeyCountMap);
            }

            ClusterGroup clientGroup = ignite.cluster().forAttribute("role", "master");
            myLocalID = ignite.cluster().localNode().id();




            int masterSize = clientGroup.nodes().size();

            System.out.println("The masters size is:" + masterSize);
            IgniteMessaging groupMastersMessage = ignite.message(clientGroup);
            //   ClusterGroup groupDWorkers = ignite.cluster().forAttribute("group", "D").forAttribute("role", "worker");

            //Send the request.
            //First set the Listerner in all masters.
            //  ClusterNode donatedNode = groupDWorkers.forOldest().node();
            // int[] backupPartition = affinity.primaryPartitions(donatedNode);

            final Object[] senderNode = new Object[1];
            final String[] receivedHost = new String[1];

            Map<UUID, Object> allDonationReceived = new HashMap<>();
            TreeMap<Long, UUID> offHeapUsed = new TreeMap<>();


            //Cache
            // ignite.active(true);
            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheCfg);
            // Clear caches before running example.
            cache.clear();

            for (int i = 0; i < 100; i++) {

                cache.put(i, i);
                System.out.println(i);
                cache.get(i);
            }

           


            ignite.cluster().forAttribute("","").forCacheNodes(cacheName);
            //cache.loadCache();


            //Local cache peek to find the size of partitions in the node
            ClusterNode node = ignite.cluster().localNode();


            Map<Integer, Long> partitionToCount = new TreeMap<>();
            //Map<Integer, > partitionToHitInfo = new TreeMap<>();
            int[] partitions = affinity.allPartitions(node);
            for(int i=0; i< partitions.length; i++){
                partitionToCount.put(partitions[i], cache.localMetrics().getOffHeapGets());
                //partitionToHitInfo.put(partitions[i], cache);
            }


            //Listen to service message. Expect receiving node that has been donated.
            groupMastersMessage.remoteListen(OFFER_TOPIC, new IgniteBiPredicate<UUID, String>() {
                @Override
                public boolean apply(UUID nodeId, String msg) {

                    if (!nodeId.equals(ignite.cluster().localNode().id())) {


                        System.out.println("4. Received Node");


                        System.out.println("Received request message [msg=" + msg + ", from=" + nodeId + ']');
                        System.out.println("THE:" + Long.parseLong(msg.toString().split("bbkstha")[1]));

                        allDonationReceived.put(nodeId, msg);
                        offHeapUsed.put(Long.parseLong(msg.toString().split("bbkstha")[1]), nodeId);
                        if (allDonationReceived.size() == masterSize - 1) {
                            System.out.println("Entering");
                            Map.Entry<Long, UUID> longUUIDEntry = offHeapUsed.lastEntry();
                            senderNode[0] = nodeId;
                            receivedHost[0] = allDonationReceived.get(longUUIDEntry.getValue()).toString().split("bbkstha")[0];
                            String se = msg.toString().split("bbkstha")[2];
                            System.out.println("The received host is: " + receivedHost[0]);
                            System.out.println("5. Send Ack");

                            groupMastersMessage.send(OFFER_ACKNOWLEDGED, se);
                        }
                    }
                    return true;
                }
            });


            groupMastersMessage.localListen(OFFER_GRANTED, new IgniteBiPredicate<UUID, UUID>() {
                @Override
                public boolean apply(UUID nodeId, UUID msg) {
                    if (!nodeId.equals(ignite.cluster().localNode().id())) {


                        System.out.println("8. Received Final Grant.");


                        System.out.println("Offer granted to kill node");

                        System.out.println("The host to be killed: " + msg);
                        //Verify the backup has been saved and now it is safe to start that node again.
                        //if (nodeId.equals(senderNode)) {
                        // if(ignite.cluster().forAttribute("group","A").forAttribute("role","worker").nodes().size()>1) {


                        ignite.cluster().stopNodes(Collections.singleton(msg));

                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }


                        Collection<Map<String, Object>> hostNames = new ArrayList<>();
                        Map<String, Object> tmpMap = new HashMap<String, Object>() {{
                            put("host", "cut-bank");
                            put("uname", "bbkstha");
                            put("passwd", "Bibek2753");
                            put("cfg", "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/D/GroupD.xml");
                            put("nodes", 4);
                        }};
                        hostNames.add(tmpMap);
                        Map<String, Object> dflts = null;

                        ignite.cluster().startNodes(hostNames, dflts, false, 10000, 5);
                            //}
                        //}
                    }

                    return true;
                }
            });
        }


//        if(args[0] == "Y"){
//
//            groupMastersMessage.send(REQUEST_TOPIC, "NEED HELP");
//
//        }





//
//        boolean flag = true;
//
//        while(flag){
//            System.out.println("1. Send Request");
//            groupMastersMessage.send(REQUEST_TOPIC, "NEED HELP");
//            flag = false;
//        }

//        ResourceRequest resourceRequest = new ResourceRequest(groupMastersMessage, ignite.cluster().localNode().metrics());
//        Thread t = new Thread(resourceRequest);
//        t.start();


    }

    static <K,V extends Comparable<? super V>> SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
        SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
                new Comparator<Map.Entry<K,V>>() {
                    @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
                        int res = e2.getValue().compareTo(e1.getValue());
                        return res != 0 ? res : 1; // Special fix to preserve items with equal values
                    }
                }
        );
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }
}
