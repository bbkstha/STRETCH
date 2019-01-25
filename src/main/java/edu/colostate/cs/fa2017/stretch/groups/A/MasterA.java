package edu.colostate.cs.fa2017.stretch.groups.A;

import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;

import java.util.*;

public class MasterA {

    private static final String REQUEST_TOPIC = "Resource_Requested";
    private static final String OFFER_TOPIC = "Resource_Offered";
    private static final String OFFER_ACKNOWLEDGED = "Resource_Acknowledged";
    private static final String OFFER_GRANTED = "Resource_Granted";

    private static UUID myLocalID = null;

    public static void main(String[] args) {

        Map<UUID, ClusterNode> nodeMap = new HashMap<>();
        String cacheName = "MyCache";
        CacheConfiguration cacheCfg = new CacheConfiguration("MyCacheConfig");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);



        StretchAffinityFunction stretchAffinityFunction = new StretchAffinityFunction();
        stretchAffinityFunction.setPartitions(2000);
        cacheCfg.setAffinity(stretchAffinityFunction);

        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group", "A");
            put("role", "master");
        }};
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setCacheConfiguration(cacheCfg);
        cfg.setUserAttributes(userAtt);
        cfg.setClientMode(true);
        // Start Ignite node.
        Ignite ignite = Ignition.start(cfg);
        //Affinity affinity = ignite.affinity(cacheName);

        ClusterGroup groupACluster = ignite.cluster().forAttribute("group", "A").forAttribute("role", "worker");
        myLocalID = ignite.cluster().localNode().id();
        ClusterGroup clientGroup = ignite.cluster().forAttribute("role", "master");
        IgniteMessaging groupMastersMessage = ignite.message(clientGroup);

        //Send the request.
        //First set the Listerner in all masters.
        ClusterNode donatedNoded = groupACluster.forOldest().node();
        //int[] backupPartition = affinity.primaryPartitions(donatedNode);

        final Object[] senderNode = new Object[1];
        final String[] receivedHost = new String[1];

        ClusterNode donatedNode = groupACluster.forRemotes().node();
        groupACluster.node().hostNames();

        Map<UUID, Object> allDonationReceived = new HashMap<>();
        TreeMap<Long, UUID> offHeapUsed = new TreeMap<>();

        groupMastersMessage.remoteListen(REQUEST_TOPIC, new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {


                System.out.println("The request has been sent! by: "+nodeId+ " while my local ID is: "+ignite.cluster().localNode().id());
                System.out.println("The ids are different: "+nodeId.equals(ignite.cluster().localNode().id()));

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (!nodeId.equals(ignite.cluster().localNode().id())) {
                    System.out.println("2. Received Request");

                    System.out.println("Received request message [msg=" + msg + ", from=" + nodeId + ']');
                    ClusterGroup workerCluster = ignite.cluster().forAttribute("group", "A").forAttribute("role", "worker");
                    if(!workerCluster.nodes().isEmpty()){
                        ClusterNode chosenOne = workerCluster.forRemotes().node();
                        long leastUsedOffHeap = 5;
                        UUID leastUsedNodeID =chosenOne.id();
                        String hostName = chosenOne.hostNames().iterator().next();

                        System.out.println("Sent message: " + hostName + "bbkstha" + leastUsedOffHeap + "bbkstha" + leastUsedNodeID);
                        String msg1 = hostName + "bbkstha" + leastUsedOffHeap + "bbkstha" + leastUsedNodeID;

                        System.out.println("3. Send Node");

                        groupMastersMessage.send(OFFER_TOPIC, msg1);
                        //donatedNode[0] = leastUsedNodeID;

                    }
//                    long leastUsedOffHeap = Long.MAX_VALUE;
//                    Collection<ClusterNode> workerNodes = groupACluster.nodes();
//                    for (ClusterNode worker : workerNodes) {
//                        long tmp = worker.metrics().getNonHeapMemoryUsed();
//                        if (tmp < leastUsedOffHeap) {
//                            leastUsedOffHeap = tmp;
//                            leastUsedNodeID = worker.id();
//                            hostName = worker.hostNames().iterator().next();
//                        }
//                    }

                }
                return true;
            }
        });

        groupMastersMessage.remoteListen(OFFER_ACKNOWLEDGED,  new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {

                System.out.println("The Offer Ack has been sent! by: "+nodeId+ " while my local ID is: "+ignite.cluster().localNode().id());
                System.out.println("The ids are different: "+nodeId.equals(ignite.cluster().localNode().id()));


                if (!nodeId.equals(ignite.cluster().localNode().id())) {


                    System.out.println("6. Received Ack");


                    System.out.println("The received node to be stopped is: " + msg.toString());

                    //if (msg.toString() == myLocalID.toString()) {
                    //Stop your worker node...if required backup data from the stopping node
                    System.out.println("Offer acknowledged Listener");

                    System.out.println("The node about to be stopped is: " + msg);


                    UUID t = donatedNode.id();
                    System.out.println("The remote node is: " + t);


                        System.out.println("7. Sent Final Grant.");

                        //ignite.cluster().stopNodes(Collections.singleton(msg));

//                    try {
//                        Thread.sleep(5000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }

                    System.out.println("You can kill the node");

                        groupMastersMessage.send(OFFER_GRANTED, t);





                    //}
                }
                return true;
            }
        });
    }
}

//    Remove node from clusterGroup
//    Collection<UUID> uuidColl = new ArrayList<>();
//
//            for(ClusterNode elem: coll){
//        System.out.println("The id of old group A are: "+elem.id());
//        uuidColl.add(elem.id());
//    }
//
//            System.out.println("The size of uuidcoll A was :"+uuidColl.size());
//            uuidColl.remove(donatedNode.id());
//            System.out.println("The size of uuidcoll A is :"+uuidColl.size());
//            groupAWorkers.set(ignite.cluster().forNodeIds(uuidColl));