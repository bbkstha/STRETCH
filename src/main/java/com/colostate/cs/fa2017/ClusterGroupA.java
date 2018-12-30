package com.colostate.cs.fa2017;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ClusterGroupA {

    private static final String REQUEST_TOPIC = "NEED_RESOURCES";
    private static final String SERVICE_TOPIC = "GOT_RESOURCES";


    public static void main(String[] args) {


        Map<UUID, ClusterNode> nodeMap = new HashMap<>();
        String cacheName = "GeoCache";

        CacheConfiguration cacheCfg = new CacheConfiguration("MyCache");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);


        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group", "A");
            put("role", "master");
        }};

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setCacheConfiguration(cacheCfg);
        cfg.setUserAttributes(userAtt);
        cfg.setClientMode(true);


        //CacheConfiguration<GeoHashAsKey.GeoEntry, String>  cacheCfg1 = new CacheConfiguration(cacheName);

        // Start Ignite node.
        Ignite ignite = Ignition.start(cfg);

        ClusterGroup clusterGroupA = ignite.cluster().forAttribute("group", "A");
        ClusterGroup groupAWorkers = clusterGroupA.forAttribute("role", "worker");
        Collection<ClusterNode> workerNodes = groupAWorkers.nodes();


        ClusterGroup subClusterA = ignite.cluster().forAttribute("group", "A").forAttribute("role", "worker");
        ClusterMetrics metrics = subClusterA.metrics();

        long offHeapUsed = metrics.getNonHeapMemoryUsed();
        long heapUsed = metrics.getHeapMemoryUsed();
        int activeJobs = metrics.getCurrentActiveJobs();
        float idealTime = metrics.getIdleTimePercentage();

        //Condition that triggers the sending a request for new node to be donated.
        //Message sent to all sub-clusters' masters.
        //TOPIC="Resource Requried"
        //Use the avg metrices of the given sub-cluster to trigger the event.

        ClusterGroup clientGroup = ignite.cluster().forRemotes().forClients();

        //Send the request.
        //First set the Listerner in all masters.

        UUID senderID = startListening(ignite, clientGroup);


        final Object[] receivedNode = new Object[1];
        IgniteMessaging groupMastersMessage = ignite.message(clientGroup);

        //Listen to service messgae. Expect receiving node that has been donated.
        groupMastersMessage.remoteListen(SERVICE_TOPIC, (nodeId, msg) -> {
            System.out.println("Received message [msg=" + msg + ", from=" + nodeId + ']');
            receivedNode[0] = msg;
            return true; // Return true to continue listening.
        });

        System.out.println("The received node is: " + receivedNode[0]);

        //Select least used node from your group and donate. Here, we are sending the youngest node in the currect sub-cluster.
        UUID donatedNode = clusterGroupA.forYoungest().node().id();

        if (senderID != clusterGroupA.node().id() && idealTime > 0.5) {

            groupMastersMessage.send(SERVICE_TOPIC, donatedNode);
        }

        //Send request.
        String myStatus = "Heap memory in my cluster is getting low.";
        if (activeJobs > 100) {

            groupMastersMessage.send(REQUEST_TOPIC, myStatus);
        }


        //Add new node to the group.
        groupAWorkers.nodes().add(ignite.cluster().node((UUID) receivedNode[0]));


//
//
//
//        for (Iterator<ClusterNode> it = workerNodes.iterator(); it.hasNext(); ) {
//            ClusterNode c = it.next();
//            nodeMap.put(c.id(), c);
//            Double cpuLoad = c.metrics().getCurrentCpuLoad();
//            System.out.println("The CPU load for node: " + c.id() + " is: " + cpuLoad);
//        }
//
//
//        System.out.println("The number of client nodes are: " + clientGroup.nodes().size());
//
//        IgniteMessaging groupMastersMessage = ignite.message(clientGroup);
//
//        final Object[] receivedNode = new Object[1];
//
//        int MAX_PLAYS = 2;
//
//        final CountDownLatch cnt = new CountDownLatch(MAX_PLAYS);
//
//        groupMastersMessage.localListen("DonatedNode", (nodeId, msg) -> {
//            System.out.println("Received message [msg=" + msg + ", from=" + nodeId + ']');
//            receivedNode[0] = msg;
//            cnt.countDown();
//
//            return true; // Return true to continue listening.
//        });
//
//        System.out.println("The received node is: "+ receivedNode[0]);
//
//
//
//        if(receivedNode[0]!=null) {
//            Boolean added = workerNodes.add(ignite.cluster().node((UUID) receivedNode[0]));
//
//            System.out.println("The new node is added " + added);
//
//            while (added) {
//                for (Iterator<ClusterNode> it = workerNodes.iterator(); it.hasNext(); ) {
//                    ClusterNode c = it.next();
//                    nodeMap.put(c.id(), c);
//                    Double cpuLoad = c.metrics().getCurrentCpuLoad();
//                    System.out.println("The CPU load for node: " + c.id() + " is: " + cpuLoad);
//                }
//                added = false;
//            }
//        }
//
//
//        try {
//            cnt.await();
//        }
//        catch (InterruptedException e) {
//            System.err.println("Hm... let us finish the game!\n" + e);
//        }


//        try (IgniteCache<Object, String> cache = ignite.getOrCreateCache(cacheName)) {
//
//            //
//        }


    }

    private static UUID startListening(Ignite ignite, ClusterGroup grp) {
        // Add ordered message listener.
        return ignite.message(grp).remoteListen(REQUEST_TOPIC, new IgniteBiPredicate<UUID, String>() {
            @Override
            public boolean apply(UUID nodeId, String msg) {
                System.out.println("Received unordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

                return true; // Return true to continue listening.
            }
        });
    }
}
