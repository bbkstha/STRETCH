package com.colostate.cs.fa2017;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ClusterGroupA {

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

        for (Iterator<ClusterNode> it = workerNodes.iterator(); it.hasNext(); ) {
            ClusterNode c = it.next();
            nodeMap.put(c.id(), c);
            Double cpuLoad = c.metrics().getCurrentCpuLoad();
            System.out.println("The CPU load for node: " + c.id() + " is: " + cpuLoad);
        }

        ClusterGroup clientGroup = ignite.cluster().forClients();

        System.out.println("The number of client nodes are: " + clientGroup.nodes().size());

        IgniteMessaging groupMastersMessage = ignite.message(clientGroup);

        final Object[] receivedNode = new Object[1];

        int MAX_PLAYS = 2;

        final CountDownLatch cnt = new CountDownLatch(MAX_PLAYS);

        groupMastersMessage.localListen("DonatedNode", (nodeId, msg) -> {
            System.out.println("Received message [msg=" + msg + ", from=" + nodeId + ']');
            receivedNode[0] = msg;
            cnt.countDown();

            return true; // Return true to continue listening.
        });

        System.out.println("The received node is: "+ receivedNode[0]);



        if(receivedNode[0]!=null) {
            Boolean added = workerNodes.add(ignite.cluster().node((UUID) receivedNode[0]));

            System.out.println("The new node is added " + added);

            while (added) {
                for (Iterator<ClusterNode> it = workerNodes.iterator(); it.hasNext(); ) {
                    ClusterNode c = it.next();
                    nodeMap.put(c.id(), c);
                    Double cpuLoad = c.metrics().getCurrentCpuLoad();
                    System.out.println("The CPU load for node: " + c.id() + " is: " + cpuLoad);
                }
                added = false;
            }
        }


        try {
            cnt.await();
        }
        catch (InterruptedException e) {
            System.err.println("Hm... let us finish the game!\n" + e);
        }




//        try (IgniteCache<Object, String> cache = ignite.getOrCreateCache(cacheName)) {
//
//            //
//        }


    }
}
