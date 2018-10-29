package com.colostate.cs.fa2017;

import com.sun.org.apache.xpath.internal.operations.Bool;
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

public class ClusterGroupB {

    public static void main(String[] args) {

        Map<UUID, ClusterNode> nodeMap = new HashMap<>();

        String cacheName = "GeoCache";

        CacheConfiguration cacheCfg = new CacheConfiguration("MyCache");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        Map<String, String> userAtt = new HashMap<String, String>(){{put("group","B");
            put("role", "master");}};

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setCacheConfiguration(cacheCfg);
        cfg.setUserAttributes(userAtt);
        cfg.setClientMode(true);


        //CacheConfiguration<GeoHashAsKey.GeoEntry, String>  cacheCfg1 = new CacheConfiguration(cacheName);

        // Start Ignite node.
        Ignite ignite = Ignition.start(cfg);

        ClusterGroup clusterGroupB = ignite.cluster().forAttribute("group", "B");
        ClusterGroup groupAWorkers = clusterGroupB.forAttribute("role", "worker");
        Collection<ClusterNode> workerNodes = groupAWorkers.nodes();
        Collection<ClusterNode> updatedWorkerNodes = new ArrayList<>();

        Boolean flag = true;
        UUID donatedNodeID = null;

        for (Iterator<ClusterNode> it = workerNodes.iterator(); it.hasNext(); ) {
            ClusterNode c = it.next();

            if(flag){
                donatedNodeID = c.id();
            }
            flag = false;
            nodeMap.put(c.id(),c);
            Double cpuLoad = c.metrics().getCurrentCpuLoad();
            System.out.println("The CPU load for node: "+c.id()+" is: "+cpuLoad);
        }

        for (Iterator<ClusterNode> it = workerNodes.iterator(); it.hasNext(); ) {
            ClusterNode c = it.next();
            if(c.id()!=donatedNodeID)
                updatedWorkerNodes.add(c);
        }

        ClusterGroup clientGroup = ignite.cluster().forClients();

        System.out.println("The number of client nodes are: "+clientGroup.nodes().size());
        IgniteMessaging groupMastersMessage = ignite.message(clientGroup);

        groupMastersMessage.send("DonatedNode",donatedNodeID);

        System.out.println("The donated node is "+donatedNodeID);


        for (Iterator<ClusterNode> it = updatedWorkerNodes.iterator(); it.hasNext(); ) {
            ClusterNode c = it.next();
            Double cpuLoad = c.metrics().getCurrentCpuLoad();
            System.out.println("The CPU load for node: "+c.id()+" is: "+cpuLoad);
        }



        //while(true)


//
//        try (IgniteCache<Object, String> cache = ignite.getOrCreateCache(cacheName)) {
//
//            //
//        }

    }

}
