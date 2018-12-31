package com.colostate.cs.fa2017;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterGroupB {

    private static final String REQUEST_TOPIC = "NEED_RESOURCES";
    private static final String SERVICE_TOPIC = "GOT_RESOURCES";


    public static void main(String[] args) {

        Map<UUID, ClusterNode> nodeMap = new HashMap<>();

        String cacheName = "GeoCache";

        CacheConfiguration cacheCfg = new CacheConfiguration("MyCache");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        Map<String, String> userAtt = new HashMap<String, String>(){{
            put("group","B");
            put("role", "master");
        }};

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setCacheConfiguration(cacheCfg);
        cfg.setUserAttributes(userAtt);
        cfg.setClientMode(true);


        //CacheConfiguration<GeoHashAsKey.GeoEntry, String>  cacheCfg1 = new CacheConfiguration(cacheName);

        // Start Ignite node.
        Ignite ignite = Ignition.start(cfg);

        ClusterGroup clusterGroupB = ignite.cluster().forAttribute("group", "B");
        ClusterGroup groupBWorkers = clusterGroupB.forAttribute("role", "worker");
        Collection<ClusterNode> workerNodes = groupBWorkers.nodes();
        Collection<ClusterNode> updatedWorkerNodes = new ArrayList<>();


        AtomicReference<ClusterGroup> subClusterB = new AtomicReference<>(ignite.cluster().forAttribute("group", "B").forAttribute("role", "worker"));
        ClusterMetrics metrics = subClusterB.get().metrics();
        int activeJobs = metrics.getCurrentActiveJobs();
        System.out.println("active jobs: "+activeJobs);

        ClusterGroup clientGroup = ignite.cluster().forAttribute("role","master");
        IgniteMessaging groupMastersMessage = ignite.message(clientGroup);


        final Object[] receivedNode = new Object[1];
        //Listen to service messgae. Expect receiving node that has been donated.
        groupMastersMessage.remoteListen(SERVICE_TOPIC, (nodeId, msg) -> {
            System.out.println("Received message [msg=" + msg + ", from=" + nodeId + ']');
            receivedNode[0] = msg;
            System.out.println("The received node is: " + receivedNode[0]);

            System.out.println("The size of groupB was: "+clusterGroupB.nodes().size());

            if(receivedNode[0].toString().length()!=0){

                ClusterNode node = ignite.cluster().node((UUID)receivedNode[0]);

                System.out.println("Donated Node id is:"+node.id());
                System.out.println("Donated Node consistent id is:"+node.consistentId());

                ClusterGroup tmpGroup = ignite.cluster().forNode(node);
                System.out.println("Donated Node consistent id is:"+tmpGroup.node().consistentId());



                System.out.println("The size of tmpG: "+tmpGroup.nodes().size());


                System.out.println("The size of subB: "+ subClusterB.get().nodes().size());
                Collection<ClusterNode>coll = subClusterB.get().nodes();
                System.out.println("The size of coll: "+coll.size());

                Collection<UUID> uuidColl = new ArrayList<>();

                for(ClusterNode elem: coll){
                    System.out.println("The id of old group are: "+elem.id());
                    uuidColl.add(elem.id());
                }

                System.out.println("The size of uuidcoll was :"+uuidColl.size());
                uuidColl.add(node.id());
                System.out.println("The size of uuidcoll is :"+uuidColl.size());


                subClusterB.set(ignite.cluster().forNodeIds(uuidColl));
                //Add new node to the group.
                System.out.println("Add new node.");
                ClusterGroup subClusterB1 = subClusterB.get().ignite().cluster().forOthers(tmpGroup);
                System.out.println("The size of servers "+subClusterB1.forServers().nodes().size());
                System.out.println("The size of clients"+subClusterB1.forClients().nodes().size());

                for(ClusterNode col: subClusterB.get().nodes()){
                    System.out.println("The id of new group are: "+col.id());

                }



                System.out.println("The size of coll was :"+coll.size());
                ClusterNode newNode = ignite.cluster().node((UUID) receivedNode[0]);
                System.out.println("Donated newNode id is:"+newNode.id());

                System.out.println("Added new node: "+coll.add(newNode));
                System.out.println("The size of coll is :"+coll.size());

                for(ClusterNode col: coll){
                    System.out.println("The id are: "+col.consistentId());

                }

            }


            System.out.println("The updated size of groupA worker is: "+groupBWorkers.nodes().size());

            return true; // Return true to continue listening.
        });


        UUID myID = clusterGroupB.forClients().node().id();
        System.out.println("My node id is: "+myID);

        //Send request.
        String myStatus = "Heap memory in my cluster is getting low.";
        //if (activeJobs < 100) {

            groupMastersMessage.send(REQUEST_TOPIC, myStatus);
            System.out.println("Sent Request!");
       // }

/*
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

*/


        //while(true)



//        try (IgniteCache<Object, String> cache = ignite.getOrCreateCache(cacheName)) {
//
//            //
//        }

    }

}
