package edu.colostate.cs.fa2017.stretch.groups.X;

import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunction;
import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunctionX;
import edu.colostate.cs.fa2017.stretch.util.FileEditor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.eclipse.jetty.io.ssl.SslClientConnectionFactory;

import java.util.*;

import static org.apache.ignite.internal.util.IgniteUtils.fl;
import static org.apache.ignite.internal.util.IgniteUtils.sleep;

public class ClusterMasterX {

    private static final String cacheName = "STRETCH-CACHE";

    private static final String configTemplate = "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/X/ClusteWorkerTemplate.xml";

    private static final String RESOURCE_MONITOR = "Resource_Monitor";
    private static final String REQUEST_TOPIC = "Resource_Requested";
    private static final String OFFER_TOPIC = "Resource_Offered";
    private static final String OFFER_ACKNOWLEDGED = "Resource_Acknowledged";
    private static final String OFFER_GRANTED = "Resource_Granted";

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

        StretchAffinityFunctionX stretchAffinityFunctionX = new StretchAffinityFunctionX(false, 1024);
        cacheConfiguration.setAffinity(stretchAffinityFunctionX);
        cacheConfiguration.setRebalanceMode(CacheRebalanceMode.ASYNC);

        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        // Region name.
        regionCfg.setName("100MB_Region");
        // Setting the size of the default memory region to 80MB to achieve this.
        regionCfg.setInitialSize(
                100L * 1024 * 1024);
        regionCfg.setMaxSize(100L * 1024 * 1024);
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
        }};
        igniteConfiguration.setCacheConfiguration(cacheConfiguration);
        igniteConfiguration.setUserAttributes(userAtt);
        igniteConfiguration.setClientMode(false);

        // Start Ignite node.
        try (Ignite ignite = Ignition.start(igniteConfiguration)) {

            ClusterGroup masterGroup = ignite.cluster().forAttribute("role", "master");

            IgniteMessaging mastersMessanger = ignite.message(masterGroup);
            Map<UUID, Object> offerReceived = new HashMap<>();


            //All other listeners here!!

            //4.Listen for offer grant
            mastersMessanger.remoteListen(OFFER_GRANTED, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {
                    if (msg.toString().split("::")[0].equalsIgnoreCase(ignite.cluster().localNode().id().toString())) {

                        System.out.println("FInal stage of borrowing.");

                        String[] param = msg.toString().split("::");
                        String hotPartitions = param[1];
                        System.out.println("Hotpartitions: "+hotPartitions);
                        String host = param[2];
                        System.out.println("HOst: "+host);
                        String group = ignite.cluster().localNode().attribute("group");
                        System.out.println("Group:"+group);

                        String placeHolder = "_GROUP-NAME_" + "##" + "_DONATED_" + "##" + "_HOT-PARTITIONS_";
                        String replacement = group + "##" + "yes" +"##"+"43,";
                        FileEditor fileEditor = new FileEditor(configTemplate, placeHolder, replacement, group);
                        String configPath = fileEditor.replace();


                        Collection<Map<String, Object>> hostNames = new ArrayList<>();
                        Map<String, Object> tmpMap = new HashMap<String, Object>() {{
                            put("host", host);
                            put("uname", "bbkstha");
                            put("passwd", "Bibek2753");
                            put("cfg", configPath);
                            put("nodes", 4);
                        }};
                        hostNames.add(tmpMap);
                        Map<String, Object> dflts = null;

                        System.out.println("Starting new node!!!!!");
                        ignite.cluster().startNodes(hostNames, dflts, false, 10000, 5);

                        try {
                            sleep(5000);
                        }catch (IgniteInterruptedCheckedException e) {
                            e.printStackTrace();
                        }


                        alreadyRequested = false;
                    }
                    return true;
                }
            });


            //3. Listen for acknowledgement
            mastersMessanger.remoteListen(OFFER_ACKNOWLEDGED, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {

                    System.out.println("My local id is: "+ignite.cluster().localNode().id());
                    System.out.println("And, my offer has been ack by: "+nodeId);
                    System.out.println("Condition is: "+msg.toString().split("::")[0].equals(ignite.cluster().localNode().id()));
                    System.out.println("First element of msg is: "+msg.toString().split("::")[0]);

                    if (msg.toString().split("::")[0].equalsIgnoreCase(ignite.cluster().localNode().id().toString())) {
                        //Transfer data or persist it in the disk from the donated node
                        //Send the final permission
                        Object reply = nodeId + "::" + msg.toString().split("::")[1] + "::" + msg.toString().split("::")[2];

                        System.out.println("Offer Granted: "+reply);
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
                        System.out.println("OFFER RECEIVED");
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

                        System.out.println("OfferAcknowledged: "+reply);

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

                        System.out.println("RESOURCE REQUESTED BY "+nodeId);

                        String groupName = ignite.cluster().localNode().attribute("group");
                        ClusterGroup workerCluster = ignite.cluster().forAttribute("group", groupName);
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
                            System.out.println("USe this host: "+idleHost);
                            String hotPartitions = msg.toString();
                            Object offer = (Object) usage + "::" + idleHost + "::" + hotPartitions;

                            System.out.println("SENDING RESPONSE: "+offer);
                            mastersMessanger.send(OFFER_TOPIC, offer);
                        }
                    }
                    return true;
                }
            });


            //0. Listen to Resource Monitor
            mastersMessanger.remoteListen(RESOURCE_MONITOR, new IgniteBiPredicate<UUID, Object>() {
                        @Override
                        public boolean apply(UUID nodeId, Object msg) {



                                String groupName = ignite.cluster().localNode().attribute("group");
                                ClusterGroup clusterGroup = ignite.cluster().forAttribute("group", groupName);

                               // System.out.println("---------------------------------------------------------------------------------------------");

                                //System.out.println("The size of the subCluster is: " + clusterGroup.nodes().size());
                                for (ClusterNode c : clusterGroup.nodes()) {
                                    /*System.out.println("The nonHeapMemeory initialized (MB): "+c.metrics().getNonHeapMemoryInitialized()/(1024*1024));
                                    System.out.println("The nonHeapMemeory max (MB): "+c.metrics().getNonHeapMemoryMaximum()/(1024*1024));
                                    System.out.println("The nonHeapMemeory committed (MB): "+c.metrics().getNonHeapMemoryCommitted()/(1024*1024));
                                    System.out.println("The nonHeapMemeory used (MB): "+c.metrics().getNonHeapMemoryUsed()/(1024*1024));
                                    System.out.println("The nonHeapMemeory total (MB): "+c.metrics().getNonHeapMemoryTotal()/(1024*1024));*/
                                }


                                ClusterMetrics clusterMetrics = clusterGroup.metrics();

                                long startTimeMills = clusterMetrics.getStartTime();
                                long committedMemory = clusterMetrics.getNonHeapMemoryCommitted();
                                long usedMemory = clusterMetrics.getNonHeapMemoryUsed();
                                //System.out.println("UsedMemory: " + usedMemory + " and AllocatedMemory: " + committedMemory);
                                long currentTimeMillis = System.currentTimeMillis();

                                double usage = usedMemory * (100 / (double) committedMemory); //usage percentage (Bytes)
                                double usageRate = usedMemory * 1000 / (currentTimeMillis - startTimeMills); //usage per second
                                double standardUsage = 4768371582.03; //50MB per second
                                //System.out.println("For Cluster: " + groupName);
                                //System.out.println("Usage: " + usage + " and usageRage: " + usageRate);

                                Long ig = ignite.cache(cacheName).localSizeLong(43, CachePeekMode.PRIMARY);
                                //System.out.println("The size of partition 43 is: "+ig);

                                if (ig > 400 ) {
                                    while(!alreadyRequested) {

                                        Map<Double, ClusterNode> nodeToUsage = new TreeMap<>();
                                        Collection<ClusterNode> nodes = clusterGroup.nodes();
                                        Iterator<ClusterNode> iterator = nodes.iterator();
                                        while (iterator.hasNext()) {
                                            ClusterNode n = iterator.next();
                                            double localUsage = n.metrics().getNonHeapMemoryUsed() / (double) n.metrics().getNonHeapMemoryCommitted();
                                            boolean flag = true;
                                            while (flag) {
                                                if (!nodeToUsage.containsKey(localUsage)) {
                                                    nodeToUsage.put(localUsage, n);
                                                    flag = false;
                                                } else {
                                                    localUsage += 0.001;
                                                }
                                            }
                                        }

                                        ClusterNode hotspotNode = ((TreeMap<Double, ClusterNode>) nodeToUsage).lastEntry().getValue();
                                        System.out.println("Hotspot node is: " + hotspotNode.id());
                                        System.out.println("Hotspot group is: " + hotspotNode.attribute("group"));
                                        System.out.println("Hotspot hostname is: " + hotspotNode.hostNames().iterator().next());

                                        //Finding hot partition
                                        int[] localParts = ignite.affinity(cacheName).allPartitions(hotspotNode);
                                        System.out.println("The size of partition asssociated with hotspot is: (should be 341) " + localParts.length);
                                        ClusterGroup hotspotCluster = ignite.cluster().forNode(hotspotNode);

                                        Map<Integer, Long> partitionToCount = ignite.compute(hotspotCluster).apply(

                                                new IgniteClosure<Integer, Map<Integer, Long>>() {
                                                    @Override
                                                    public Map<Integer, Long> apply(Integer x) {

                                                        System.out.println("Inside hotspot node!");
                                                        Map<Integer, Long> localCountToPart = new TreeMap<>();
                                                        System.out.println("Again The size of partition asssociated with hotspot is: (should be 341) " + localParts.length);
                                                        for (int i = 0; i < localParts.length; i++) {
                                                            //System.out.println(cacheName);
                                                            Long keysCountInPartition = ignite.cache(cacheName).localSizeLong(localParts[i], CachePeekMode.PRIMARY);
                                                            localCountToPart.put(localParts[i], keysCountInPartition);
                                                        }
                                                        return localCountToPart;
                                                    }
                                                },
                                                1
                                        );
                                        System.out.println("OUT OF THE LOOP");
                                        System.out.println("The size of 43 in the hotspot node is: " + partitionToCount.get(43));
                                        System.out.println("The parts length in the hotspot node is: " + localParts.length);
                                        System.out.println("The hot partition is: " + ((TreeMap<Integer, Long>) partitionToCount).lastEntry().getKey());

                                        //calculate skewness
                                    /*
                                    Implementation required
                                     */

                                        String hotspotPartitions = ((TreeMap<Integer, Long>) partitionToCount).lastEntry().getKey().toString() + ",";
                                    /*for (Map.Entry<Integer, Long> e: partitionToCount.entrySet()) {

                                        hotspotPartitions += Integer.toString(partitionToCount.) + ",";
                                        System.out.println("The partition is: " + part[j] + " and the count is: " + map.get(part[j]));
                                    }*/

                                        System.out.println("Hotspot partitions to move size: " + hotspotPartitions.split(",").length);

                                        mastersMessanger.send(REQUEST_TOPIC, (Object) hotspotPartitions);
                                        alreadyRequested = true;
                                    }
                            }



                                return true;


                        }
                    });

            boolean flag = true;
            while(flag){
                if(ignite.cluster().forAttribute("role","master").nodes().size() == numberOfMastersExpected){
                    //flag = false;


                    //Need to wait for few seconds to let the system come to stable state
                    try {
                        sleep(3000);
                    } catch (IgniteInterruptedCheckedException e) {
                        e.printStackTrace();
                    }

                    mastersMessanger.send(RESOURCE_MONITOR, "START");

                }
            }



        }
    }
}
