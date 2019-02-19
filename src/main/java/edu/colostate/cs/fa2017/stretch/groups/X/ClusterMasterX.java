package edu.colostate.cs.fa2017.stretch.groups.X;

import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunctionXX;
import edu.colostate.cs.fa2017.stretch.util.FileEditor;
import edu.colostate.cs.fa2017.stretch.util.GeoHashProcessor.GeoHash;
import edu.colostate.cs.fa2017.stretch.util.SortMapUsingValue;
import org.apache.commons.collections.map.HashedMap;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.query.ScanQuery;
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
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import javax.cache.Cache;
import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;

import static org.apache.ignite.internal.util.IgniteUtils.*;

public class ClusterMasterX {

    private static final String cacheName = "STRETCH-CACHE";

    private static final String configTemplate = "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/X/ClusterWorkerTemplate.xml";
    private static final String configTemplatePartitionSplit = "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/X/ClusterWorkerPowerLawTemplate.xml";

    private static final String RESOURCE_MONITOR = "Resource_Monitor";
    private static final String REQUEST_TOPIC = "Resource_Requested";
    private static final String OFFER_TOPIC = "Resource_Offered";
    private static final String OFFER_ACKNOWLEDGED = "Resource_Acknowledged";
    private static final String OFFER_GRANTED = "Resource_Granted";
    private static double keyValuePairSize = 2080.0;

    private static final String dataRegionName = "50GB_Region";

    private static boolean alreadyRequested = false;
    private static String hotspotPartitions = "";
    private static String localClusterHotspotNodeID = "";
    private static Map<UUID, Object> offerReceived = new HashMap<>();
    private static int nodesAllowedPerMachine = 2; //default 2

    private static Map<String, Integer> keyToPartitionMapForCluster = new HashMap<>();

   // private static Logger logger = Logger.getLogger("MyLog");




    public static void main(String[] args) {


        /*Appender fh = null;
        try {
            fh = new FileAppender(new SimpleLayout(), "MyLogFile.log");
            logger.addAppender(fh);
            fh.setLayout(new SimpleLayout());
            logger.info("Entered the main method of Master X.");
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }*/





        SortMapUsingValue sortMapUsingValue = new SortMapUsingValue();
        sortMapUsingValue.initializeMap();


        if (args.length < 2) {
            return;
        }
        String groupName = args[0];
        nodesAllowedPerMachine = Integer.parseInt(args[1]);


        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName(cacheName);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);

        StretchAffinityFunctionXX stretchAffinityFunctionXX = new StretchAffinityFunctionXX(false, 25000);
        cacheConfiguration.setAffinity(stretchAffinityFunctionXX);
        cacheConfiguration.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheConfiguration.setOnheapCacheEnabled(false);

        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        // Region name.
        regionCfg.setName(dataRegionName);
        // Setting the size of the default memory region to 80MB to achieve this.
        regionCfg.setInitialSize(
                50L * 1024 * 1024);
        regionCfg.setMaxSize(52224L * 1024 * 1024);
        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);
        storageCfg.setSystemRegionMaxSize(45L * 1024 * 1024);
        // Setting the data region configuration.
        storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        // Applying the new configuration.
        igniteConfiguration.setDataStorageConfiguration(storageCfg);
        igniteConfiguration.setRebalanceThreadPoolSize(4);


        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group", groupName);
            put("role", "master");
            put("donated", "no");
            put("split", "no");
            put("region-max", "51200");
        }};
        igniteConfiguration.setCacheConfiguration(cacheConfiguration);
        igniteConfiguration.setUserAttributes(userAtt);
        igniteConfiguration.setClientMode(false);

        // Start Ignite node.
        try (Ignite ignite = Ignition.start(igniteConfiguration)) {

            ClusterGroup masterGroup = ignite.cluster().forAttribute("role", "master");

            IgniteMessaging mastersMessanger = ignite.message(masterGroup);


            //All other listeners here!!

            //4.Listen for offer grant
            mastersMessanger.remoteListen(OFFER_GRANTED, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {
                    if (msg.toString().split("::")[0].equalsIgnoreCase(ignite.cluster().localNode().id().toString())) {

                        //System.out.println("FInal stage of borrowing.");

                        String[] param = msg.toString().split("::");

                        //System.out.println("Hot partitions from static variable: "+hotspotPartitions);
                        String hotPartitions = hotspotPartitions;
                        //System.out.println("Hotpartitions: "+hotPartitions);
                        String idleNodeTobeUsed = param[1].trim();
                        //System.out.println("HOst: "+idleNodeTobeUsed);
                        String group = ignite.cluster().localNode().attribute("group");
                        //System.out.println("Group:"+group);

                        String placeHolder = "_GROUP-NAME_" + "##" + "_DONATED_" + "##" + "_HOT-PARTITIONS_" + "##" + "_CAUSE_" + "##" + "_IDLE-NODE_";
                        //System.out.println(hotPartitions);
                        String replacement = group + "##" + "yes" + "##" + hotPartitions + "##" + "M" + "##" + idleNodeTobeUsed;
                        FileEditor fileEditor = new FileEditor(configTemplate, placeHolder, replacement, group);
                        String configPath = fileEditor.replace();
                        String hostName = ignite.cluster().localNode().hostNames().iterator().next();
                        System.out.println("Idle node to be used : " + idleNodeTobeUsed);


                        Collection<Map<String, Object>> hostNames = new ArrayList<>();
                        Map<String, Object> tmpMap = new HashMap<String, Object>() {{
                            put("host", hostName);
                            put("uname", "bbkstha");
                            put("passwd", "Bibek2753");
                            put("cfg", configPath);
                            put("nodes", nodesAllowedPerMachine); //set to 2 as master is running in a separete machine alone..so trying to start a faulty node shoule be possible just by setting it to 2.
                        }};
                        hostNames.add(tmpMap);
                        Map<String, Object> dflts = null;

                        System.out.println("Starting new node for partition transfer!!!!!");
                        System.out.println("*************************************************");
                        System.out.println(LocalDateTime.now());
                        System.out.println("The partitions: "+hotPartitions+" are being transferred from: "+localClusterHotspotNodeID+" to: "+idleNodeTobeUsed);
                        System.out.println("*************************************************");


                        ignite.cluster().startNodes(hostNames, dflts, false, 10000, nodesAllowedPerMachine);

                        Map<String, Boolean> hotPartitionList = new HashMap<>();
                        String[] tmpList = hotPartitions.split(",");
                        for (int j = 0; j < tmpList.length; j++) {
                            hotPartitionList.put(tmpList[j], true);
                        }
                        boolean flag = true;
                        while (flag) {
                            //check memory utilization to go out of the loop.
                            int[] localParts = ignite.affinity(cacheName).allPartitions(ignite.cluster().forNodeId(UUID.fromString(localClusterHotspotNodeID)).node());
                            for (int i = 0; i < localParts.length; i++) {
                                if (hotPartitionList.containsKey(Integer.toString(localParts[i]))){
                                    break;
                                }
                                flag = false;
                            }
                        }

                        /*try {
                            sleep(10000);
                        }catch (IgniteInterruptedCheckedException e) {
                            e.printStackTrace();
                        }*/
                        System.out.println("Partition transfer finished.");
                        System.out.println("------------------------------------------------");
                        alreadyRequested = false;
                    }
                    return true;
                }
            });


            //3. Listen for acknowledgement
            mastersMessanger.remoteListen(OFFER_ACKNOWLEDGED, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {

                    //System.out.println("My local id is: "+ignite.cluster().localNode().id());
                    //System.out.println("And, my offer has been accepted by: "+nodeId);
                    //System.out.println("Condition is: "+msg.toString().split("::")[0].equals(ignite.cluster().localNode().id().toString()));
                    //System.out.println("First element of msg is: "+msg.toString().split("::")[0]);
                    if (msg.toString().split("::")[0].equalsIgnoreCase(ignite.cluster().localNode().id().toString())) {

                        //Send the final permission
                        Object reply = nodeId + "::" + msg.toString().split("::")[1];

                        //System.out.println("Offer Granted: "+reply);
                        System.out.println("***********************************");
                        System.out.println("REMOTE IDLE NODE TO BE USED: "+msg.toString().split("::")[1]);
                        System.out.println("***********************************");
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

                    if (msg.toString().split("::")[3].equals(ignite.cluster().localNode().id().toString())) {
                        //System.out.println("OFFER RECEIVED");
                        int expectedNumberOfOffers = ignite.cluster().forAttribute("role", "master").nodes().size() - 1;
                        System.out.println("Expecting " + expectedNumberOfOffers + " offers.");
                        synchronized (offerReceived) {
                            offerReceived.put(nodeId, msg);
                            //System.out.println("The number of offer is: "+offerReceived.size());
                            if (offerReceived.size() < expectedNumberOfOffers) {
                                //System.out.println("Still Listening");
                                return true;
                            }

                        }


                        //Make selection of hostname to use
                        double usage = 1.0;
                        double maxCapacity = 0.0;
                        String idlenodeID = "";
                        UUID sender = null;
                        for (Map.Entry<UUID, Object> e : offerReceived.entrySet()) {

                            String[] offer = e.getValue().toString().split("::");

                            double tmpUsage = Double.parseDouble(offer[0]);
                            double tmpcapacity = Double.parseDouble(offer[2]);

                            if (tmpUsage <= usage && tmpcapacity >= maxCapacity) {
                                usage = Double.parseDouble(offer[0]);
                                idlenodeID = offer[1];
                                maxCapacity = tmpcapacity;
                                sender = e.getKey();
                            }
                        }
                        Object reply = sender + "::" + idlenodeID;

                        System.out.println("OfferAcknowledged: "+reply);
                        System.out.println("Idle node to be used is: "+idlenodeID);

                        if(usage < 0.3){
                            offerReceived.clear();
                            mastersMessanger.send(OFFER_ACKNOWLEDGED, reply);
                        }
                        else {
                            //Spawn new JVM from workers file
                        }

                    }
                    return true;
                }
            });


            //1. Listen to Resource Request
            mastersMessanger.remoteListen(REQUEST_TOPIC, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {

                    if (!nodeId.equals(ignite.cluster().localNode().id())) {

                        //System.out.println("RESOURCE REQUESTED BY "+nodeId);
                        //System.out.println("Hot partitions using static variable: "+hotspotPartitions);

                        String groupName = ignite.cluster().localNode().attribute("group");
                        ClusterGroup workerCluster = ignite.cluster().forAttribute("group", groupName);

                        if (!workerCluster.nodes().isEmpty()) {

                            Map<Double, UUID> offheapUsage = new TreeMap<>();
                            Map<Double, UUID> cpuUsage = new TreeMap<>();
                            Map<UUID, Double> maxMemoryAllocatedPerNode = new HashMap<>();

                            //Subcluster nodes usage stats collector
                            for (ClusterNode c : workerCluster.nodes()) {

                                String remoteDataRegionMetrics = ignite.compute(ignite.cluster().forNode(c)).apply(
                                        new IgniteClosure<Integer, String>() {
                                            @Override
                                            public String apply(Integer x) {
                                                int[] localParts = ignite.affinity(cacheName).allPartitions(c);
                                                //DataRegionMetrics dM = ignite.dataRegionMetrics(dataRegionName);
                                                ClusterMetrics metrics = ignite.cluster().localNode().metrics();
                                                long sum = 0;
                                                for(int i = 0; i< localParts.length; i++){
                                                    sum+= ignite.cache(cacheName).localSizeLong(localParts[i], CachePeekMode.OFFHEAP);
                                                }

                                                double totalMemoryUsed = sum * keyValuePairSize / (double) (1024 * 1024);

                                                String stat = "" + totalMemoryUsed + ", "
                                                        + metrics.getCurrentCpuLoad();
                                                System.out.println("The worker stat is: "+stat);
                                                return stat;
                                            }
                                        },
                                        1
                                );

                                double maxMemoryAllocated = Double.parseDouble(ignite.cluster().localNode().attribute("region-max")); //In MB
                                maxMemoryAllocatedPerNode.put(c.id(), maxMemoryAllocated);

                                double usage = Double.parseDouble(remoteDataRegionMetrics.split(",")[0]) / maxMemoryAllocated;
                                double cpu = Double.parseDouble(remoteDataRegionMetrics.split(",")[1]);

                                boolean flag1 = true;
                                while (flag1) {
                                    if (!offheapUsage.containsKey(usage)) {
                                        offheapUsage.put(usage, c.id());
                                        flag1 = false;
                                    } else {
                                        usage += (Math.random() % 0.00001);
                                    }
                                }
                                boolean flag2 = true;
                                while (flag2) {
                                    if (!cpuUsage.containsKey(cpu)) {
                                        cpuUsage.put(cpu, c.id());
                                        flag2 = false;
                                    } else {
                                        cpu += (Math.random() % 0.00001);
                                    }
                                }
                            }
                            double minMemoryUsed = ((TreeMap<Double, UUID>) offheapUsage).firstEntry().getKey();
                            UUID minMemoryUsedID = ((TreeMap<Double, UUID>) offheapUsage).firstEntry().getValue();
                            double minCpuUsed = ((TreeMap<Double, UUID>) cpuUsage).firstEntry().getKey();
                            UUID minCpuUsedID = ((TreeMap<Double, UUID>) cpuUsage).firstEntry().getValue();


                            //System.out.println("Donated nodeid and my local is same: "+minMemoryUsedID.equals(ignite.cluster().localNode().id()));

                            String hotPartitions = msg.toString();
                            Object offer = (Object) minMemoryUsed + "::" + minMemoryUsedID + "::" + maxMemoryAllocatedPerNode.get(minMemoryUsedID) + "::" + nodeId;

                            //System.out.println("SENDING RESPONSE: "+offer);
                            mastersMessanger.sendOrdered(OFFER_TOPIC, offer, 0);
                        }
                    }
                    return true;
                }
            });


            //0. Listen to Resource Monitor
            mastersMessanger.remoteListen(RESOURCE_MONITOR, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {


                    long startTimeInMilli = System.currentTimeMillis();
                    //System.out.println("Alreadyrequested Status: "+alreadyRequested);


                    String groupName = ignite.cluster().localNode().attribute("group");
                    ClusterGroup clusterGroup = ignite.cluster().forAttribute("group", groupName);
                    Map<Double, UUID> offheapUsage = new TreeMap<>();
                    Map<Double, UUID> cpuUsage = new TreeMap<>();
                    //Map<Double, UUID> growthRate = new TreeMap<>();

                    Map<UUID, Boolean> partitionSplitOngoing = new HashMap<>();

                    System.out.println("__________________________________________");


                    System.out.println("I am monitoring " + ignite.cluster().localNode().attribute("group") + "Workers Count:" + clusterGroup.nodes().size());
                    //Subcluster nodes usage stats collector

                    if(alreadyRequested){
                        clusterGroup = clusterGroup.forOthers(ignite.cluster().forNodeId(UUID.fromString(localClusterHotspotNodeID)));
                        System.out.println("Partition Transfer going on so excluding hotspot node from the group: "+localClusterHotspotNodeID);
                    }

                    for (ClusterNode clusterWorker : clusterGroup.nodes()) {

                        boolean isSplitOnging = ignite.compute(ignite.cluster().forNode(clusterWorker)).apply(
                                new IgniteClosure<Integer, Boolean>() {
                                    @Override
                                    public Boolean apply(Integer x) {

                                        CacheConfiguration tmpCacheConfiguration1 = new CacheConfiguration("GC-CACHE");
                                        tmpCacheConfiguration1.setCacheMode(CacheMode.LOCAL);
                                        IgniteCache<Integer, Long> gcCache = ignite.getOrCreateCache(tmpCacheConfiguration1);

                                        return ignite.cacheNames().contains("TMP-CACHE");
                                    }
                                }, 1
                        );

                        if (!isSplitOnging) {

                            System.out.println("--------------------------------------------------");

                            int[] localParts = ignite.affinity(cacheName).allPartitions(ignite.cluster().forNode(clusterWorker).node());
                                    /*for(int i=0; i< localParts.length; i++){
                                       if(localParts[i] == 330){
                                          System.out.println("I am node: "+clusterWorker.id()+" and I hold: "+localParts[i]);
                                       }

                                    }*/

                            Map<Integer, Long> partIDToKeyCount = ignite.compute(ignite.cluster().forNode(clusterWorker)).apply(
                                    new IgniteClosure<Integer, Map<Integer, Long>>() {
                                        @Override
                                        public Map<Integer, Long> apply(Integer x) {
                                            //System.out.println("Inside node..");

                                            int[] localParts = ignite.affinity(cacheName).allPartitions(clusterWorker);
                                                    /*for(int i=0; i< localParts.length; i++){
                                                        if(localParts[i] == 330){
                                                            System.out.println("I am in local node: "+clusterWorker.id()+" and I hold: "+localParts[i]);
                                                        }

                                                    }*/
                                            DataRegionMetrics dM = ignite.dataRegionMetrics(dataRegionName);
                                            ClusterMetrics metrics = ignite.cluster().localNode().metrics();
                                            //int currentWaitingJobsInQueue = metrics.getCurrentWaitingJobs();
                                            Map<Integer, Long> partitionToKeyCount = new HashMap<>();
                                            long keysCountInPartition = 0;
                                            long nonEmptyPartition = 0;
                                            for (int i = 0; i < localParts.length; i++) {
                                                //System.out.println(cacheName);
                                                Long keysInEachPartID = ignite.cache(cacheName).localSizeLong(localParts[i], CachePeekMode.OFFHEAP);

                                                        /*if(localParts[i]==415){
                                                            System.out.println("My 415 status: "+keysInEachPartID);
                                                        }
                                                        else if(localParts[i]==330){
                                                            System.out.println("My 330 status: "+keysInEachPartID);
                                                        }*/
                                                //System.out.println("The keysCount in part: "+i+" is: "+keysCountInPartition);
                                                //int totalGetRequest = ignite.cache(cacheName).metrics().getCacheGets();
                                                if (keysInEachPartID != 0) {

                                                    //System.out.println("NOT = 0");
                                                    nonEmptyPartition++;
                                                    //System.out.println(i);
                                                    //System.out.println(localParts[i]);
                                                    partitionToKeyCount.put(localParts[i], keysInEachPartID);
                                                    //System.out.println("The K,V just added is: "+partitionToKeyCount.get(localParts[i]));
                                                    keysCountInPartition += keysInEachPartID;
                                                }
                                            }
                                            //Exclude the gc cache elements from the key total
                                            IgniteCache<Integer, Long> gcCache = ignite.cache("GC-CACHE");
                                            Iterator<Cache.Entry<Integer, Long>> it = gcCache.localEntries(CachePeekMode.ALL).iterator();
                                            int i = 0;
                                            while (it.hasNext()) {
                                                Cache.Entry<Integer, Long> entry = it.next();
                                                //System.out.println("The GC entry is: "+entry.getKey()+" and "+entry.getValue());
                                                keysCountInPartition -= entry.getValue();
                                                int gcKey = entry.getKey();
                                                //System.out.println("Removing gc key..."+gcKey);
                                                partitionToKeyCount.remove(gcKey);
                                            }
                                            partitionToKeyCount.put(-1, keysCountInPartition);
                                            partitionToKeyCount.put(-2, (long) metrics.getCurrentCpuLoad() * 100000);
                                            partitionToKeyCount.put(-3, nonEmptyPartition);
                                            //System.out.println("The size from count is: "+(keysCountInPartition * 697.05/(1024*1024)));
                                                   /* System.out.println("Returning from local node: "+clusterWorker.id());
                                                    for (Map.Entry<Integer, Long> d : partitionToKeyCount.entrySet()) {
                                                        System.out.println("Local Key: "+d.getKey()+"Value: "+d.getValue());
                                                    }*/

                                            return partitionToKeyCount;
                                            //System.out.println("MEM PAGES: "+(dM.getPhysicalMemoryPages() * 4 / (double) 1024));
/*                                                    String stat = "" + (keysCountInPartition * 697.05 / (double) (1024 * 1024)) + ","
                                                        + metrics.getCurrentCpuLoad() + "," + (dM.getPhysicalMemoryPages() * 4 / (double) 1024);
                                                return stat;*/
                                        }
                                    },
                                    1
                            );


                                    /*System.out.println("For node: "+clusterWorker.id());
                                    for (Map.Entry<Integer, Long> d : partIDToKeyCount.entrySet()) {
                                        System.out.println("Key: "+d.getKey()+"Value: "+d.getValue());
                                    }*/

                            //remoteDataRegionMetrics
                            double maxMemoryAllocated = Double.parseDouble(ignite.cluster().forNode(clusterWorker).node().attribute("region-max")); //In MB
                            //System.out.println("Region max: "+maxMemoryAllocated);
                            //System.out.println("Used : "+remoteDataRegionMetrics.split(",")[0]);
                            long totalKeyCount = partIDToKeyCount.get(-1);
                            double totalMemoryUsed = (totalKeyCount * keyValuePairSize / (double) (1024 * 1024));
                            double memoryUsageProp = totalMemoryUsed / maxMemoryAllocated;
                            double rate = memoryUsageProp / (double) (System.currentTimeMillis() - startTimeInMilli);
                            double cpuUsageProp = partIDToKeyCount.get(-2) / (double) 100000.0;
                            System.out.println("--------------------------------------------------");

                            System.out.println("Stats: " + clusterWorker.id());
                            System.out.println("The CPU usage is: " + cpuUsageProp + ", memeory usage is: " + memoryUsageProp + " and used memory is " + memoryUsageProp * maxMemoryAllocated);
                            long totalNonEmptyPartititons = partIDToKeyCount.get(-3);
                            //Remove the -1 and -2 and -3 key
                            partIDToKeyCount.remove(-1);
                            partIDToKeyCount.remove(-2);
                            partIDToKeyCount.remove(-3);
                            //System.out.println("Total non empty partitions: "+totalNonEmptyPartititons+" and map size "+partIDToKeyCount.size());
                            LinkedHashMap<Integer, Long> partIDToKeyCountReverse = new LinkedHashMap<>();
                            partIDToKeyCount.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                                    .forEachOrdered(x -> partIDToKeyCountReverse.put(x.getKey(), x.getValue()));

                                    /*for (Map.Entry<Integer, Long> d : partIDToKeyCountReverse.entrySet()) {
                                        System.out.println("Again Key: "+d.getKey()+"Value: "+d.getValue());
                                    }*/


                                    /*try {
                                        Thread.sleep(5000);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }*/

                            //Power Law Distribution:: Test only after 40% of memory allocated is used.
                            if (memoryUsageProp >= 0.1) {
                                //System.out.println("Power Law Test");


                                //System.out.println("Total non-empty partitions is: " + totalNonEmptyPartititons);
                                int upper20 = (int) Math.ceil(totalNonEmptyPartititons / 5.0);
                                System.out.println("20% of partitions is: " + upper20);
                                //System.out.println("Total Keys: "+totalKeyCount);
                                long usage80 = totalKeyCount * 4 / 5;
                                //System.out.println("80% of memory is: " + usage80);
                                String skewedPartitions = "";

                                for (Map.Entry<Integer, Long> d : partIDToKeyCountReverse.entrySet()) {
                                    usage80 -= d.getValue();
                                    skewedPartitions += d.getKey() + ",";
                                    upper20--;
                                    //System.out.println("The key,value is: "+d.getKey()+", "+d.getValue());
                                    if (upper20 < 1) {

                                        //System.out.println("upper20 is:" + upper20 + " and usage80 is: " + usage80);
                                        break;
                                    }
                                }

                                //20% of partitions are holding 80% or more keys.
                                //Need to split the partitions
                                if (usage80 <= 1500) {
                                    System.out.println("Skewed Distribution Detected.");
                                    System.out.println("Skewed partitions: " + skewedPartitions);
                                            /*for (Map.Entry<Integer, Long> d : partIDToKeyCountReverse.entrySet()) {
                                                System.out.println("Key: "+d.getKey()+"Value: "+d.getValue());
                                            }*/

                                          /*  try {
                                                Thread.sleep(500000);
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }*/

                                    int startSplit = -1;
                                    int endSplit = -1;


                                    partitionSplitOngoing.put(clusterWorker.id(), true);
                                    //Create TMP-CACHE to avoid mulitple splitting on the same node at a time
                                    ignite.compute(ignite.cluster().forNode(clusterWorker)).apply(

                                            new IgniteClosure<Integer, Integer>() {
                                                @Override
                                                public Integer apply(Integer x) {

                                                    //Create tmp-cache to move data from hot partition to be split next
                                                    CacheConfiguration tmpCacheConfiguration = new CacheConfiguration("TMP-CACHE");
                                                    CacheConfiguration tmpCacheConfiguration1 = new CacheConfiguration("GC-CACHE");
                                                    tmpCacheConfiguration.setCacheMode(CacheMode.LOCAL);
                                                    tmpCacheConfiguration1.setCacheMode(CacheMode.LOCAL);
                                                    IgniteCache<DataLoader.GeoEntry, String> tmpCache = ignite.createCache(tmpCacheConfiguration);
                                                    IgniteCache<Integer, Long> gcCache = ignite.getOrCreateCache(tmpCacheConfiguration1);
                                                    return 1;
                                                }
                                            },
                                            1
                                    );

                                    String[] skewedPart = skewedPartitions.split(",");

                                    String skewedKeys = "";
                                    //String path = "/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-2.7.0-bin/STRETCH/KeyToPartitionMap-" + groupName + ".ser";
                                    String path = "/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-2.7.0-bin/STRETCH/KeyToPartitionMap-X.ser";
                                    char[] base32 = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
                                            'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
                                    Map<String, Integer> keyToPartitionMap = new HashMap<>();


                                    File file = new File(path);
                                    if (!file.isFile()) {
                                        System.out.println("FILE DOESN'T EXIST");
                                        for (int i = 0; i < base32.length; i++) {
                                            for (int j = 0; j < base32.length; j++) {
                                                String tmp = Character.toString(base32[i]);
                                                tmp += Character.toString(base32[j]);
                                                keyToPartitionMap.put(tmp, (32 * i) + j);
                                            }
                                        }
                                    } else {
                                        try {
                                            FileChannel channel1 = new RandomAccessFile(file, "rw").getChannel();
                                            FileLock lock = channel1.lock(); //Lock the file. Block until release the lock
                                            System.out.println("FILE LOCKED.");
                                            Map<String, Integer> map = new HashMap<>();
                                            ObjectInputStream ois = new ObjectInputStream(Channels.newInputStream(channel1));
                                            map = (HashMap<String, Integer>) ois.readObject();
                                            lock.release(); //Release the file
                                            System.out.println("UNLOCKED.");
                                            ois.close();
                                            channel1.close();
                                            //Set set = map.entrySet();
                                            //Iterator iterator = set.iterator();
                                                /*while (iterator.hasNext()) {
                                                    Map.Entry mentry = (Map.Entry) iterator.next();
                                                    System.out.print("key: " + mentry.getKey() + " & Value: ");
                                                    System.out.println(mentry.getValue());
                                                }*/
                                            keyToPartitionMap = map;

                                            for (int index = 0; index < skewedPart.length; index++) {

                                                if (!ignite.cache("GC-CACHE").containsKey(Integer.parseInt(skewedPart[index]))) {

                                                    int eachSkewedPartID = Integer.parseInt(skewedPart[index]);
                                                    //System.out.println("Skewed partition idex: " + eachSkewedPartID);
                                                    Iterator<Map.Entry<String, Integer>> iterator = keyToPartitionMap.entrySet().iterator();
                                                    while (iterator.hasNext()) {
                                                        Map.Entry<String, Integer> entry = iterator.next();
                                                        if (eachSkewedPartID == entry.getValue()) {
                                                            skewedKeys += entry.getKey() + ",";
                                                            //System.out.println("Skewed key are: " + skewedKeys);
                                                        }
                                                    }
                                                }
                                            }

                                            startSplit = Collections.max(keyToPartitionMap.entrySet(), Map.Entry.comparingByValue()).getValue() + 1;
                                            String[] eachSkewedKeys = skewedKeys.split(",");
                                            for (int index = 0; index < eachSkewedKeys.length; index++) {
                                                int largestPartitionID = Collections.max(keyToPartitionMap.entrySet(), Map.Entry.comparingByValue()).getValue() + 1;
                                                String hotKey = eachSkewedKeys[index];


                                                //System.out.println("The hotkey is: "+hotKey);

                                                for (int j = 0; j < base32.length; j++) {
                                                    String tmpHotKey = hotKey + base32[j];
                                                    for (int k = 0; k < base32.length; k++) {
                                                        String innerTmpHotKey = tmpHotKey + base32[k];
                                                        keyToPartitionMap.put(innerTmpHotKey, largestPartitionID + ((32 * j) + k));
                                                        //System.out.println("The inner hotKey is: "+innerTmpHotKey+" and partID is: "+keyToPartitionMap.get(innerTmpHotKey));
                                                    }
                                                }
                                                keyToPartitionMap.remove(eachSkewedKeys[index]);
                                                endSplit = Collections.max(keyToPartitionMap.entrySet(), Map.Entry.comparingByValue()).getValue();

                                            }




                                            //Save to modified KeyToPartitionMap
                                            FileChannel channel2 = new RandomAccessFile(file, "rw").getChannel();
                                            FileLock lock2 = channel2.lock(); //Lock the file. Block until release the lock
                                            System.out.println("FILE LOCKED AGAIN.");
                                            ObjectOutputStream oos = new ObjectOutputStream(Channels.newOutputStream(channel2));
                                            oos.writeObject(keyToPartitionMap);
                                            lock2.release(); //Release the file
                                            System.out.println("UNLOCKED.");
                                            oos.close();
                                            channel2.close();

                                            keyToPartitionMapForCluster = keyToPartitionMap;

                                            System.out.println("***********************************");
                                            System.out.println("POWER LAW: HOT PARTITIONS ARE: "+skewedKeys);
                                            System.out.println("POWER LAW: START PART to END PART: "+startSplit+" & "+endSplit);
                                            System.out.println("***********************************");

                                        } catch (FileNotFoundException e) {
                                            e.printStackTrace();
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        } catch (ClassNotFoundException e) {
                                            e.printStackTrace();
                                        }
                                    }

                                            /*try {
                                                Thread.sleep(10000000);
                                            }catch (InterruptedException e){

                                            }*/

                                    String partitionsArgs = startSplit + "," + endSplit;
                                    System.out.println(partitionsArgs);


                                    //Start faulty node to update keyToPartitionMap at the specific worker
                                    ignite.compute(ignite.cluster().forNode(clusterWorker)).apply(

                                            new IgniteClosure<String, Integer>() {
                                                @Override
                                                public Integer apply(String args) {

                                                    String[] bbk = args.split(",");
                                                    int start = Integer.parseInt(bbk[0]);
                                                    int end = Integer.parseInt(bbk[1]);

                                                    System.out.println(start+" >> "+end);


                                                    IgniteCache<DataLoader.GeoEntry, String> tmpCache = ignite.getOrCreateCache("TMP-CACHE");
                                                    int prevPart = -1;
                                                    DataLoader.GeoEntry testKey = new DataLoader.GeoEntry();
                                                    //System.out.println("Previous partition ID is: "+prevPart);
                                                    IgniteCache<DataLoader.GeoEntry, String> localCache = ignite.cache(cacheName);
                                                    Iterator<Cache.Entry<DataLoader.GeoEntry, String>> it = localCache.localEntries(CachePeekMode.OFFHEAP).iterator();
                                                    //int i = 0;
                                                    while (it.hasNext()) {
                                                        //i++;
                                                        Cache.Entry<DataLoader.GeoEntry, String> e = it.next();
                                                        if (prevPart == -1) {
                                                            int tmpPart = ignite.affinity(cacheName).partition(e.getKey());
                                                            for (int index = 0; index < skewedPart.length; index++) {

                                                                if (tmpPart == Integer.parseInt(skewedPart[index])) {
                                                                    prevPart = tmpPart;
                                                                    testKey = e.getKey();
                                                                }
                                                            }
                                                        }
                                                        //System.out.println("FROM ANOTHER: " + i);
                                                        tmpCache.put(e.getKey(), e.getValue());
                                                        localCache.remove(e.getKey());
                                                        //System.out.println("" + i + ". " + e.getKey() + " and value: " + e.getValue());
                                                    }
                                                    String myHostName = ignite.cluster().localNode().hostNames().iterator().next();
                                                    //System.out.println("The hostname is: "+myHostName);
                                                    //String group = ignite.cluster().localNode().attribute("group");
                                                    String group = "X";
                                                    String placeHolder = "_GROUP-NAME_" + "##" + "_DONATED_" + "##" + "_SPLIT-PARTITION_" + "##" + "_PATH_" + "##" + "_START_" + "##" + "_END_" + "##" + "_NODE_";
                                                    //System.out.println(hotPartitions);
                                                    String replacement = group + "##" + "no" + "##" + "yes" + "##" + path + "##" + start + "##" + end + "##" + ignite.cluster().localNode().id();
                                                    FileEditor fileEditor = new FileEditor(configTemplatePartitionSplit, placeHolder, replacement, group);
                                                    String configPath1 = fileEditor.replace();

                                                    Collection<Map<String, Object>> hostConfig = new ArrayList<>();
                                                    Map<String, Object> tmpMap = new HashMap<String, Object>() {{
                                                        put("host", myHostName);
                                                        put("uname", "bbkstha");
                                                        put("passwd", "Bibek2753");
                                                        put("cfg", configPath1);
                                                        put("nodes", nodesAllowedPerMachine);
                                                    }};
                                                    hostConfig.add(tmpMap);
                                                    Map<String, Object> dflts = null;
                                                    System.out.println("Starting partition split node!!!!!");


                                                    ignite.cluster().startNodes(hostConfig, dflts, false, 10000, nodesAllowedPerMachine);

                                                    //Now wait until partition split is apparent


                                                    boolean flag = true;
                                                    while (flag) {

                                                        //System.out.println("OLD PATITION ID: "+prevPart);
                                                        //System.out.println("NEW PATITION ID: "+ignite.affinity(cacheName).partition(testKey));
                                                        try {
                                                            Thread.sleep(2000);
                                                        } catch (InterruptedException e) {

                                                        }

                                                        if (ignite.affinity(cacheName).partition(testKey) != prevPart) {
                                                            //System.out.println("New partition ID is: " + ignite.affinity(cacheName).partition(testKey));
                                                            flag = false;
                                                        }
                                                    }
                                                    long cc = 0;
                                                    Iterator<Cache.Entry<DataLoader.GeoEntry, String>> itr = tmpCache.localEntries(CachePeekMode.OFFHEAP).iterator();
                                                    while (itr.hasNext()) {
                                                        cc++;
                                                        Cache.Entry<DataLoader.GeoEntry, String> e = itr.next();
                                                        //System.out.println("Moving tmpcache data to main cache with changed partID.");
                                                        localCache.put(e.getKey(), e.getValue());
                                                        tmpCache.remove(e.getKey());
                                                    }

                                                    //System.out.println("The total cache elems moved from tmpcache to main cache is: "+cc);

                                                    for (int index = 0; index < skewedPart.length; index++) {
                                                        ScanQuery scanQuery = new ScanQuery();

                                                        //System.out.println("Inside Scan query"+Integer.parseInt(skewedPart[index]));

                                                        scanQuery.setPartition(Integer.parseInt(skewedPart[index]));
                                                        // Execute the query.
                                                        Iterator<Cache.Entry<DataLoader.GeoEntry, String>> iterator1 = localCache.query(scanQuery).iterator();
                                                        long c1 = 0;
                                                        while (iterator1.hasNext()) {
                                                            Cache.Entry<DataLoader.GeoEntry, String> remainder = iterator1.next();
                                                            //System.out.println("The remaining key in 330 being moved: "+remainder.getKey());
                                                            localCache.put(remainder.getKey(), remainder.getValue());
                                                            c1++;
                                                        }

                                                        //System.out.println("garbage in prev partition: "+c1);
                                                        ignite.cache("GC-CACHE").put(Integer.parseInt(skewedPart[index]), c1);
                                                    }

                                                    tmpCache.destroy();
                                                    return 1;
                                                }
                                            },
                                            partitionsArgs
                                    );
                                    //return true;
                                }

                                        /*try{
                                            Thread.sleep(20000000);
                                        }catch (InterruptedException e){

                                        }*/


                            }

                            //System.out.println("Out of Power Law Test");


                            boolean flag1 = true;
                            while (flag1) {
                                if (!offheapUsage.containsKey(memoryUsageProp)) {
                                    offheapUsage.put(memoryUsageProp, clusterWorker.id());
                                    flag1 = false;
                                } else {
                                    memoryUsageProp += (Math.random() % 0.00001);
                                }
                            }
                            boolean flag2 = true;
                            while (flag2) {
                                if (!cpuUsage.containsKey(cpuUsageProp)) {
                                    cpuUsage.put(cpuUsageProp, clusterWorker.id());
                                    flag2 = false;
                                } else {
                                    cpuUsageProp += (Math.random() % 0.00001);
                                }
                            }
                        }
                    }

                    if (offheapUsage.isEmpty()) {
                        System.out.println("No workers or Partition Split ongoing...");
                        return true;
                    }

                    double maxMemoryUsed = ((TreeMap<Double, UUID>) offheapUsage).lastEntry().getKey();
                    UUID maxMemoryUsedID = ((TreeMap<Double, UUID>) offheapUsage).lastEntry().getValue();
                    double maxCpuUsed = ((TreeMap<Double, UUID>) cpuUsage).lastEntry().getKey();
                    UUID maxCpuUsedID = ((TreeMap<Double, UUID>) cpuUsage).lastEntry().getValue();
                    String cause = "";
                    UUID hotspotNodeID = maxMemoryUsedID;
                    boolean checker = false;

                    if (maxMemoryUsedID.equals(maxCpuUsedID)) {
                        cause = "CM";
                        checker = partitionSplitOngoing.containsKey(maxMemoryUsedID);
                        //hotspotNodeID = maxMemoryUsedID;
                    } else if (maxMemoryUsed > maxCpuUsed) {
                        cause = "M";
                        checker = partitionSplitOngoing.containsKey(maxMemoryUsedID);
                        //hotspotNodeID = maxMemoryUsedID;
                    } else if (maxCpuUsed > maxMemoryUsed) {
                        cause = "C";
                        checker = partitionSplitOngoing.containsKey(maxCpuUsedID);
                        hotspotNodeID = maxCpuUsedID;
                    }

                    if ((maxMemoryUsed > 0.9 || maxCpuUsed > 0.9)) {

                        System.out.println("I am group: " + groupName + " and I reached a hotspot");
                        //System.out.println("Cause of hotspot: "+cause);
                        //Finding hot partition
                        int[] localParts = ignite.affinity(cacheName).allPartitions(ignite.cluster().forNodeId(hotspotNodeID).node());
                        //System.out.println("The size of partition asssociated with hotspot node is: " + localParts.length);
                        ClusterGroup hotspotCluster = ignite.cluster().forNodeId(hotspotNodeID);
                        Map<Integer, Long> partitionToCount = ignite.compute(hotspotCluster).apply(
                                new IgniteClosure<Integer, Map<Integer, Long>>() {
                                    @Override
                                    public Map<Integer, Long> apply(Integer x) {

                                        //System.out.println("Inside hotspot node!");
                                        Map<Integer, Long> localCountToPart = new TreeMap<>();
                                        //System.out.println("Again The size of partition asssociated with hotspot is: " + localParts.length);
                                        long totalKeys = 0;
                                        for (int i = 0; i < localParts.length; i++) {

                                            if (!ignite.cache("GC-CACHE").containsKey(localParts[i])) {
                                                Long keysCountInPartition = ignite.cache(cacheName).localSizeLong(localParts[i], CachePeekMode.OFFHEAP);
                                                if (keysCountInPartition != 0) {
                                                    localCountToPart.put(localParts[i], keysCountInPartition);
                                                    totalKeys += keysCountInPartition;
                                                }
                                            }
                                        }
                                        localCountToPart.put(-1, totalKeys);
                                        return localCountToPart;
                                    }
                                },
                                1
                        );

                        long totalKeys = partitionToCount.get(-1);
                        partitionToCount.remove(-1);
                        //calculate skewness
                                /*
                                Implementation required
                                 */

                        int smallestPart = ((TreeMap<Integer, Long>) partitionToCount).firstEntry().getKey();
                        int largestPart = ((TreeMap<Integer, Long>) partitionToCount).lastEntry().getKey();

                        //Alternate partition selection approach
                        LinkedHashMap<Integer, Long> reverseSortedMap = new LinkedHashMap<>();
                        partitionToCount.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));
                        //System.out.println(reverseSortedMap);
                        //System.out.println(reverseSortedMap.size());

                        //group partitions based on their neighborhood information
                        List<Integer> partitionList = new ArrayList<>();
                        for (Map.Entry<Integer, Long> e : partitionToCount.entrySet()) {
                            partitionList.add(e.getKey());
                        }




                        String partitionToMove = "";
                        int select = 0;
                        double amountToTransfer = 0.0;

                        //System.out.println("Current stat of hotspot: "+totalKeys * 697.05 / (1024 *1024));

                        long threshold = (long) totalKeys * 5/10;
                        //System.out.println(threshold);
                        //int size = reverseSortedMap.size() / 2;
                        /*int i = reverseSortedMap.size();
                        for (Map.Entry<Integer, Long> e : reverseSortedMap.entrySet()) {
                            //System.out.println("Partition is: "+e.getKey()+" and count is: "+e.getValue());
                            if(i%2 == 0){

                                System.out.println("i: "+i);
                                partitionToMove = partitionToMove + e.getKey() + ",";
                                System.out.println("partition to move: "+partitionToMove);
                                System.out.println(e.getValue());
                                threshold -= e.getValue();
                                System.out.println(""+threshold);
                                if (threshold < 100) {
                                    System.out.println("break");
                                    //System.out.println("Amount to transfer: " + amountToTransfer + " has surpassed threshold: " + threshold);
                                    break;
                                }
                            }
                            i--;
                        }*/

                        Iterator<Map.Entry<Integer, Long>> itr = reverseSortedMap.entrySet().iterator();
                        int firstElem = itr.next().getKey();
                        if(threshold > 1000){
                            int startingPart = itr.next().getKey();
                            threshold = (long) totalKeys * 5/10;
                            partitionToMove = "";
                            int i = 1;
                            partitionToMove = partitionToMove + startingPart + ",";
                            threshold -= reverseSortedMap.get(startingPart);
                            System.out.println(""+threshold);
                            while(startingPart+i <= largestPart || startingPart -i >=smallestPart) {
                                if(partitionToCount.containsKey(startingPart+i)){
                                    partitionToMove = partitionToMove + (startingPart+i) + ",";
                                    threshold -= partitionToCount.get(startingPart+i);
                                }else if(partitionToCount.containsKey(startingPart-i)){
                                    partitionToMove = partitionToMove + (startingPart-i) + ",";
                                    threshold -= partitionToCount.get(startingPart-i);
                                }
                                i++;
                                if (threshold < 1000 && threshold > -15000) {
                                    System.out.println("break");
                                    //System.out.println("Amount to transfer: " + amountToTransfer + " has surpassed threshold: " + threshold);
                                    break;
                                }
                            }
                        }

                        System.out.println("selective partititon to move: "+partitionToMove);






                        //System.out.println("The length of partitions to move is: "+partitionToMove);

                        //Try to find idle node within own cluster

                            Map.Entry<Double, UUID> leastUsedLocalWorker = ((TreeMap<Double, UUID>) offheapUsage).firstEntry();
                            if (leastUsedLocalWorker.getKey() < 0.3) {

                                Iterator<Map.Entry<Double, UUID>> iterator = cpuUsage.entrySet().iterator();
                                while (iterator.hasNext()) {
                                    Map.Entry<Double, UUID> entry = iterator.next();
                                    if (leastUsedLocalWorker.getValue() == entry.getValue()) {
                                        if (entry.getKey() < 0.3) {

//                                            String partitionToMove = "";
//                                            int select = 0;
//                                            double amountToTransfer = 0.0;
//
//                                            //System.out.println("Current stat of hotspot: "+totalKeys * 697.05 / (1024 *1024));
//                                            double maxMemoryAllocated = Double.parseDouble(ignite.cluster().forNodeId(leastUsedLocalWorker.getValue()).node().attribute("region-max")); //In MB
//                                            long idleNodeKeysEstimate = Double.valueOf(leastUsedLocalWorker.getKey() * maxMemoryAllocated *(1024 * 1024) / 697.05).longValue();
//                                            long maxKeysIdleNodeCouldTake = Double.valueOf(0.6 * maxMemoryAllocated *(1024 * 1024) / 697.05).longValue();
//
//                                            long threshold = maxKeysIdleNodeCouldTake - idleNodeKeysEstimate;
//
//                                            /*System.out.println("idlenodeestimate: "+idleNodeKeysEstimate);
//                                            System.out.println("maxestimate: "+maxKeysIdleNodeCouldTake);
//                                            System.out.println("threshold"+threshold);
//                                            if(maxKeysIdleNodeCouldTake - threshold < 1000){
//                                                System.out.println("Idle node is empty.");
//                                                threshold = maxKeysIdleNodeCouldTake * 4/5;
//                                            }
//                                            System.out.println("threshold"+threshold);*/
//
//                                            int i = reverseSortedMap.size();
//                                            System.out.println(i);
//                                            for (Map.Entry<Integer, Long> e : reverseSortedMap.entrySet()) {
//                                                //System.out.println("Partition is: "+e.getKey()+" and count is: "+e.getValue());
//                                                if(i%2 == 0){
//
//                                                    System.out.println("i: "+i);
//                                                    partitionToMove = partitionToMove + e.getKey() + ",";
//                                                    System.out.println("partition to move: "+partitionToMove);
//                                                    System.out.println(e.getValue());
//                                                    threshold -= e.getValue();
//                                                    System.out.println(""+threshold);
//                                                    if (threshold < 100) {
//                                                        System.out.println("break");
//                                                        //System.out.println("Amount to transfer: " + amountToTransfer + " has surpassed threshold: " + threshold);
//                                                        break;
//                                                    }
//                                                }
//                                                i--;
//                                            }

                                            //System.out.println("The Partitions to move is: " + partitionToMove);
                                            hotspotPartitions = partitionToMove;
                                            String localDonation = ignite.cluster().localNode().id() + "::" + leastUsedLocalWorker.getValue();
                                            System.out.println("Local idle worker used.");
                                            alreadyRequested = true;
                                            localClusterHotspotNodeID = hotspotNodeID.toString();

                                            System.out.println("***********************************");
                                            System.out.println("LOCAL IDLE TO BE USED: "+leastUsedLocalWorker.getValue());
                                            System.out.println("***********************************");
                                            mastersMessanger.send(OFFER_GRANTED, localDonation);
                                            return true;
                                        }
                                    }
                                }
                            }
                         /*else if (cause.equalsIgnoreCase("C")) {
                            Map.Entry<Double, UUID> leastCPUUsedLocalWorker = ((TreeMap<Double, UUID>) cpuUsage).firstEntry();
                            if (leastCPUUsedLocalWorker.getKey() < 0.4) {

                                Iterator<Map.Entry<Double, UUID>> iterator = offheapUsage.entrySet().iterator();
                                while (iterator.hasNext()) {
                                    Map.Entry<Double, UUID> entry = iterator.next();
                                    if (leastCPUUsedLocalWorker.getValue() == entry.getValue()) {
                                        if (entry.getKey() < 0.4) {


                                            String partitionToMove = "";
                                            int select = 0;
                                            double amountToTransfer = 0.0;

                                            //System.out.println("Current stat of hotspot: "+totalKeys * 697.05 / (1024 *1024));
                                            double maxMemoryAllocated = Double.parseDouble(ignite.cluster().forNodeId(leastUsedLocalWorker.getValue()).node().attribute("region-max")); //In MB
                                            long idleNodeKeysEstimate = Double.valueOf(leastUsedLocalWorker.getKey() * maxMemoryAllocated *(1024 * 1024) / 697.05).longValue();
                                            long maxKeysIdleNodeCouldTake = Double.valueOf(0.9 * maxMemoryAllocated *(1024 * 1024) / 697.05).longValue();

                                            long threshold = maxKeysIdleNodeCouldTake - idleNodeKeysEstimate;
                                            //System.out.println(threshold);
                                            int size = reverseSortedMap.size() / 2;
                                            for (Map.Entry<Integer, Long> e : reverseSortedMap.entrySet()) {
                                                //System.out.println("Partition is: "+e.getKey()+" and count is: "+e.getValue());
                                                partitionToMove = partitionToMove + e.getKey() + ",";
                                                threshold -= e.getValue();
                                                size--;
                                                if (threshold < 100 || size < 1) {
                                                    //System.out.println("Amount to transfer: " + amountToTransfer + " has surpassed threshold: " + threshold);
                                                    break;
                                                }
                                            }

                                            System.out.println("The Partitions to move is: " + partitionToMove);






                                            hotspotPartitions = partitionToMove;
                                            String localDonation = ignite.cluster().localNode().id() + "::" + leastCPUUsedLocalWorker.getValue();
                                            alreadyRequested = true;
                                            localClusterHotspotNodeID = hotspotNodeID.toString();
                                            mastersMessanger.send(OFFER_GRANTED, localDonation);
                                            return true;
                                        }
                                    }
                                }
                            }
                        }*/ /*else if (cause.equalsIgnoreCase("CM") || cause.equalsIgnoreCase("C")) {
                            Map.Entry<Double, UUID> leastUsedLocalWorker = ((TreeMap<Double, UUID>) offheapUsage).firstEntry();
                            if (leastUsedLocalWorker.getKey() < 0.4) {

                                Iterator<Map.Entry<Double, UUID>> iterator = cpuUsage.entrySet().iterator();
                                while (iterator.hasNext()) {
                                    Map.Entry<Double, UUID> entry = iterator.next();
                                    if (leastUsedLocalWorker.getValue() == entry.getValue()) {
                                        if (entry.getKey() < 0.4) {


                                            String partitionToMove = "";
                                            int select = 0;
                                            double amountToTransfer = 0.0;

                                            //System.out.println("Current stat of hotspot: "+totalKeys * 697.05 / (1024 *1024));
                                            double maxMemoryAllocated = Double.parseDouble(ignite.cluster().forNodeId(leastUsedLocalWorker.getValue()).node().attribute("region-max")); //In MB
                                            long idleNodeKeysEstimate = Double.valueOf(leastUsedLocalWorker.getKey() * maxMemoryAllocated *(1024 * 1024) / 697.05).longValue();
                                            long maxKeysIdleNodeCouldTake = Double.valueOf(0.9 * maxMemoryAllocated *(1024 * 1024) / 697.05).longValue();

                                            long threshold = maxKeysIdleNodeCouldTake - idleNodeKeysEstimate;
                                            //System.out.println(threshold);
                                            int size = reverseSortedMap.size() / 2;
                                            for (Map.Entry<Integer, Long> e : reverseSortedMap.entrySet()) {
                                                //System.out.println("Partition is: "+e.getKey()+" and count is: "+e.getValue());
                                                partitionToMove = partitionToMove + e.getKey() + ",";
                                                threshold -= e.getValue();
                                                size--;
                                                if (threshold < 100 || size < 1) {
                                                    //System.out.println("Amount to transfer: " + amountToTransfer + " has surpassed threshold: " + threshold);
                                                    break;
                                                }
                                            }

                                            System.out.println("The Partitions to move is: " + partitionToMove);
                                            hotspotPartitions = partitionToMove;
                                            String localDonation = ignite.cluster().localNode().id() + "::" + leastUsedLocalWorker.getValue();
                                            alreadyRequested = true;
                                            localClusterHotspotNodeID = hotspotNodeID.toString();
                                            mastersMessanger.send(OFFER_GRANTED, localDonation);
                                            return true;
                                        }
                                    }
                                }
                            }
                        }*/



                        System.out.println("Could not find local idle node.");



                        System.out.println("The Partitions to be moved on remote idle node is: " + partitionToMove);
                        hotspotPartitions = partitionToMove;
                        alreadyRequested = true;
                        localClusterHotspotNodeID = hotspotNodeID.toString();
                        mastersMessanger.send(REQUEST_TOPIC, (Object) cause);
                    }
                    return true;
                }
            });


            boolean flag = true;
            while (flag) {
                //if (ignite.cluster().forAttribute("role", "master").nodes().size() == numberOfMastersExpected) {
                    //Need to wait for few seconds to let the system come to stable state
//                    if(!alreadyRequested){
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //System.out.println("NUmber of masters: "+ignite.cluster().forAttribute("role","master").nodes().size());
                    //System.out.println("START");
                    mastersMessanger.send(RESOURCE_MONITOR, "START");
                    //}
                    //}
                //}
            }
        }
    }
}
