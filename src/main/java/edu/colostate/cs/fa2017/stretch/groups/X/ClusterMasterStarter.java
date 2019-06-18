package edu.colostate.cs.fa2017.stretch.groups.X;

import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunction;
import edu.colostate.cs.fa2017.stretch.client.DataLoader;
import edu.colostate.cs.fa2017.stretch.util.FileEditor;
import edu.colostate.cs.fa2017.stretch.util.SortMapUsingValue;
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

import javax.cache.Cache;
import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;

import static org.apache.ignite.internal.util.IgniteUtils.*;

public class ClusterMasterStarter {

    private static final String cacheName = "STRETCH-CACHE";

    private static final String configTemplate = "./config/group/X/ClusterWorkerTemplate.xml";
    private static final String configTemplatePartitionSplit = "./config/group/X/ClusterWorkerPowerLawTemplate.xml";

    private static final String RESOURCE_MONITOR = "Resource_Monitor";
    private static final String REQUEST_TOPIC = "Resource_Requested";
    private static final String OFFER_TOPIC = "Resource_Offered";
    private static final String OFFER_ACKNOWLEDGED = "Resource_Acknowledged";
    private static final String OFFER_GRANTED = "Resource_Granted";
    private static double keyValuePairSize = 2080.0;
    private static long countOfKeysToMove = 0;

    private static final String dataRegionName = "50GB_Region";

    private static boolean alreadyRequested = false;
    private static String hotspotPartitions = "";
    private static String localClusterHotspotNodeID = "";
    private static String idleNodeID = "";
    private static Map<UUID, Object> offerReceived = new HashMap<>();
    private static int nodesAllowedPerMachine = 2; //default 2
    private static Map<String, Integer> keyToPartitionMapForCluster = new HashMap<>();
    //private static Logger logger = Logger.getLogger("MyLog");
    public static void main(String[] args) {

        SortMapUsingValue sortMapUsingValue = new SortMapUsingValue();
        sortMapUsingValue.initializeMap();

        if (args.length < 1) return;
        String groupName = "X"; // Defalut Master
        nodesAllowedPerMachine = Integer.parseInt(args[0]);
        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName(cacheName);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);

        StretchAffinityFunction stretchAffinityFunction = new StretchAffinityFunction(false, 20000);
        cacheConfiguration.setAffinity(stretchAffinityFunction);
        cacheConfiguration.setRebalanceMode(CacheRebalanceMode.ASYNC);
        cacheConfiguration.setRebalanceBatchSize(512 * 1024 * 2 * 10);
        cacheConfiguration.setRebalanceBatchesPrefetchCount(4);
        cacheConfiguration.setOnheapCacheEnabled(false);

        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        // Region name.
        regionCfg.setName(dataRegionName);
        // Setting the size of the default memory region to 80MB to achieve this.
        regionCfg.setInitialSize(
                50L * 1024 * 1024);
        regionCfg.setMaxSize(52224L * 1024 * 1024); //52224L
        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);
        storageCfg.setSystemRegionMaxSize(45L * 1024 * 1024);
        // Setting the data region configuration.
        storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        // Applying the new configuration.
        igniteConfiguration.setDataStorageConfiguration(storageCfg);
        igniteConfiguration.setSystemThreadPoolSize(16);
        igniteConfiguration.setRebalanceThreadPoolSize(6);


        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group", groupName);
            put("role", "master");
            put("donated", "no");
            put("split", "no");
            put("region-max", "49152");
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

                        String[] param = msg.toString().split("::");
                        String hotPartitions = hotspotPartitions;
                        String idleNodeTobeUsed = param[1].trim();
                        idleNodeID = idleNodeTobeUsed;
                        String group = ignite.cluster().localNode().attribute("group");
                        String placeHolder = "_GROUP-NAME_" + "##" + "_DONATED_" + "##" + "_HOT-PARTITIONS_" + "##" + "_CAUSE_" + "##" + "_IDLE-NODE_";
                        String replacement = group + "##" + "yes" + "##" + hotPartitions + "##" + "M" + "##" + idleNodeTobeUsed;
                        FileEditor fileEditor = new FileEditor(configTemplate, placeHolder, replacement, group);
                        String configPath = fileEditor.replace();
                        String hostName = ignite.cluster().localNode().hostNames().iterator().next();
                        System.out.println("Idle node to be used : " + idleNodeTobeUsed);

                        Collection<Map<String, Object>> hostNames = new ArrayList<>();
                        Map<String, Object> tmpMap = new HashMap<String, Object>() {{
                            put("host", hostName);
                            put("uname", "bbkstha");
                            put("passwd", "******");
                            put("cfg", configPath);
                            put("nodes", nodesAllowedPerMachine); //set to 2 as master is running in a separete machine alone..so trying to start a faulty node should be possible just by setting it to 2.
                        }};
                        hostNames.add(tmpMap);
                        Map<String, Object> dflts = null;
                        //System.out.println("Starting new node for partition transfer!!!!!");
                        //System.out.println("*************************************************");
                        //System.out.println(LocalDateTime.now());
                        //System.out.println("The partitions: "+hotPartitions+" are being transferred from: "+localClusterHotspotNodeID+" to: "+idleNodeTobeUsed);
                        //System.out.println("*************************************************");
                        long startingTime = System.currentTimeMillis();
                        ignite.cluster().startNodes(hostNames, dflts, false, 10000, nodesAllowedPerMachine);
                        try {
                            sleep(10000);
                        }catch (IgniteInterruptedCheckedException e) {
                            e.printStackTrace();
                        }
                        long startedTime = System.currentTimeMillis();
                        //System.out.println("Time (in sec) taken to transfer partitions: "+(startedTime - startingTime)/1000);
                        //System.out.println("Partition transfer finished.");
                        //System.out.println("------------------------------------------------");
                        alreadyRequested = false;
                    }
                    return true;
                }
            });


            //3. Listen for acknowledgement
            mastersMessanger.remoteListen(OFFER_ACKNOWLEDGED, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {

                    if (msg.toString().split("::")[0].equalsIgnoreCase(ignite.cluster().localNode().id().toString())) {

                        //Send the final permission
                        Object reply = nodeId + "::" + msg.toString().split("::")[1];

                        //System.out.println("***********************************");
                        //System.out.println("REMOTE IDLE NODE TO BE USED: "+msg.toString().split("::")[1]);
                        //System.out.println("***********************************");
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

                        int expectedNumberOfOffers = ignite.cluster().forAttribute("role", "master").nodes().size() - 1;
                        synchronized (offerReceived) {
                            offerReceived.put(nodeId, msg);
                            if (offerReceived.size() < expectedNumberOfOffers) return true;
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
                            String hotPartitions = msg.toString();
                            Object offer = (Object) minMemoryUsed + "::" + minMemoryUsedID + "::" + maxMemoryAllocatedPerNode.get(minMemoryUsedID) + "::" + nodeId;
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
                    String groupName = ignite.cluster().localNode().attribute("group");
                    ClusterGroup clusterGroup = ignite.cluster().forAttribute("group", groupName);
                    Map<Double, UUID> offheapUsage = new TreeMap<>();
                    Map<Double, UUID> cpuUsage = new TreeMap<>();
                    Map<UUID, Boolean> partitionSplitOngoing = new HashMap<>();

                    //Subcluster nodes usage stats collector
                    if(!localClusterHotspotNodeID.isEmpty()){

                        long keysCount = ignite.compute(ignite.cluster().forNodeId(UUID.fromString(idleNodeID))).apply(
                                new IgniteClosure<Integer, Long>() {
                                    @Override
                                    public Long apply(Integer x) {

                                        int[] localParts = ignite.affinity(cacheName).allPartitions(ignite.cluster().localNode());
                                        Map<Integer, Long> partitionToKeyCount = new HashMap<>();
                                        long keysCountInPartition = 0;
                                        long nonEmptyPartition = 0;
                                        for (int i = 0; i < localParts.length; i++) {
                                            Long keysInEachPartID = ignite.cache(cacheName).localSizeLong(localParts[i], CachePeekMode.OFFHEAP);
                                            keysCountInPartition += keysInEachPartID;
                                        }
                                       return keysCountInPartition;
                                    }
                                },
                                1
                        );
                        if(keysCount < countOfKeysToMove){

                            clusterGroup = clusterGroup.forOthers(ignite.cluster().forNodeId(UUID.fromString(localClusterHotspotNodeID)));
                        }
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

                            int[] localParts = ignite.affinity(cacheName).allPartitions(ignite.cluster().forNode(clusterWorker).node());
                            Map<Integer, Long> partIDToKeyCount = ignite.compute(ignite.cluster().forNode(clusterWorker)).apply(
                                    new IgniteClosure<Integer, Map<Integer, Long>>() {
                                        @Override
                                        public Map<Integer, Long> apply(Integer x) {

                                            int[] localParts = ignite.affinity(cacheName).allPartitions(clusterWorker);
                                            DataRegionMetrics dM = ignite.dataRegionMetrics(dataRegionName);
                                            ClusterMetrics metrics = ignite.cluster().localNode().metrics();
                                            Map<Integer, Long> partitionToKeyCount = new HashMap<>();
                                            long keysCountInPartition = 0;
                                            long nonEmptyPartition = 0;
                                            for (int i = 0; i < localParts.length; i++) {

                                                Long keysInEachPartID = ignite.cache(cacheName).localSizeLong(localParts[i], CachePeekMode.PRIMARY);
                                                if (keysInEachPartID != 0) {

                                                    nonEmptyPartition++;
                                                    partitionToKeyCount.put(localParts[i], keysInEachPartID);
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
                                                partitionToKeyCount.remove(gcKey);
                                            }
                                            partitionToKeyCount.put(-1, keysCountInPartition);
                                            partitionToKeyCount.put(-2, (long) (metrics.getCurrentCpuLoad() * 1000000000));
                                            partitionToKeyCount.put(-3, nonEmptyPartition);
                                            return partitionToKeyCount;
                                        }
                                    },
                                    1
                            );

                            //remoteDataRegionMetrics
                            double maxMemoryAllocated = Double.parseDouble(ignite.cluster().forNode(clusterWorker).node().attribute("region-max")); //In MB
                            long totalKeyCount = partIDToKeyCount.get(-1);
                            double totalMemoryUsed = (totalKeyCount * keyValuePairSize / (double) (1024 * 1024));
                            double memoryUsageProp = totalMemoryUsed / maxMemoryAllocated;
                            double rate = memoryUsageProp / (double) (System.currentTimeMillis() - startTimeInMilli);

                            double cpuUsageProp = partIDToKeyCount.get(-2) / 1000000000.0;
                            System.out.println("Stats: " + clusterWorker.id());
                            System.out.println("The CPU usage is: " + cpuUsageProp + ", memeory usage is: " + memoryUsageProp + " and used memory is " + memoryUsageProp * maxMemoryAllocated);
                            long totalNonEmptyPartititons = partIDToKeyCount.get(-3);
                            //Remove the -1 and -2 and -3 key
                            partIDToKeyCount.remove(-1);
                            partIDToKeyCount.remove(-2);
                            partIDToKeyCount.remove(-3);
                            LinkedHashMap<Integer, Long> partIDToKeyCountReverse = new LinkedHashMap<>();
                            partIDToKeyCount.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                                    .forEachOrdered(x -> partIDToKeyCountReverse.put(x.getKey(), x.getValue()));

                            //Power Law Distribution:: Test only after 10% of memory allocated is used.
                            if (memoryUsageProp >= 0.1) {

                                //System.out.println("Total non-empty partitions is: " + totalNonEmptyPartititons);
                                int upper20 = (int) Math.ceil(totalNonEmptyPartititons / 5.0);
                                System.out.println("20% of partitions is: " + upper20);
                                long usage80 = totalKeyCount * 4 / 5;
                                //System.out.println("80% of memory is: " + usage80);
                                String skewedPartitions = "";

                                for (Map.Entry<Integer, Long> d : partIDToKeyCountReverse.entrySet()) {
                                    usage80 -= d.getValue();
                                    skewedPartitions += d.getKey() + ",";
                                    upper20--;
                                    if (upper20 < 1) {

                                        break;
                                    }
                                }

                                //20% of partitions are holding 80% or more keys.
                                //Need to split the partitions
                                if (usage80 <= 1500) {
                                    //System.out.println("Skewed Distribution Detected.");
                                    int startSplit = -1;
                                    int endSplit = -1;
                                    partitionSplitOngoing.put(clusterWorker.id(), true);

                                    //Create TMP-CACHE to avoid multiple splitting on the same node at a time
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
                                    String path = "./KeyToPartitionMap-X.ser";
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
                                                        }
                                                    }
                                                }
                                            }
                                            startSplit = Collections.max(keyToPartitionMap.entrySet(), Map.Entry.comparingByValue()).getValue() + 1;
                                            String[] eachSkewedKeys = skewedKeys.split(",");
                                            for (int index = 0; index < eachSkewedKeys.length; index++) {

                                                int largestPartitionID = Collections.max(keyToPartitionMap.entrySet(), Map.Entry.comparingByValue()).getValue() + 1;
                                                String hotKey = eachSkewedKeys[index];
                                                for (int j = 0; j < base32.length; j++) {

                                                    String tmpHotKey = hotKey + base32[j];
                                                    for (int k = 0; k < base32.length; k++) {
                                                        String innerTmpHotKey = tmpHotKey + base32[k];
                                                        keyToPartitionMap.put(innerTmpHotKey, largestPartitionID + ((32 * j) + k));
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
                                        } catch (FileNotFoundException e) {
                                            e.printStackTrace();
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        } catch (ClassNotFoundException e) {
                                            e.printStackTrace();
                                        }
                                    }
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
                                                        tmpCache.put(e.getKey(), e.getValue());
                                                        localCache.remove(e.getKey());
                                                    }
                                                    String myHostName = ignite.cluster().localNode().hostNames().iterator().next();
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
                                                        put("passwd", "*****");
                                                        put("cfg", configPath1);
                                                        put("nodes", nodesAllowedPerMachine);
                                                    }};
                                                    hostConfig.add(tmpMap);
                                                    Map<String, Object> dflts = null;
                                                    ignite.cluster().startNodes(hostConfig, dflts, false, 10000, nodesAllowedPerMachine);

                                                    //Now wait until partition split is apparent
                                                    boolean flag = true;
                                                    while (flag) {

                                                        try {
                                                            Thread.sleep(5000);
                                                        } catch (InterruptedException e) {

                                                        }

                                                        if (ignite.affinity(cacheName).partition(testKey) != prevPart) {

                                                            flag = false;
                                                        }
                                                    }
                                                    long cc = 0;
                                                    Iterator<Cache.Entry<DataLoader.GeoEntry, String>> itr = tmpCache.localEntries(CachePeekMode.OFFHEAP).iterator();
                                                    while (itr.hasNext()) {
                                                        cc++;
                                                        Cache.Entry<DataLoader.GeoEntry, String> e = itr.next();
                                                        localCache.put(e.getKey(), e.getValue());
                                                        tmpCache.remove(e.getKey());
                                                    }
                                                    for (int index = 0; index < skewedPart.length; index++) {

                                                        ScanQuery scanQuery = new ScanQuery();
                                                        scanQuery.setPartition(Integer.parseInt(skewedPart[index]));

                                                        // Execute the query.
                                                        Iterator<Cache.Entry<DataLoader.GeoEntry, String>> iterator1 = localCache.query(scanQuery).iterator();
                                                        long c1 = 0;
                                                        while (iterator1.hasNext()) {
                                                            Cache.Entry<DataLoader.GeoEntry, String> remainder = iterator1.next();
                                                            localCache.put(remainder.getKey(), remainder.getValue());
                                                            c1++;
                                                        }
                                                        ignite.cache("GC-CACHE").put(Integer.parseInt(skewedPart[index]), c1);
                                                    }
                                                    tmpCache.destroy();
                                                    return 1;
                                                }
                                            },
                                            partitionsArgs
                                    );
                                }
                            }

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
                    if (offheapUsage.isEmpty()) return true;

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
                    } else if (maxMemoryUsed > maxCpuUsed) {

                        cause = "M";
                        checker = partitionSplitOngoing.containsKey(maxMemoryUsedID);
                    } else if (maxCpuUsed > maxMemoryUsed) {

                        cause = "C";
                        checker = partitionSplitOngoing.containsKey(maxCpuUsedID);
                        hotspotNodeID = maxCpuUsedID;
                    }

                    if ((maxMemoryUsed > 0.85 || maxCpuUsed > 0.85)) {

                        //Finding hot partition
                        int[] localParts = ignite.affinity(cacheName).allPartitions(ignite.cluster().forNodeId(hotspotNodeID).node());
                        ClusterGroup hotspotCluster = ignite.cluster().forNodeId(hotspotNodeID);
                        Map<Integer, Long> partitionToCount = ignite.compute(hotspotCluster).apply(
                                new IgniteClosure<Integer, Map<Integer, Long>>() {
                                    @Override
                                    public Map<Integer, Long> apply(Integer x) {

                                        Map<Integer, Long> localCountToPart = new TreeMap<>();
                                        long totalKeys = 0;
                                        for (int i = 0; i < localParts.length; i++) {

                                            if (!ignite.cache("GC-CACHE").containsKey(localParts[i])) {
                                                Long keysCountInPartition = ignite.cache(cacheName).localSizeLong(localParts[i], CachePeekMode.OFFHEAP);
                                                ignite.cluster().localNode().metrics().getCurrentWaitingJobs();
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
                        List<Integer> partitionList = new ArrayList<>();
                        for (Map.Entry<Integer, Long> e : partitionToCount.entrySet()) {

                            partitionList.add(e.getKey());
                        }
                        String partitionToMove = "";
                        int select = 0;
                        double amountToTransfer = 0.0;
                        long threshold = (long) totalKeys * 2/3;
                        long keysToMoveCount = 0;
                        Iterator<Map.Entry<Integer, Long>> itr = reverseSortedMap.entrySet().iterator();
                        //int firstElem = itr.next().getKey();
                        if(threshold > 1000){
                            Map<Integer, Boolean> partitionsToMoveMap = new HashMap<>();
                            int startingPart = itr.next().getKey();
                            partitionsToMoveMap.put(startingPart, true);
                            threshold = totalKeys * 2/3;
                            partitionToMove = "";
                            int i = 1;
                            partitionToMove = partitionToMove + startingPart + ",";
                            threshold -= reverseSortedMap.get(startingPart);
                            keysToMoveCount+=reverseSortedMap.get(startingPart);

                            while(startingPart+i <= largestPart && startingPart -i >=smallestPart) {
                                if(partitionToCount.containsKey(startingPart+i)){

                                    if(!partitionsToMoveMap.containsKey(startingPart+i)){

                                        partitionToMove = partitionToMove + (startingPart+i) + ",";
                                        threshold -= partitionToCount.get(startingPart+i);
                                        keysToMoveCount+=partitionToCount.get(startingPart+i);
                                        partitionsToMoveMap.put(startingPart+i, true);
                                    }
                                }else if(partitionToCount.containsKey(startingPart-i)){

                                    if(!partitionsToMoveMap.containsKey(startingPart-i)){

                                        partitionToMove = partitionToMove + (startingPart-i) + ",";
                                        threshold -= partitionToCount.get(startingPart-i);
                                        keysToMoveCount+=partitionToCount.get(startingPart-i);
                                        partitionsToMoveMap.put(startingPart-i, true);
                                    }
                                }
                                i++;
                                if (threshold < 1000) {
                                    break;
                                }
                            }
                        }
                        countOfKeysToMove = keysToMoveCount;
                        //Try to find idle node within own cluster
                        Map.Entry<Double, UUID> leastUsedLocalWorker = ((TreeMap<Double, UUID>) offheapUsage).firstEntry();
                        if (leastUsedLocalWorker.getKey() < 0.35) {

                            Iterator<Map.Entry<Double, UUID>> iterator = cpuUsage.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<Double, UUID> entry = iterator.next();
                                if (leastUsedLocalWorker.getValue() == entry.getValue()) {

                                    if (entry.getKey() < 0.35) {

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
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                mastersMessanger.send(RESOURCE_MONITOR, "START");
            }
        }
    }
}
