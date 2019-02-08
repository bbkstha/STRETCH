package edu.colostate.cs.fa2017.stretch.groups.X;

import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunctionX;
import edu.colostate.cs.fa2017.stretch.util.FileEditor;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheEntry;
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
import scala.reflect.internal.Trees;

import javax.cache.Cache;
import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.util.IgniteUtils.*;

public class ClusterMasterX {

    private static final String cacheName = "STRETCH-CACHE";

    private static final String configTemplate = "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/X/ClusteWorkerTemplate.xml";
    private static final String configTemplatePartitionSplit = "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/X/ClusteWorkerPowerLawTemplate.xml";

    private static final String RESOURCE_MONITOR = "Resource_Monitor";
    private static final String REQUEST_TOPIC = "Resource_Requested";
    private static final String OFFER_TOPIC = "Resource_Offered";
    private static final String OFFER_ACKNOWLEDGED = "Resource_Acknowledged";
    private static final String OFFER_GRANTED = "Resource_Granted";

    private static final String dataRegionName = "150MB_Region";

    private static boolean alreadyRequested = false;
    private static String hotspotPartitions = "";
    private static Map<UUID, Object> offerReceived = new HashMap<>();

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
        regionCfg.setMaxSize(500L * 1024 * 1024);
        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);
        storageCfg.setSystemRegionMaxSize(45L * 1024 * 1024);
        // Setting the data region configuration.
        storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        // Applying the new configuration.
        igniteConfiguration.setDataStorageConfiguration(storageCfg);
        igniteConfiguration.setRebalanceThreadPoolSize(4);




        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group",groupName);
            put("role", "master");
            put("donated","no");
            put("region-max", "500");
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
                        String group = "GG"; //ignite.cluster().localNode().attribute("group");
                        //System.out.println("Group:"+group);

                        String placeHolder = "_GROUP-NAME_" + "##" + "_DONATED_" + "##" + "_HOT-PARTITIONS_" + "##" + "_CAUSE_" +"##"+ "_IDLE-NODE_";
                        //System.out.println(hotPartitions);
                        String replacement = group + "##" + "yes" +"##"+hotPartitions +"##"+ "M" +"##"+ idleNodeTobeUsed;
                        FileEditor fileEditor = new FileEditor(configTemplate, placeHolder, replacement, group);
                        String configPath = fileEditor.replace();


                        Collection<Map<String, Object>> hostNames = new ArrayList<>();
                        Map<String, Object> tmpMap = new HashMap<String, Object>() {{
                            put("host", "cut-bank");
                            put("uname", "bbkstha");
                            put("passwd", "Bibek2753");
                            put("cfg", configPath);
                            put("nodes", 5);
                        }};
                        hostNames.add(tmpMap);
                        Map<String, Object> dflts = null;

                        System.out.println("Starting new node!!!!!");

                        ignite.cluster().startNodes(hostNames, dflts, false, 10000, 5);

                        try {
                            sleep(10000);
                        }catch (IgniteInterruptedCheckedException e) {
                            e.printStackTrace();
                        }
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

                    if (msg.toString().split("::")[3].equals(ignite.cluster().localNode().id().toString())) {
                        System.out.println("OFFER RECEIVED");
                        int expectedNumberOfOffers = ignite.cluster().forAttribute("role", "master").nodes().size() - 1;
                        System.out.println("Expecting "+expectedNumberOfOffers+" offers.");
                        synchronized (offerReceived){
                            offerReceived.put(nodeId, msg);
                            System.out.println("The number of offer is: "+offerReceived.size());
                            if (offerReceived.size() < expectedNumberOfOffers) {
                                System.out.println("Still Listening");
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

                        offerReceived.clear();

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

                                                DataRegionMetrics dM = ignite.dataRegionMetrics(dataRegionName);
                                                ClusterMetrics metrics = ignite.cluster().localNode().metrics();
                                                String stat = "" + (dM.getPhysicalMemoryPages() * 4 / (double) 1024) + ", "
                                                        + metrics.getCurrentCpuLoad();
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
                            Object offer = (Object) minMemoryUsed + "::" + minMemoryUsedID + "::" + maxMemoryAllocatedPerNode.get(minMemoryUsedID) + "::" + nodeId ;

                            System.out.println("SENDING RESPONSE: "+offer);
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

                            if(!alreadyRequested) {
                                //System.out.println("I am monitoring "+ignite.cluster().localNode().attribute("group"));



                                //System.out.println("Entered monitoring part!!");

                                String groupName = ignite.cluster().localNode().attribute("group");
                                ClusterGroup clusterGroup = ignite.cluster().forAttribute("group", groupName);
                                Map<Double, UUID> offheapUsage = new TreeMap<>();
                                Map<Double, UUID> cpuUsage = new TreeMap<>();
                                Map<Double, UUID> growthRate = new TreeMap<>();

                                //Subcluster nodes usage stats collector
                                for (ClusterNode c : clusterGroup.nodes()) {

                                    int[] localParts = ignite.affinity(cacheName).allPartitions(ignite.cluster().forNode(c).node());


                                    Map<Integer, Long> partIDToKeyCount  = ignite.compute(ignite.cluster().forNode(c)).apply(
                                            new IgniteClosure<Integer, Map<Integer, Long>>() {
                                                @Override
                                                public Map<Integer, Long> apply(Integer x) {

                                                    DataRegionMetrics dM = ignite.dataRegionMetrics(dataRegionName);

                                                    ClusterMetrics metrics = ignite.cluster().localNode().metrics();
                                                    Map<Integer, Long> partitionToKeyCount = new HashMap<>();
                                                    long keysCountInPartition = 0;
                                                    for (int i = 0; i < localParts.length; i++) {
                                                        //System.out.println(cacheName);
                                                        partitionToKeyCount.put(i, ignite.cache(cacheName).localSizeLong(localParts[i], CachePeekMode.PRIMARY));
                                                        keysCountInPartition += ignite.cache(cacheName).localSizeLong(localParts[i], CachePeekMode.PRIMARY);
                                                    }
                                                    partitionToKeyCount.put(-1, keysCountInPartition);
                                                    partitionToKeyCount.put(-2, (long) metrics.getCurrentCpuLoad() * 100000);

                                                    return partitionToKeyCount;
                                                    //System.out.println("The size from coutn is: "+(keysCountInPartition * 697.05/(1024*1024)));
                                                    //System.out.println("MEM PAGES: "+(dM.getPhysicalMemoryPages() * 4 / (double) 1024));
/*                                                    String stat = "" + (keysCountInPartition * 697.05 / (double) (1024 * 1024)) + ","
                                                            + metrics.getCurrentCpuLoad() + "," + (dM.getPhysicalMemoryPages() * 4 / (double) 1024);
                                                    return stat;*/
                                                }
                                            },
                                            1
                                    );
                                    //remoteDataRegionMetrics
                                    double maxMemoryAllocated = Double.parseDouble(ignite.cluster().localNode().attribute("region-max")); //In MB
                                    //System.out.println("Region max: "+maxMemoryAllocated);
                                    //System.out.println("Used : "+remoteDataRegionMetrics.split(",")[0]);
                                    long totalKeyCount = partIDToKeyCount.get(-1);
                                    double totalUsedMemory = (totalKeyCount * 697.05/(double) (1024 * 1024));
                                    double usage = totalUsedMemory / maxMemoryAllocated;
                                    double rate = usage / (double) (System.currentTimeMillis() - startTimeInMilli);
                                    double cpu = partIDToKeyCount.get(-2) / (double) 100000.0 ;
                                    System.out.println("The CPU usage is: "+cpu+" and used memory is "+usage * maxMemoryAllocated);


                                    //Remove the -1 and -2 key
                                    partIDToKeyCount.remove(-1);
                                    partIDToKeyCount.remove(-2);
                                    LinkedHashMap<Integer, Long> partIDToKeyCountReverse = new LinkedHashMap<>();
                                    partIDToKeyCount.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                                            .forEachOrdered(x -> partIDToKeyCountReverse.put(x.getKey(), x.getValue()));

                                    //Power Law Distribution
                                    int sizeOfMap = partIDToKeyCountReverse.size();
                                    int upper20 = (int) Math.ceil(sizeOfMap/ 5.0);
                                    long usage80 = totalKeyCount * 4/5;
                                    String skewedPartitons = "";
                                    for (Map.Entry<Integer, Long> d : partIDToKeyCountReverse.entrySet()) {
                                        usage80-=d.getValue();
                                        skewedPartitons += d.getKey()+",";
                                        upper20--;
                                        if(upper20 < 1 || usage80<=0){
                                            break;
                                        }
                                    }


                                    //20% of partitions are holding 80%+ keys.
                                    //Need to split the partitions
                                    if(usage80<=0){

                                        String path = "/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-2.7.0-bin/STRETCH/KeyToPartitionMap";
                                        char[] base32 = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
                                                'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
                                        Map<String, Integer > keyToPartitionMap = new TreeMap<>();

                                        File oldFile = new File(path);
                                        if(!oldFile.isFile()){
                                            System.out.println("OLD FILE DOESN'T EXIST");
                                            for(int i=0; i< base32.length; i++){
                                                for(int j = 0; j< base32.length; j++){
                                                    String tmp = Character.toString(base32[i]);
                                                    tmp+=Character.toString(base32[j]);
                                                    keyToPartitionMap.put(tmp,(32*i)+j);
                                                }
                                            }
                                        }else {



                                            try
                                            {
                                                FileInputStream fis = new FileInputStream(path);
                                                ObjectInputStream ois = new ObjectInputStream(fis);
                                                keyToPartitionMap = (TreeMap) ois.readObject();
                                                ois.close();
                                                fis.close();
                                            }catch(IOException ioe)
                                            {
                                                ioe.printStackTrace();
                                            }catch(ClassNotFoundException cf)
                                            {
                                                System.out.println("Class not found");
                                                cf.printStackTrace();
                                            }
                                        }

                                        String[] skewedPart = skewedPartitons.split(",");
                                        String skewedKeys = "";
                                        for(int index=0; index < skewedPart.length; index++){
                                            for(Iterator iter = keyToPartitionMap.entrySet().iterator(); iter.hasNext();) {
                                                Map.Entry e = (Map.Entry) iter.next();
                                                if (skewedPart[index].equals(e.getValue())) {
                                                    skewedKeys+=e.getKey()+",";
                                                }
                                            }
                                        }
                                        String[] eachSkewedKeys = skewedKeys.split(",");
                                        for(int index=0; index < eachSkewedKeys.length; index++){
                                            int sizeOfKeyToPartitionMap = ((TreeMap<String,Integer>) keyToPartitionMap).lastEntry().getValue() + 1;
                                            for(int j = 0; j< base32.length; j++){
                                                String tmpHotKey = eachSkewedKeys[index];
                                                tmpHotKey+=Character.toString(base32[j]);
                                                keyToPartitionMap.put(tmpHotKey,(sizeOfKeyToPartitionMap+j));
                                            }
                                            keyToPartitionMap.remove(eachSkewedKeys[index]);
                                        }


                                        File oldFile1 = new File(path);
                                        if(oldFile1.isFile()){
                                            oldFile1.delete();
                                        }
                                        try
                                        {
                                            FileOutputStream fos =
                                                    new FileOutputStream(path);
                                            ObjectOutputStream oos = new ObjectOutputStream(fos);
                                            oos.writeObject(keyToPartitionMap);
                                            oos.close();
                                            fos.close();
                                            System.out.printf("Serialized HashMap data is saved in hashmap.ser");
                                        }catch(IOException ioe)
                                        {
                                            ioe.printStackTrace();
                                        }

                                        //Create tmp-cache to move data from hot partition to be split next
                                        CacheConfiguration tmpCacheConfiguration = new CacheConfiguration("TMP-CACHE");
                                        tmpCacheConfiguration.setCacheMode(CacheMode.LOCAL);
                                        IgniteCache<DataLoader.GeoEntry, String> tmpCache = ignite.createCache(tmpCacheConfiguration);
                                        int prevPart = -1;
                                        DataLoader.GeoEntry testKey = new DataLoader.GeoEntry();
                                        //System.out.println("Previous partition ID is: "+prevPart);
                                        IgniteCache<DataLoader.GeoEntry, String> localCache = ignite.cache(cacheName);
                                        Iterator<Cache.Entry<DataLoader.GeoEntry, String>> it = localCache.localEntries(CachePeekMode.OFFHEAP).iterator();
                                        int i=0;
                                        while(it.hasNext()){
                                            i++;
                                            Cache.Entry<DataLoader.GeoEntry, String> e = it.next();
                                            if(prevPart==-1){
                                                int tmpPart = ignite.affinity(cacheName).partition(e.getKey());
                                                for(int index=0; index < skewedPart.length; index++){
                                                    if(tmpPart == Integer.parseInt(skewedPart[index])){
                                                        prevPart = tmpPart;
                                                        testKey = e.getKey();
                                                    }
                                                }
                                           }
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

                                        //Start faulty node to update keyToPartitionMap
                                        String myHostName = ignite.cluster().localNode().hostNames().iterator().next();
                                        String group = ignite.cluster().localNode().attribute("group");
                                        String placeHolder = "_GROUP-NAME_" + "##" + "_DONATED_" + "##" + "_SPLIT-PARTITION_" + "_PATH_";
                                        //System.out.println(hotPartitions);
                                        String replacement = group + "##" + "no" +"##"+"yes" + path;
                                        FileEditor fileEditor = new FileEditor(configTemplatePartitionSplit, placeHolder, replacement, group);
                                        String configPath1 = fileEditor.replace();

                                        Collection<Map<String, Object>> hostConfig = new ArrayList<>();
                                        Map<String, Object> tmpMap = new HashMap<String, Object>() {{
                                            put("host", myHostName);
                                            put("uname", "bbkstha");
                                            put("passwd", "Bibek2753");
                                            put("cfg", configPath1);
                                            put("nodes", 2);
                                        }};
                                        hostConfig.add(tmpMap);
                                        Map<String, Object> dflts = null;
                                        System.out.println("Starting partition split node!!!!!");
                                        ignite.cluster().startNodes(hostConfig, dflts, false, 10000, 2);


                                        //Now wait until partition split is apparent
                                        boolean flag = true;
                                        while(flag){
                                            System.out.println("New partition ID is: "+ignite.affinity(cacheName).partition(testKey));
                                            try {
                                                Thread.sleep(1000);
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
                                        for(int index = 0; index < skewedPart.length; index++) {
                                            ScanQuery scanQuery = new ScanQuery();
                                            scanQuery.setPartition(Integer.parseInt(skewedPart[index]));
                                            // Execute the query.
                                            Iterator<Cache.Entry<DataLoader.GeoEntry, String>> iterator1 = localCache.query(scanQuery).iterator();
                                            int c1 = 0;
                                            while (iterator1.hasNext()) {
                                                Cache.Entry<DataLoader.GeoEntry, String> remainder = iterator1.next();
                                                //System.out.println("The remaining key in 330 is: "+x.getKey());
                                                localCache.put(remainder.getKey(), remainder.getValue());
                                                c1++;
                                            }
                                        }
                                        return true;
                                    }



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


                                double maxMemoryUsed = ((TreeMap<Double, UUID>) offheapUsage).lastEntry().getKey();
                                UUID maxMemoryUsedID = ((TreeMap<Double, UUID>) offheapUsage).lastEntry().getValue();
                                double maxCpuUsed = ((TreeMap<Double, UUID>) cpuUsage).lastEntry().getKey();
                                UUID maxCpuUsedID = ((TreeMap<Double, UUID>) cpuUsage).lastEntry().getValue();


                                if ((maxMemoryUsed > 0.8 || maxCpuUsed > 0.8) && !alreadyRequested) {

                                    System.out.println("I am group: " + groupName + " and I reached a hotspot.And my memory util is: " + maxMemoryUsed*500);
                                    String cause = "";
                                    UUID hotspotNodeID = maxMemoryUsedID;
                                    if (maxMemoryUsedID.equals(maxCpuUsedID)) {
                                        cause = "CM";
                                        //hotspotNodeID = maxMemoryUsedID;
                                    } else if (maxMemoryUsed > maxCpuUsed) {
                                        cause = "M";
                                        //hotspotNodeID = maxMemoryUsedID;
                                    } else if (maxCpuUsed > maxMemoryUsed) {
                                        cause = "C";
                                        hotspotNodeID = maxCpuUsedID;
                                    }

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
                                                    Map<Integer, Long> localCountToPart = new HashMap<>();
                                                    //System.out.println("Again The size of partition asssociated with hotspot is: " + localParts.length);
                                                    //Long counter = Integer.toUnsignedLong(0);
                                                    for (int i = 0; i < localParts.length; i++) {
                                                        //System.out.println(cacheName);
                                                        Long keysCountInPartition = ignite.cache(cacheName).localSizeLong(localParts[i], CachePeekMode.PRIMARY);
                                                        //counter+=keysCountInPartition;
                                                        localCountToPart.put(localParts[i], keysCountInPartition);
                                                    }
                                                    //localCountToPart.put(-1, counter);

                                                    return localCountToPart;
                                                }
                                            },
                                            1
                                    );


                                    //calculate skewness
                                    /*
                                    Implementation required
                                     */

                                    //Alternate partition selection approach
                                    LinkedHashMap<Integer, Long> reverseSortedMap = new LinkedHashMap<>();
                                    partitionToCount.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                                            .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));
                                    //System.out.println(reverseSortedMap);
                                    //System.out.println(reverseSortedMap.size());


                                    String partitionToMove = "";
                                    int select = 0;
                                    double amountToTransfer = 0.0;


                                    long sumed = 0;
                                    for (Map.Entry<Integer, Long> d : reverseSortedMap.entrySet()) {

                                        sumed += d.getValue();

                                    }

                                    System.out.println("Current stat of hotspot: "+sumed * 697.05 / (1024 *1024));

                                    double threshold = sumed * 697.05 * 0.9 / (double) (1024 * 1024);
                                    System.out.println(threshold);
                                    int size = reverseSortedMap.size() / 2;
                                    for (Map.Entry<Integer, Long> e : reverseSortedMap.entrySet()) {

                                        //System.out.println("Partition is: "+e.getKey()+" and count is: "+e.getValue());


                                        partitionToMove = partitionToMove + e.getKey() + ",";
                                        amountToTransfer += e.getValue() * 697.05 / (double) (1024 * 1024);
                                        if (amountToTransfer > threshold) {
                                            System.out.println("Amount to transfer: " + amountToTransfer + " has surpassed threshold: " + threshold);
                                            break;
                                        }
                                    }



                                    System.out.println("The length of partitions to move is: " + partitionToMove.split(",").length);

                                    //System.out.println("The length of partitions to move is: "+partitionToMove);

                                    hotspotPartitions = partitionToMove;
                                    alreadyRequested = true;
                                    mastersMessanger.send(REQUEST_TOPIC, (Object) cause);
                                }
                            }
                            return true;
                        }
                    });



            boolean flag = true;
            while(flag){
                if(ignite.cluster().forAttribute("role","master").nodes().size() == numberOfMastersExpected+1){
                    //Need to wait for few seconds to let the system come to stable state
                    if(!alreadyRequested){

                        Thread.sleep(3000);
                        //System.out.println("NUmber of masters: "+ignite.cluster().forAttribute("role","master").nodes().size());
                       // System.out.println("START");
                        mastersMessanger.send(RESOURCE_MONITOR, "START");
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
