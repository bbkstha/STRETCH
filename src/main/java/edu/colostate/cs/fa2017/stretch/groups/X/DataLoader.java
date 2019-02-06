package edu.colostate.cs.fa2017.stretch.groups.X;

import ch.hsr.geohash.GeoHash;
import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunctionX;
import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunctionXX;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.ClusterLocalNodeMetricsMXBeanImpl;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsSnapshot;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.mxbean.DataRegionMetricsMXBean;

import javax.cache.Cache;
import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

public class DataLoader {

    private static final String cacheName = "STRETCH-CACHE";

    private static final String outputFileName = "/s/chopin/b/grad/bbkstha/stretch/output.txt";

    public static void main(String[] args) throws IOException, InterruptedException, IgniteCheckedException {

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();

        igniteConfiguration.setMetricsUpdateFrequency(2000);
        igniteConfiguration.setMetricsLogFrequency(60000);


        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

 /*       DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        regionCfg.setMetricsEnabled(true);

        // Region name.
        regionCfg.setName("300MB_Region");
        // Setting the size of the default memory region to 80MB to achieve this.
        regionCfg.setInitialSize(
                50L * 1024 * 1024);
        regionCfg.setMaxSize(300L * 1024 * 1024);

        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);*/


        // Setting the size of the default memory region to 4GB to achieve this.
//        storageCfg.getDefaultDataRegionConfiguration().setMaxSize(
//                250L * 1024 * 1024);

        //cfg.setDataStorageConfiguration(storageCfg);
        storageCfg.setSystemRegionInitialSize(15L * 1024 * 1024);
        storageCfg.setSystemRegionMaxSize(45L * 1024 * 1024);
        storageCfg.setMetricsEnabled(true);

        // Setting the data region configuration.
        //storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        // Applying the new configuration.
        igniteConfiguration.setDataStorageConfiguration(storageCfg);

        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setManagementEnabled(true);
        cacheConfiguration.setStatisticsEnabled(true);
        cacheConfiguration.setName(cacheName);

        cacheConfiguration.setOnheapCacheEnabled(false);

        igniteConfiguration.setCacheConfiguration(cacheConfiguration);
        igniteConfiguration.setRebalanceThreadPoolSize(4);

        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);

        StretchAffinityFunctionXX stretchAffinityFunctionXX = new StretchAffinityFunctionXX(false, 1024*32);
        cacheConfiguration.setAffinity(stretchAffinityFunctionXX);
        cacheConfiguration.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheConfiguration.setStatisticsEnabled(true);
        //cacheConfiguration.setDataRegionName("default");




        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group","loader");
            put("role", "master");
            put("donated","no");
            put("region-max", "1000");
            put("split","no");

        }};
        igniteConfiguration.setUserAttributes(userAtt);
        igniteConfiguration.setClientMode(false);

        // Start Ignite node.
        Ignite ignite = Ignition.start(igniteConfiguration);

        //ignite.cluster().resetMetrics();
        IgniteCache<GeoEntry, String> cache = ignite.getOrCreateCache(cacheConfiguration);

        Affinity affinity = ignite.affinity(cacheName);


        cache.clear();


        String path = "/s/chopin/b/grad/bbkstha/stretch/data/";

//            for(int i=0; i<100000000; i++)
//                cache.put(Integer.toString(i),Integer.toString(i));
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        String strLine;
        BufferedReader bufferReader = null;
        int counter = 0;

        GeoEntry tmpGeoEntry = null;

        long sum = 0;
        GeoHashUtils geoHashUtils = new GeoHashUtils();

        File oldFile = new File(outputFileName);
        if(oldFile.isFile()){
            //System.out.println("OLD FILE EXIST");
            oldFile.delete();
        }

        BufferedWriter bw = null;
        bw = new BufferedWriter(new FileWriter(outputFileName));

        for (File file : listOfFiles) {
                InputStream inputStream = new FileInputStream(file.getPath());
                InputStreamReader streamReader = new InputStreamReader(inputStream);
                BufferedReader br = new BufferedReader(streamReader);
                while (( strLine= br.readLine()) != null) {
                    if (!strLine.startsWith("LAT")) {

                        counter++;
                        //System.out.println(counter);
                        double lat = Double.parseDouble(strLine.split(",")[0]);
                        double lon = Double.parseDouble(strLine.split(",")[1]);
                        String timestamp = strLine.split(",")[2];

                        GeoEntry geoEntry = new GeoEntry(lat, lon, 12, timestamp);

                        //System.out.println("The geohash is: "+geoEntry.geoHash);
                        cache.put(geoEntry, strLine);

                        Thread.sleep(2000);



                        if(counter == 150000){
                            //System.out.println("Entered.");
                            tmpGeoEntry = geoEntry;
                        }


                        //byte[] arr = ignite.configuration().getMarshaller().marshal(new GeoEntry(lat, lon, 5, timestamp));
                        //byte[] arr1 = ignite.configuration().getMarshaller().marshal(new String(strLine));

                        //sum += (arr.length + arr1.length);

                        /*if(counter > 0){

                            Thread.sleep(1000);

                        }*/


                        /*if(counter == 184000  || counter==294000) {
                            DataRegionMetrics dataRegionMetrics = ignite.dataRegionMetrics("default");
                            ClusterMetrics metrics = ignite.cluster().localNode().metrics();
                            //Remote node data region metrics using ignite.compute
                            Collection<ClusterNode> remoteNode = ignite.cluster().nodes();
                            Iterator<ClusterNode> it = remoteNode.iterator();
                            while(it.hasNext()) {
                                String remoteDataRegionMetrics = ignite.compute(ignite.cluster().forNode(it.next())).apply(
                                        new IgniteClosure<Integer, String>() {
                                            @Override
                                            public String apply(Integer x) {
                                                System.out.println("Inside hotspot node!");
                                                DataRegionMetrics dM = ignite.dataRegionMetrics("default");
                                                ClusterMetrics metrics = ignite.cluster().localNode().metrics();
                                            *//*try {
                                                Thread.sleep(2000);
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }
*//*                                                String stat = "" + ignite.cluster().localNode().id() + "," + (dM.getPhysicalMemoryPages() * 4 / (double) 1024) + ", " + metrics.getCurrentCpuLoad();
                                                System.out.println(stat);
                                                System.out.println("-----------------------------");
                                                return stat;
                                            }
                                        },
                                        1
                                );
                                System.out.println("Remote total used: " + remoteDataRegionMetrics.split(",")[1]);
                                System.out.println("Remote cpu: " + remoteDataRegionMetrics.split(",")[2]);
                                System.out.println("______________________________________________________________");
                            }
                            //Thread.sleep(2000);
                            //System.out.println("MB getTotalAllocatedPages: " + dataRegionMetrics.getTotalAllocatedPages());
                            //System.out.println("MB getPhysicalMemoryPages: " + dataRegionMetrics.getPhysicalMemoryPages());
                            System.out.println("Local total used: " + dataRegionMetrics.getPhysicalMemoryPages() * 4 / (double) 1024);
                            //System.out.println("MB total allocated: " + 2300.0);
                            //System.out.println("MB total usage %: " + (dataRegionMetrics.getPhysicalMemoryPages() * 4 / ( 1024))/ 2300.0 );
                            System.out.println("Local cpu: " + metrics.getCurrentCpuLoad());
                        }*/





                        /*System.out.println("getOffHeapEntriesCount: "+cacheMetrics.getOffHeapEntriesCount());
                        System.out.println("getOffHeapPrimaryEntriesCount: "+cacheMetrics.getOffHeapPrimaryEntriesCount());
                        System.out.println("getOffHeapAllocatedSize: "+cacheMetrics.getOffHeapAllocatedSize());
                        System.out.println("getCacheSize: "+cacheMetrics.getCacheSize());
                        System.out.println("getHeapEntriesCount: "+cacheMetrics.getHeapEntriesCount());
                        System.out.println("getAveragePutTime: "+cacheMetrics.getAveragePutTime());*/


                        /*System.out.println(
                                counter+" ."+"\n"+
                                "NonHeapMemoryCommitted: "+nodeMetrics.getNonHeapMemoryCommitted()/(1024*1024)+"\n"+
                                         "NonHeapMemoryInitialized: "+nodeMetrics.getNonHeapMemoryInitialized()/(1024*1024)+"\n"+
                                        "NonHeapMemoryUsed: "+nodeMetrics.getNonHeapMemoryUsed()/(1024*1024)+"\n"+
                                        "HeapMemoryCommitted: "+nodeMetrics.getHeapMemoryCommitted()/(1024*1024)+"\n"+
                                        "HeapMemoryInitialized: "+nodeMetrics.getHeapMemoryInitialized()/(1024*1024)+"\n"+
                                        "HeapMemoryMaximum: "+nodeMetrics.getHeapMemoryMaximum()/(1024*1024)+"\n"+
                                        "HeapMemoryUsed: "+nodeMetrics.getHeapMemoryUsed()/(1024*1024)+"\n"+
                                        "HeapMemoryTotal: "+nodeMetrics.getHeapMemoryTotal()/(1024*1024)+"\n"+
                                "-------------------------------------------------------------"+"\n");*/


                    }
                }
            }

        //System.out.println("The value of counter is: "+counter);
        //System.out.println("The sum is: "+sum);
        ClusterMetrics clusterMetrics= ignite.cluster().metrics();
        Object key = tmpGeoEntry.subGeoHash;



        ClusterNode cn = affinity.mapKeyToNode(tmpGeoEntry);
        int[] localParts = ignite.affinity(cacheName).allPartitions(ignite.cluster().forNode(cn).node());


        ignite.compute(ignite.cluster().forNode(cn)).apply(
                new IgniteClosure<Integer, Integer>() {
                    @Override
                    public Integer apply(Integer x) {

                        DataRegionMetrics dM = ignite.dataRegionMetrics("150MB_Region");

                        ClusterMetrics metrics = ignite.cluster().localNode().metrics();

                        for(int j =0 ; j< x; j++) {
                            long keysCountInPartition = 0;
                            for (int i = 0; i < localParts.length; i++) {
                                //System.out.println(cacheName);
                                keysCountInPartition += ignite.cache(cacheName).localSizeLong(localParts[i], CachePeekMode.PRIMARY);
                            }
                            long keysCountInPartitiona = keysCountInPartition;
                            //System.out.println("The size from coutn is: " + (keysCountInPartition * 697.05 / (1024 * 1024)));
                            //System.out.println("MEM PAGES: " + (dM.getPhysicalMemoryPages() * 4 / (double) 1024));
                            //System.out.println("CPU: " + metrics.getCurrentCpuLoad());
                        }

                        return 1;
                    }
                },
                10000000
        );



        /*Collection<ClusterNode> lst = affinity.mapKeyToPrimaryAndBackups(key);
        ignite.compute(ignite.cluster().forNode(lst.iterator().next())).apply(
                new IgniteClosure<Integer, Long>() {
                    @Override
                    public Long apply(Integer x) {

                        IgniteCache localCache = ignite.cache(cacheName);

                        Iterator<Cache.Entry<Object, Object>> itr = localCache.localEntries(CachePeekMode.PRIMARY).iterator();
                        while(itr.hasNext()){

                            Cache.Entry ele = itr.next();
                            //localCache.localLoadCache();
                            localCache.put(ele.getKey(), ele.getValue());
                        }

                    return new Random().nextLong();

                    }
                },
                1
        );*/


        try {
            if(bw != null)
                bw.close();
        } catch (IOException e) {
            System.out.println(e);
        }

        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

        /*CacheMetrics cacheMetrics  = cache.metrics(ignite.cluster());

        Thread.sleep(3000);

        System.out.println("-------------------------------------------");

        System.out.println("getOffHeapEntriesCount: "+cacheMetrics.getOffHeapEntriesCount());
        System.out.println("getOffHeapPrimaryEntriesCount: "+cacheMetrics.getOffHeapPrimaryEntriesCount());
        System.out.println("getOffHeapAllocatedSize: "+cacheMetrics.getOffHeapAllocatedSize());
        System.out.println("getCacheSize: "+cacheMetrics.getCacheSize());
        System.out.println("getHeapEntriesCount: "+cacheMetrics.getHeapEntriesCount());
        System.out.println("getAveragePutTime: "+cacheMetrics.getAveragePutTime());


        System.out.println("-------------------------------------------");*/



    }

    public static class GeoEntry implements Serializable {
        @AffinityKeyMapped
        private String geoHash;

        private String subGeoHash;

        private String timestamp;

        private GeoEntry(){}

        private GeoEntry( double lat, double lon, int upperRange, String timestamp) {

            this.geoHash = GeoHash.withCharacterPrecision(lat,lon, 12).toBase32();
            this.subGeoHash = this.geoHash.substring(0, upperRange);
            this.timestamp = timestamp;
        }

        private String getGeoHash() {
            return this.geoHash;
        }

        private String getSubGeoHash() {
            return subGeoHash;
        }

        private String getTimestamp() {
            return timestamp;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "GeoHash is: " + geoHash + " & subGeoHash is: " + subGeoHash;
        }

    }
}
