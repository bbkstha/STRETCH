package com.colostate.cs.fa2017;

import com.colostate.cs.fa2017.affinity.StretchAffinityFunction;
import org.apache.ignite.*;
import ch.hsr.geohash.GeoHash;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
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
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.lang.IgniteRunnable;

import java.io.*;
import java.util.*;

public class GeoHashAsKey {

    private static final String cacheName = "MyCache";

    public static void main(String[] args) {

        System.setProperty("-DIGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK", "true");
        //System.setProperty("-DIGNITE_REST_START_ON_CLIENT", "true");

        CacheConfiguration cacheCfg = new CacheConfiguration("MyCache");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setOnheapCacheEnabled(false);
        cacheCfg.setAffinity(new StretchAffinityFunction(64));
        cacheCfg.setRebalanceMode(CacheRebalanceMode.NONE);

        // Enabling the metrics for the cache.
        //cacheCfg.setStatisticsEnabled(true);

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setCacheConfiguration(cacheCfg);
        cfg.setClientMode(true);

        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        // Region name.
        regionCfg.setName("80MB_Region");

        // Enabe metrics for this region.
        //regionCfg.setMetricsEnabled(true);

        // Setting the size of the default memory region to 80MB to achieve this.
        regionCfg.setInitialSize(
                10L * 1024 * 1024);
        regionCfg.setMaxSize( 2000L * 1024 * 1024);




        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);

        storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        //storageCfg.setSystemRegionMaxSize(45L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);


        // Start Ignite node.
        Ignite ignite = Ignition.start(cfg);

        ClusterGroup workerGroup = ignite.cluster().forAttribute("role", "worker");
        System.out.println("The workers are: "+workerGroup.nodes().size());
        ClusterMetrics clusterMetrics = workerGroup.metrics();

        Collection<ClusterNode> nodes =  workerGroup.nodes();
        Iterator<ClusterNode> iterator = nodes.iterator();
        ClusterNode node1 = iterator.next();
        //ClusterNode node2 = iterator.next();
        ClusterMetrics node1Metrics = node1.metrics();
        //ClusterMetrics node2Metrics = node2.metrics();


        try (IgniteCache<Object, String> cache = ignite.getOrCreateCache(cacheName)) {
            // Clear caches before running example.
            cache.clear();
            String strLine;


            // Get cache metrics
            CacheMetrics cacheMetrics = cache.metrics();



            Integer counter = 0;
            Collection<String> keys = new ArrayList<>(12447);
            int duplicate = 0;

            Affinity<Object> affinity = ignite.affinity(cacheName);
            File folder = new File("/s/chopin/b/grad/bbkstha/Desktop/GeospatialSample/naam/");
            File[] listOfFiles = folder.listFiles();
            BufferedReader bufferReader = null;


            for (File file : listOfFiles) {
                InputStream inputStream = new FileInputStream(file.getPath());
                InputStreamReader streamReader = new InputStreamReader(inputStream);
                BufferedReader br = new BufferedReader(streamReader);
                //Read File Line By Line
                while ((strLine = br.readLine()) != null) {

                    if (!strLine.startsWith("LON")) {

                        String lat = strLine.split(",")[0];
                        String lon = strLine.split(",")[1];
                        GeoEntry geoEntry = new GeoEntry(lat, lon, 2);

                        System.out.println("Geohash values: " + geoEntry.getGeoHash());

                        GridCacheDefaultAffinityKeyMapper cacheAffinityKeyMapper = new GridCacheDefaultAffinityKeyMapper();
                        Object affKey = cacheAffinityKeyMapper.affinityKey(geoEntry);
                        System.out.println("The corresponding aff key is: " + affKey);


                        cache.put(geoEntry, "");



                        System.out.println("NonHeap used in cluster: "+clusterMetrics.getNonHeapMemoryUsed());
                        System.out.println("Heap used in cluster: "+clusterMetrics.getHeapMemoryUsed());
                        System.out.println("Avg cpu load in cluster: "+clusterMetrics.getAverageCpuLoad());


                        System.out.println("NonHeap used in node1: "+node1Metrics.getNonHeapMemoryUsed());
                        System.out.println("Heap used in node1: "+node1Metrics.getHeapMemoryUsed());
                        System.out.println("Avg cpu load in node1: "+node1Metrics.getAverageCpuLoad());

                        System.out.println("Off heap allocated: "+cacheMetrics.getOffHeapAllocatedSize());
                        System.out.println("Off heap entries count: "+cacheMetrics.getOffHeapEntriesCount());
                        System.out.println("Off heap allocated: "+cacheMetrics.getRebalancingPartitionsCount());
                        System.out.println("Off heap allocated: "+cacheMetrics.getCacheEvictions());
                        System.out.println("Heap entries count: "+cacheMetrics.getHeapEntriesCount());
                        System.out.println("Off heap eviction: "+cacheMetrics.getOffHeapEvictions());





//                        System.out.println("NonHeap used in node2: "+node2Metrics.getNonHeapMemoryUsed());
//                        System.out.println("Heap used in node2: "+node2Metrics.getHeapMemoryUsed());
//                        System.out.println("Avg cpu load in node2: "+node2Metrics.getAverageCpuLoad());



//                        System.out.println("The corresponding partition ID for key is: " + affinity.partition(geoEntry));
                        System.out.println("The primary node is: " + affinity.mapPartitionToNode(affinity.partition(geoEntry)).id());
//                        System.out.println("The size of collectio is: " + affinity.mapPartitionToPrimaryAndBackups(affinity.partition(geoEntry)).size());
                        counter++;
                    }
                }
            }

            System.out.println("Off heap allocated: "+cacheMetrics.getOffHeapAllocatedSize());
            System.out.println("Off heap entries count: "+cacheMetrics.getOffHeapEntriesCount());
            System.out.println("Off heap allocated: "+cacheMetrics.getRebalancingPartitionsCount());
            System.out.println("Off heap allocated: "+cacheMetrics.getCacheEvictions());
            System.out.println("Heap entries count: "+cacheMetrics.getHeapEntriesCount());
            System.out.println("Off heap eviction: "+cacheMetrics.getOffHeapEvictions());

            System.out.println("Counter: " + counter);
            //cache.destroy();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void visitUsingMapKeysToNodes(int KEY_CNT, Collection<String> keys) {
        final Ignite ignite = Ignition.ignite();


        // Map all keys to nodes.
        Map<ClusterNode, Collection<String>> mappings = ignite.<String>affinity(cacheName).mapKeysToNodes(keys);

        for (Map.Entry<ClusterNode, Collection<String>> mapping : mappings.entrySet()) {
            ClusterNode node = mapping.getKey();

            System.out.println("The cluster node id is: " + node.id());


            final Collection<String> mappedKeys = mapping.getValue();

            if (node != null) {
                // Bring computations to the nodes where the data resides (i.e. collocation).
                ignite.compute(ignite.cluster().forNode(node)).run(() -> {
                    IgniteCache<Integer, String> cache = ignite.cache(cacheName);

                    // Peek is a local memory lookup, however, value should never be 'null'
                    // as we are co-located with node that has a given key.
                    for (String key : mappedKeys)
                        System.out.println("Co-located using mapKeysToNodes [key= " + key);
                });
            }
        }
    }


    private static void visitUsingAffinityRun(final int key) {
        Ignite ignite = Ignition.ignite();

        final IgniteCache<Integer, String> cache = ignite.cache("SpatialDataCache");

//        for (int i = 0; i < KEY_CNT; i++) {
//            final int key = i;

        // This runnable will execute on the remote node where
        // data with the given key is located. Since it will be co-located
        // we can use local 'peek' operation safely.
        ignite.compute().affinityRun("SpatialDataCache", key, new IgniteRunnable() {
            @Override
            public void run() {
                // Peek is a local memory lookup, however, value should never be 'null'
                // as we are co-located with node that has a given key.
                System.out.println("Co-located using affinityRun [key= " + key +
                        ", value=" + cache.localPeek(key) + ']');
            }
        });
    }


    public static class GeoEntry implements Serializable {

        private String geoHash;

        @AffinityKeyMapped
        private String subGeoHash;

        private GeoEntry(String lat, String lon, int upperRange) {

            this.geoHash = GeoHash.withCharacterPrecision(Double.parseDouble(lat), -Double.parseDouble(lon), 12).toBase32();
            this.subGeoHash = this.geoHash.substring(0, upperRange);

        }

        private String getGeoHash() {
            return this.geoHash;
        }

        private String getSubGeoHash() {
            return subGeoHash;
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

