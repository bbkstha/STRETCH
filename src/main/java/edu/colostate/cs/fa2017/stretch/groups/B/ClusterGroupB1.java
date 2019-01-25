package edu.colostate.cs.fa2017.stretch.groups.B;

import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ClusterGroupB1 {

    public static void main(String[] args) {

        String cacheName = "GeoCache";

        CacheConfiguration cacheCfg = new CacheConfiguration("MyCache");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setOnheapCacheEnabled(false);


        StretchAffinityFunction stretchAffinityFunction = new StretchAffinityFunction();
        stretchAffinityFunction.setPartitions(2000);
        cacheCfg.setAffinity(stretchAffinityFunction);

        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group", "B");
            put("role", "worker");
        }};

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setCacheConfiguration(cacheCfg);
        cfg.setUserAttributes(userAtt);


        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        // Region name.
        regionCfg.setName("80MB_Region");

        // Setting the size of the default memory region to 80MB to achieve this.
        regionCfg.setInitialSize(
                10L * 1024 * 1024);
        regionCfg.setMaxSize(80L * 1024 * 1024);


        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);

        storageCfg.setSystemRegionMaxSize(45L * 1024 * 1024);


        // Setting the data region configuration.
        storageCfg.setDefaultDataRegionConfiguration(regionCfg);


        // Applying the new configuration.
        cfg.setDataStorageConfiguration(storageCfg);
        //CacheConfiguration<GeoHashAsKey.GeoEntry, String>  cacheCfg1 = new CacheConfiguration(cacheName);

        // Start Ignite node.
        Ignite ignite = Ignition.start(cfg);

        try (IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheName)) {


            for (int i = 0; i < 10000; i++)
                cache.put(i, i);


            System.out.println("The partition for key: 5000 is:"+ignite.affinity(cacheName).partition(5000) );




        UUID id = ignite.cluster().forRemotes().node().id();
        System.out.println("The remote node id is: " + id);

        ignite.cluster().restartNodes(Collections.singleton(id));

        System.out.println("The partition for key: 50000 is:"+ignite.affinity(cacheName).partition(50000) );



        }


    }
}
