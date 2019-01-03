package com.colostate.cs.fa2017;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.HashMap;
import java.util.Map;

public class ClusterGroupC1 {

    public static void main(String[] args) {

        String cacheName = "GeoCache";

        CacheConfiguration cacheCfg = new CacheConfiguration("MyCache");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);


        Map<String, String> userAtt = new HashMap<String, String>(){{put("group","C");
            put("role", "worker");}};

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setCacheConfiguration(cacheCfg);
        cfg.setUserAttributes(userAtt);

        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        // Region name.
        regionCfg.setName("40MB_Region");

        // Setting the size of the default memory region to 50MB to achieve this.
        regionCfg.setInitialSize(
                10L * 1024 * 1024);
        regionCfg.setMaxSize( 50L * 1024 * 1024);


        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);

        // Setting the data region configuration.
        storageCfg.setDataRegionConfigurations(regionCfg);


        // Applying the new configuration.
        cfg.setDataStorageConfiguration(storageCfg);


        //CacheConfiguration<GeoHashAsKey.GeoEntry, String>  cacheCfg1 = new CacheConfiguration(cacheName);

        // Start Ignite node.
        Ignite ignite = Ignition.start(cfg);

//        try (IgniteCache<Object, String> cache = ignite.getOrCreateCache(cacheName)) {
//
//
//            //
//        }


    }
}
