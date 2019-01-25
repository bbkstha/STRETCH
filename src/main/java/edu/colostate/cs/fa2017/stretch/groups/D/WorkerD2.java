package edu.colostate.cs.fa2017.stretch.groups.D;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.HashMap;
import java.util.Map;

public class WorkerD2 {

    public static void main(String[] args) {

        String cacheName = "MyCache";

        CacheConfiguration cacheCfg = new CacheConfiguration("MyCache");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setOnheapCacheEnabled(false);

        Map<String, String> userAtt = new HashMap<String, String>(){{put("group","A");
            put("role", "worker");}};

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setCacheConfiguration(cacheCfg);
        cfg.setUserAttributes(userAtt);


        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        regionCfg.setName("80MB_Region");


        // Setting the size of the default memory region to 40MB to achieve this.
        regionCfg.setInitialSize(
                10L * 1024 * 1024);
        regionCfg.setMaxSize( 500L * 1024 * 1024);


        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(true);

        storageCfg.setSystemRegionMaxSize(45L * 1024 * 1024);

        // Setting the data region configuration.
        storageCfg.setDataRegionConfigurations(regionCfg);

        // Applying the new configuration.
        cfg.setDataStorageConfiguration(storageCfg);

        // Start Ignite node.
        Ignite ignite = Ignition.start(cfg);

        //ClusterGroup clusterGroupA2 = ignite.cluster().forAttribute("role","worker");


       // IgniteCache<Object, String> cache = ignite.getOrCreateCache(cacheName);
            //cache.rebalance();

            //



    }
}
