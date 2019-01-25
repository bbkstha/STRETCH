package examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class CustomAffinityTest {

   final static String cacheName = "CustomAffinityTestCache";


    public static void main(String[] args){

        // Preparing Apache Ignite node configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();

// Creating a cache configuration.
        CacheConfiguration cacheCfg = new CacheConfiguration();

// Creating the affinity function with custom setting.
        RendezvousAffinityFunction affFunc = new RendezvousAffinityFunction();

        affFunc.setExcludeNeighbors(true);

        affFunc.setPartitions(2048);

// Applying the affinity function configuration.
        cacheCfg.setAffinity(affFunc);

        cacheCfg.setName("test-cache");

// Setting the cache configuration.
        cfg.setCacheConfiguration(cacheCfg);

        Ignite ignite = Ignition.start(cfg);
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheCfg);

        for (int i=0;i<100; i++){
            cache.put(i,i);
            System.out.println(i);
            cache.get(i);
        }
    }
}
