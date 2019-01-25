package edu.colostate.cs.fa2017.stretch.groups.A;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ClusterGroupA1 {

    public static void main(String[] args) {

        String cacheName = "MyCache";

        CacheConfiguration cacheCfg = new CacheConfiguration("MyCache");
        cacheCfg.setCacheMode(CacheMode.REPLICATED);
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
        //ignite.active(false);



            IgniteCache<Object, String> cache = ignite.getOrCreateCache(cacheName);
            UUID localUUID = ignite.cluster().localNode().id();

            System.out.println("The local UUID: "+localUUID.toString());

            System.out.println("Sleep starts.");


        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Sleep ends.");

            ignite.cluster().restartNodes();

            TcpDiscoverySpi tcpDisco = (TcpDiscoverySpi)ignite.configuration().getDiscoverySpi();
            TcpDiscoveryIpFinder tcpDiscoIpFinder = tcpDisco.getIpFinder();

            tcpDisco.getNodesJoined();

            CacheConfiguration[] cacheConfiguration = ignite.configuration().getCacheConfiguration();
            AffinityFunction affinityFunctions = cacheConfiguration[0].getAffinity();
            //AffinityFunctionContext affinityFunctionContext = affinityFunctions.getAffinityFunctionContext();

            //affinityFunctions.assignPartitions(affinityFunctionContext);






    }
}
