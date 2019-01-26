package edu.colostate.cs.fa2017.stretch.groups.X;

import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunction;
import examples.ResourceRequest;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.HashMap;
import java.util.Map;

public class ClusterMaster {

    private static final String cacheName = "Stretch-Cache";

    public static void main(String[] args){

        if(args.length<1){
            return;
        }
        String groupName = args[0];
        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName(cacheName);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);

        StretchAffinityFunction stretchAffinityFunction = new StretchAffinityFunction();
        stretchAffinityFunction.setPartitions(10240);
        cacheConfiguration.setAffinity(stretchAffinityFunction);
        cacheConfiguration.setRebalanceMode(CacheRebalanceMode.ASYNC);
        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group",groupName);
            put("role", "master");
        }};
        igniteConfiguration.setCacheConfiguration(cacheConfiguration);
        igniteConfiguration.setUserAttributes(userAtt);
        igniteConfiguration.setClientMode(false);

        // Start Ignite node.
        try (Ignite ignite = Ignition.start(igniteConfiguration)) {

            ClusterGroup masterGroup = ignite.cluster().forAttribute("role", "master");
            ClusterGroup clusterGroup = ignite.cluster().forAttribute("group",groupName);

            IgniteMessaging mastersMessanger = ignite.message(masterGroup);


            //All other listeners here!!





            ResourceRequest resourceRequest = new ResourceRequest(mastersMessanger, ignite, groupName, cacheName);
            Thread t = new Thread(resourceRequest);
            t.start();







        }



    }



}
