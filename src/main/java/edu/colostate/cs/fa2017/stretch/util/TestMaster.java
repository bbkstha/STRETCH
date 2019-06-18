package edu.colostate.cs.fa2017.stretch.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteClosure;

import java.util.*;

public class TestMaster {

    public static void main(String[] args) {

        IgniteConfiguration cfg = new IgniteConfiguration();
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName("TEST_CACHE");
        cacheCfg.setStatisticsEnabled(true);
        cfg.setCacheConfiguration(cacheCfg);
        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group", "TEST");
            put("role", "master");
            put("donated", "no");
        }};

        cfg.setUserAttributes(userAtt);

        // Start Ignite node.
        Ignite ignite = Ignition.start(cfg);


        String cacheName = "TEST_CACHE";
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheCfg);
        cache.clear();
        for(int i=0; i<10000; i++)
            cache.put(i,i);






        Affinity affinity = ignite.affinity("TEST_CACHE");





        ClusterGroup node = ignite.cluster().forAttribute("role","worker");
        //ClusterGroup worker = ignite.cluster().forNode(node);


        int[] part = affinity.allPartitions(node.node());

        Map<Integer, Long>  map = ignite.compute(node).apply(

                new IgniteClosure< Integer, Map<Integer, Long> >() {
                    @Override public Map<Integer, Long>  apply(Integer x) {
                        x--;
                        Map<Integer, Long> partToCount = new HashMap();
                        for(int i=0; i<part.length; i++){
                            Long ig = cache.localSizeLong(part[i], CachePeekMode.PRIMARY);
                            partToCount.put(part[i], ig);
                        }
                        return partToCount;
                    }
                },
                5);

        for(int j =0; j< part.length; j++){

            System.out.println("The partition is: "+part[j]+" and the count is: "+map.get(part[j]));

        }



//
//        Collection<Long> res = ignite.compute(worker).apply(
//                new IgniteClosure<Ignite, Long>() {
//                    @Override
//                    public Long apply(Ignite ignite) {
//                        // Return number of letters in the word.
//
//                        final long test_cache = ignite.cache("TEST_CACHE").localMetrics().getOffHeapPrimaryEntriesCount();
//                        return test_cache;
//                    }
//                }, Arrays.asList("How ".split(" ")));
//
//        System.out.println("The size: "+res.size());

    }
}