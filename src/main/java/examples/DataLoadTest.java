package examples;

import edu.colostate.cs.fa2017.stretch.affinity.RendezvousAffinityFunction;
import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DataLoadTest {

    private static final String CACHE_NAME = "TEST_CACHE";

    public static void main(String[] args){

        IgniteConfiguration cfg = new IgniteConfiguration();
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        StretchAffinityFunction stretchAffinityFunction = new StretchAffinityFunction();
        stretchAffinityFunction.setPartitions(200);
        //cacheCfg.setAffinity(stretchAffinityFunction);

        RendezvousAffinityFunction rendezvousAffinityFunction = new RendezvousAffinityFunction(false
        , 200);

        cacheCfg.setAffinity(rendezvousAffinityFunction);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.ASYNC);

        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group", "D");
            put("role", "master");
            put("donated","no");
        }};

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setUserAttributes(userAtt);
        cfg.setClientMode(false);
        // Start Ignite node.
        Ignite ignite = Ignition.start(cfg);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);
        // Clear caches before running example.
        cache.clear();

        for (int i = 0; i < 1000; i++) {

           cache.put(i, i);
           System.out.println(i);
           //cache.get(i);
        }
    }
}

