package examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.HashMap;
import java.util.Map;

public class DataLoadDonatedNode {


        private static final String CACHE_NAME = "TEST_CACHE";

        public static void main(String[] args){

            IgniteConfiguration cfg = new IgniteConfiguration();
//            CacheConfiguration cacheCfg = new CacheConfiguration();
//
//            cacheCfg.setName(CACHE_NAME);
//            cacheCfg.setCacheMode(CacheMode.PARTITIONED);
//            StretchAffinityFunction stretchAffinityFunction = new StretchAffinityFunction();
//            stretchAffinityFunction.setPartitions(200);
//
//            StretchAffinityFunctionX rendezvousAffinityFunction = new StretchAffinityFunctionX(false
//                    , 200);
//
//            cacheCfg.setAffinity(rendezvousAffinityFunction);
//            cacheCfg.setRebalanceMode(CacheRebalanceMode.ASYNC);

            Map<String, String> userAtt = new HashMap<String, String>() {{
                //put("group", "D");
                //put("role", "master");
                put("donated","yes");
                put("hotspot_partitions","25,100,150");
            }};

           // cfg.setCacheConfiguration(cacheCfg);
            cfg.setUserAttributes(userAtt);
            cfg.setClientMode(false);
            // Start Ignite node.

            Ignite ignite = Ignition.start(cfg);



        }
    }
