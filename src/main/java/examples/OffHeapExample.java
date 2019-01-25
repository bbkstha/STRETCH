package examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class OffHeapExample {

    public static void main(String[] args) throws Exception{
//        if(args.length <= 0){
//            System.out.println("Usages! java -jar .\\target\\chapter-three-offheap-1.0-SNAPSHOT.one-jar.jar spring-offheap-tiered.xml");
//            System.exit(0);
//        }
        //String springCoreFile = "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/src/main/resources/offheap/spring-offheap-tiered.xml";
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setMemoryPolicyName("OFFHEAP_TIERED");

// Set off-heap memory to 10GB (0 for unlimited)
        //cacheCfg.setOffHeapMaxMemory(10 * 1024L * 1024L * 1024L);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheCfg);

// Start Ignite node.

        // Start Ignite cluster
        Ignite ignite = Ignition.start(cfg);
        // get or create cache
        IgniteCache<Integer, String> cache =  ignite.getOrCreateCache("offheap-cache");
        for(int i = 1; i <= 1000000; i++){
            cache.put(i, Integer.toString(i));
        }
        for(int i =1; i<=1000000;i++){
            System.out.println("Cache get:"+ cache.get(i));
        }
        System.out.println("Wait 10 seconds for statistics");
        Thread.sleep(10000);
        // statistics
        System.out.println("Cache Hits:"+ cache.metrics(ignite.cluster()).getCacheHits());

        System.out.println("Enter crtl-x to quite the application!!!");
        Thread.sleep(Integer.MAX_VALUE); // sleep for 20 seconds

        ignite.close();
    }
}

