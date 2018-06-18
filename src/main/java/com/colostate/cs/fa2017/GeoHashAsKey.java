package com.colostate.cs.fa2017;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import ch.hsr.geohash.GeoHash;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;


import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class GeoHashAsKey {


    public static void main(String[] args) {

        CacheConfiguration cacheCfg = new CacheConfiguration("myCache");

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheCfg);

        String cacheName = "SpatialDataCache1025";

        // Start Ignite node.
        //Ignition.start(cfg);
        Ignite ignite = Ignition.start("/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-fabric-2.2.0-bin/examples/config/example-memory-policies.xml");

            try (IgniteCache<String, String> cache = ignite.getOrCreateCache(cacheName)) {
                // Clear caches before running example.
                cache.clear();

                FileInputStream fstream = new FileInputStream("/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/src/main/resources/2.csv");
                BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
                String strLine;
                Integer counter = 0;
                //Read File Line By Line
                while ((strLine = br.readLine()) != null) {
                    // Print the content on the console
                    //System.out.println(strLine);

                    if (!strLine.startsWith("LON")) {

                        String lon = strLine.split(",")[1];
                        String lat = strLine.split(",")[2];
                        String geohashString = GeoHash.withCharacterPrecision(Double.parseDouble(lat), -Double.parseDouble(lon), 12).toBase32();
                        //System.out.println(strLine);
                       // System.out.println(geohashString);
                        String key = geohashString;//.substring(0,6);
                        cache.put(key, strLine);
                        //visitUsingAffinityRun(geohashString.substring(0,7).hashCode());
                        String value = cache.get(key);

                        counter++;
                         //System.out.println(counter+". The value of key:"+key+" is: "+value);
                    }
                }
                br.close();

                Affinity<Object> affinity = ignite.affinity(cacheName);
                // Building a list of all partitions numbers.
                List<Integer> rndParts = new ArrayList<>(10);
                for (int i = 0; i < affinity.partitions(); i++)
                    rndParts.add(i);
                // Getting partition to node mapping.
                Map<Integer, ClusterNode> partPerNodes = affinity.mapPartitionsToNodes(rndParts);
                //System.out.println("The partition->node map gives: "+partPerNodes.size());
                for (Map.Entry<Integer, ClusterNode> entry : partPerNodes.entrySet()) {
                    //System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                }
                int primaryCacheSize = cache.size(CachePeekMode.PRIMARY);
                //System.out.println("The size of primary cache entities is: "+primaryCacheSize);

                for(int i =0; i<1024; i++)
                    System.out.println("Partition: "+i+". NumberofEntries: "+cache.sizeLong(i, CachePeekMode.PRIMARY));



                ignite.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    private static void visitUsingAffinityRun(final int key) {
        Ignite ignite = Ignition.ignite();

        final IgniteCache<Integer, String> cache = ignite.cache("SpatialDataCache");

//        for (int i = 0; i < KEY_CNT; i++) {
//            final int key = i;

            // This runnable will execute on the remote node where
            // data with the given key is located. Since it will be co-located
            // we can use local 'peek' operation safely.
            ignite.compute().affinityRun("SpatialDataCache", key, new IgniteRunnable() {
                @Override public void run() {
                    // Peek is a local memory lookup, however, value should never be 'null'
                    // as we are co-located with node that has a given key.
                    System.out.println("Co-located using affinityRun [key= " + key +
                            ", value=" + cache.localPeek(key) + ']');
                }
            });
        }
    }
