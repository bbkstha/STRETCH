package com.colostate.cs.fa2017;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import ch.hsr.geohash.GeoHash;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;


import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class GeoHashAsKey {


    public static void main(String[] args) {

//        CacheConfiguration cacheCfg = new CacheConfiguration("myCache");
//        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
//
//        IgniteConfiguration cfg = new IgniteConfiguration();
//        cfg.setCacheConfiguration(cacheCfg);

        String cacheName = "myCustomCache";

        // Start Ignite node.
        //Ignition.start(cfg);
        Ignite ignite = Ignition.start("/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-fabric-2.2.0-bin/examples/config/example-memory-policies.xml");

            try (IgniteCache<Object, String> cache = ignite.getOrCreateCache(cacheName)) {
                // Clear caches before running example.
                cache.clear();
                Affinity<Object> affinity = ignite.affinity(cacheName);

                List<Object> geoHashArray = new ArrayList<>();


                FileInputStream fstream = new FileInputStream("/s/chopin/b/grad/bbkstha/Desktop/GeospatialSample/3.csv"); //us/or/portland_metro.csv");
                BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
                String strLine;
                Integer counter = 0;
                //Read File Line By Line
                while ((strLine = br.readLine()) != null) {
                    // Print the content on the console
                    //System.out.println(strLine);

                    if (!strLine.startsWith("LON")) {

                        String lon = strLine.split(",")[0];
                        String lat = strLine.split(",")[1];
                        String geohashString = GeoHash.withCharacterPrecision(Double.parseDouble(lat), -Double.parseDouble(lon), 12).toBase32();
                        //System.out.println(strLine);
                       // System.out.println(geohashString);
                        String affkey = geohashString.substring(0,4);

                        Object newKey = new AffinityKey<>(geohashString,affkey);

                        geoHashArray.add(geohashString);




                        cache.put(newKey, strLine);

                        //Object aKey = affinity.affinityKey(newKey);
                        //System.out.println("The affKey for KEY: "+newKey.toString()+" is: "+aKey.toString());
                        int partID = affinity.partition(newKey);
                        System.out.println("The partitionID for KEY: "+newKey+" is: "+partID);

                        //visitUsingAffinityRun(geohashString.substring(0,7).hashCode());
                        //String value = cache.get(key);

                        counter++;
                         //System.out.println(counter+". The value of key:"+key+" is: "+value);
                    }
                }













                // Building a list of all partitions numbers.
                List<Integer> rndParts = new ArrayList<>(10);
                for (int i = 0; i < affinity.partitions(); i++)
                    rndParts.add(i);
                // Getting partition to node mapping.
                Map<Integer, ClusterNode> partPerNodes = affinity.mapPartitionsToNodes(rndParts);
                System.out.println("The partition->node map gives: "+partPerNodes.size());

                PrintWriter writer = new PrintWriter("/s/chopin/b/grad/bbkstha/Desktop/GeospatialSample/observation/PartitionToNodeMap_RAffinity.txt", "UTF-8");
                for (Map.Entry<Integer, ClusterNode> entry : partPerNodes.entrySet()){
                    writer.println("Partition Number: " + entry.getKey() + ", Node: " + entry.getValue()+"\n");
                }
                writer.close();

                int primaryCacheSize = cache.size(CachePeekMode.PRIMARY);
                System.out.println("The size of primary cache entities is: "+primaryCacheSize);
//
//                //for(int i =0; i<1024; i++)
//                    //System.out.println("Partition: "+i+". NumberofEntries: "+cache.sizeLong(i, CachePeekMode.PRIMARY));
//
//                PrintWriter writer1 = new PrintWriter("/s/chopin/b/grad/bbkstha/Desktop/GeospatialSample/observation/KeyToPartitionMap_RAffninty.txt", "UTF-8");
//                HashMap<Integer, ArrayList<Object>> partIdtoKey = new HashMap();
//
//                        for(Object eachGeoHash: geoHashArray){
//
//                            Integer partId = affinity.partition(eachGeoHash);
//                            if (partIdtoKey.containsKey(partId)) {
//                                ArrayList<Object> tmpArrayList = partIdtoKey.get(partId);
//                                tmpArrayList.add(eachGeoHash);
//                                partIdtoKey.put(partId, tmpArrayList);
//                            } else {
//                                ArrayList<Object> tmpArrayList = new ArrayList<>();
//                                tmpArrayList.add(eachGeoHash);
//                                partIdtoKey.put(partId, tmpArrayList);
//                            }
//                        }
//
//
//                ArrayList<Object> emptyList = new ArrayList<>(Arrays.asList("NO-Key"));
//                for(int i =0 ; i<partIdtoKey.size(); i++) {
//                    ArrayList<Object> values = (partIdtoKey.get(i)!=null) ? partIdtoKey.get(i) : emptyList;
//                    //System.out.print("The PartitionId is: "+i+ " and keys are: ");
//                    //System.out.println(i + "=>" + values.size());
//                    writer1.print("PartitionId: " + i + "|| Keys: ");
//
//                        for (int j = 0; j < values.size(); j++) {
//                            //System.out.print(values.get(j)+", ");
//                            writer1.print(values.get(j) + ", ");
//                            writer1.println("\n");
//                        }
//
//
//                    writer1.println("-----------------------------------------------------");
//                }
//                writer1.close();
//                br.close();
//                System.out.println("Finished.");


               // ignite.close();

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
