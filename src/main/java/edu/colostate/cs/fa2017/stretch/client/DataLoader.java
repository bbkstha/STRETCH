package edu.colostate.cs.fa2017.stretch.client;

import ch.hsr.geohash.GeoHash;
import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunction;
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;


import java.io.*;
import java.util.*;

public class DataLoader {

    private static final String cacheName = "STRETCH-CACHE";

    private static final String outputFileName = "/s/chopin/b/grad/bbkstha/stretch/output.txt";

    public static void main(String[] args) throws IOException, InterruptedException, IgniteCheckedException {

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        igniteConfiguration.setMetricsUpdateFrequency(2000);
        igniteConfiguration.setMetricsLogFrequency(60000);
        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        regionCfg.setMetricsEnabled(true);

        // Region name.
        regionCfg.setName("Client_Region");
        // Setting the size of the default memory region to 80MB to achieve this.
        regionCfg.setInitialSize(
                50L * 1024 * 1024);
        regionCfg.setMaxSize(300L * 1024 * 1024);
        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);

        //Setting the size of the default memory region to 4GB to achieve this.
        //storageCfg.getDefaultDataRegionConfiguration().setMaxSize(250L * 1024 * 1024);
        //cfg.setDataStorageConfiguration(storageCfg);
        storageCfg.setSystemRegionInitialSize(100L * 1024 * 1024);
        storageCfg.setSystemRegionMaxSize(5000L * 1024 * 1024);
        storageCfg.setMetricsEnabled(true);

        //Setting the data region configuration.
        //storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        //Applying the new configuration.
        igniteConfiguration.setDataStorageConfiguration(storageCfg);
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setManagementEnabled(true);
        cacheConfiguration.setStatisticsEnabled(true);
        cacheConfiguration.setName(cacheName);
        cacheConfiguration.setOnheapCacheEnabled(false);
        igniteConfiguration.setCacheConfiguration(cacheConfiguration);
        igniteConfiguration.setSystemThreadPoolSize(16);
        igniteConfiguration.setRebalanceThreadPoolSize(6);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);
        StretchAffinityFunction stretchAffinityFunction = new StretchAffinityFunction(false, 10000);
        cacheConfiguration.setAffinity(stretchAffinityFunction);
        cacheConfiguration.setRebalanceMode(CacheRebalanceMode.ASYNC);
        cacheConfiguration.setRebalanceBatchSize(512 * 1024 * 2 * 10);
        cacheConfiguration.setRebalanceBatchesPrefetchCount(4);
        cacheConfiguration.setStatisticsEnabled(true);
        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group","loader");
            put("role", "client");
            put("donated","no");
            put("region-max", "200");
            put("split","no");
            put("map","./KeyToPartitionMap-X.ser");
        }};
        igniteConfiguration.setUserAttributes(userAtt);
        igniteConfiguration.setClientMode(false);

        // Start Ignite node.
        Ignite ignite = Ignition.start(igniteConfiguration);

        //ignite.cluster().resetMetrics();
        IgniteCache<GeoEntry, String> cache = ignite.getOrCreateCache(cacheConfiguration);
        cache.clear();
       IgniteDataStreamer<GeoEntry, String> cacheStreamer = ignite.dataStreamer(cacheName);
       Affinity affinity = ignite.affinity(cacheName);
       if(args.length < 1){

           System.out.println("Input Data Path required. ");
       }
       String path = args[0];

       List<File> fileList = listf(path);
       String strLine;
       BufferedReader bufferReader = null;
       long counter = 0;
       GeoEntry tmpGeoEntry = null;
       long sum = 0;
       GeoHashUtils geoHashUtils = new GeoHashUtils();
       File oldFile = new File(outputFileName);
       if(oldFile.isFile()){
           //System.out.println("OLD FILE EXIST");
           oldFile.delete();
       }
       long startTime = System.currentTimeMillis();
       BufferedWriter bw = null;
       bw = new BufferedWriter(new FileWriter(outputFileName));

       for (File file : fileList) {
           InputStream inputStream = new FileInputStream(file.getPath());
           InputStreamReader streamReader = new InputStreamReader(inputStream);
           BufferedReader br = new BufferedReader(streamReader);
           while (( strLine= br.readLine()) != null) {
               if (!strLine.startsWith("LAT")) {
                   String[] eachColumn = strLine.split(",");
                   double lat = Double.parseDouble(eachColumn[0]);
                   double lon = Double.parseDouble(eachColumn[1]);
                   String timestamp = eachColumn[2]; //.substring(0, eachColumn[2].indexOf("."));
                   counter++;
                   GeoEntry geoEntry = new GeoEntry(lat, lon, 12, timestamp);
                   cache.put(geoEntry, strLine);
                   //cacheStreamer.addData(geoEntry, strLine);
               }
           }
       }
       long endTime = System.currentTimeMillis();
       // cache.destroy();

        try {
            if(bw != null)
                bw.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public static class GeoEntry implements Serializable {
        @AffinityKeyMapped
        private String geoHash;
        private String subGeoHash;
        private String timestamp;
        public GeoEntry(){}
        public GeoEntry( double lat, double lon, int upperRange, String timestamp) {

            this.geoHash = GeoHash.withCharacterPrecision(lat,lon, 12).toBase32();
            this.subGeoHash = this.geoHash.substring(0, upperRange);
            this.timestamp = timestamp;
        }
        private String getGeoHash() {
            return this.geoHash;
        }
        private String getSubGeoHash() {
            return subGeoHash;
        }
        private String getTimestamp() {
            return timestamp;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "GeoHash is: " + geoHash + " & subGeoHash is: " + subGeoHash + " and timestamp: "+timestamp;
        }
    }

    public static List<File> listf(String directoryName) {
        File directory = new File(directoryName);

        List<File> resultList = new ArrayList<File>();

        // get all the files from a directory
        File[] fList = directory.listFiles();
        resultList.addAll(Arrays.asList(fList));
        for (File file : fList) {
            if (file.isFile()) {
                //System.out.println(file.getAbsolutePath());
            } else if (file.isDirectory()) {
                resultList.remove(file);
                resultList.addAll(listf(file.getAbsolutePath()));
            }
        }
        //System.out.println(fList);
        return resultList;
    }

}

