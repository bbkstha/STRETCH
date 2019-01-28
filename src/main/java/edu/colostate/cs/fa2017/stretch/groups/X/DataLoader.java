package edu.colostate.cs.fa2017.stretch.groups.X;

import ch.hsr.geohash.GeoHash;
import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunctionX;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class DataLoader {

    private static final String cacheName = "STRETCH-CACHE";

    public static void main(String[] args) throws IOException {

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();

        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName(cacheName);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);

        StretchAffinityFunctionX stretchAffinityFunctionX = new StretchAffinityFunctionX(false, 1024);
        cacheConfiguration.setAffinity(stretchAffinityFunctionX);
        cacheConfiguration.setRebalanceMode(CacheRebalanceMode.ASYNC);

        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        // Region name.
        regionCfg.setName("100MB_Region");
        // Setting the size of the default memory region to 80MB to achieve this.
        regionCfg.setInitialSize(
                10L * 1024 * 1024);
        regionCfg.setMaxSize(100L * 1024 * 1024);
        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);
        storageCfg.setSystemRegionMaxSize(45L * 1024 * 1024);
        // Setting the data region configuration.
        storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        // Applying the new configuration.
        igniteConfiguration.setDataStorageConfiguration(storageCfg);



        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group","client");
            put("role", "loader");
            put("donated","no");

        }};
        igniteConfiguration.setCacheConfiguration(cacheConfiguration);
        igniteConfiguration.setUserAttributes(userAtt);
        igniteConfiguration.setClientMode(true);

        // Start Ignite node.
        Ignite ignite = Ignition.start(igniteConfiguration);


            IgniteCache<GeoEntry, String> cache = ignite.getOrCreateCache(cacheName);

            cache.clear();
            String path = "/s/chopin/b/grad/bbkstha/stretch/data/";

//            for(int i=0; i<100000000; i++)
//                cache.put(Integer.toString(i),Integer.toString(i));
            File folder = new File(path);
            File[] listOfFiles = folder.listFiles();
            String strLine;
            BufferedReader bufferReader = null;
            int counter = 0;
            GeoHashUtils geoHashUtils = new GeoHashUtils();



        for (File file : listOfFiles) {
                InputStream inputStream = new FileInputStream(file.getPath());
                InputStreamReader streamReader = new InputStreamReader(inputStream);
                BufferedReader br = new BufferedReader(streamReader);
                //Read File Line By Line

                while (( strLine= br.readLine()) != null) {
                    if (!strLine.startsWith("LAT")) {

                        counter++;
                        double lat = Double.parseDouble(strLine.split(",")[0]);
                        double lon = Double.parseDouble(strLine.split(",")[1]);
                        String timestamp = strLine.split(",")[2];

                        GeoEntry geoEntry = new GeoEntry(lat, lon, 3, timestamp);

                        System.out.println("The geohash is: "+geoEntry.geoHash);
                        cache.put(geoEntry, strLine);
                    }
                }
            }

        System.out.println("The value of counter is: "+counter);
    }

    public static class GeoEntry implements Serializable {

        private String geoHash;

        @AffinityKeyMapped
        private String subGeoHash;

        private String timestamp;

        private GeoEntry(){}

        private GeoEntry( double lat, double lon, int upperRange, String timestamp) {

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

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "GeoHash is: " + geoHash + " & subGeoHash is: " + subGeoHash;
        }

    }
}
