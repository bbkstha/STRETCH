package edu.colostate.cs.fa2017.stretch.groups.X;

import ch.hsr.geohash.GeoHash;
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

    public static void main(String[] args){

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();

        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName(cacheName);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);
        cacheConfiguration.setRebalanceMode(CacheRebalanceMode.ASYNC);
        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        // Region name.
        regionCfg.setName("80MB_Region");
        // Setting the size of the default memory region to 80MB to achieve this.
        regionCfg.setInitialSize(
                10L * 1024 * 1024);
        regionCfg.setMaxSize(80L * 1024 * 1024);
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
        }};
        igniteConfiguration.setCacheConfiguration(cacheConfiguration);
        igniteConfiguration.setUserAttributes(userAtt);
        igniteConfiguration.setClientMode(true);

        // Start Ignite node.
        try (Ignite ignite = Ignition.start(igniteConfiguration)) {

            IgniteCache<GeoEntry, String> cache = ignite.getOrCreateCache(cacheName);

            cache.clear();
            String path = "/s/chopin/b/grad/bbkstha/stretch/data/";

//            for(int i=0; i<100000000; i++)
//                cache.put(Integer.toString(i),Integer.toString(i));
            File folder = new File(path);
            File[] listOfFiles = folder.listFiles();
            String strLine;
            BufferedReader bufferReader = null;
            for (File file : listOfFiles) {
                InputStream inputStream = new FileInputStream(file.getPath());
                InputStreamReader streamReader = new InputStreamReader(inputStream);
                BufferedReader br = new BufferedReader(streamReader);
                //Read File Line By Line
                while (( strLine= br.readLine()) != null) {
                    if (!strLine.startsWith("LAT")) {

                        String lat = strLine.split(",")[0];
                        String lon = strLine.split(",")[1];
                        GeoEntry geoEntry = new GeoEntry(lat, lon, 8);
                        cache.put(geoEntry, strLine);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class GeoEntry implements Serializable {

        private String geoHash;

        @AffinityKeyMapped
        private String subGeoHash;

        private GeoEntry(String lat, String lon, int upperRange) {

            this.geoHash = GeoHash.withCharacterPrecision(Double.parseDouble(lat), -Double.parseDouble(lon), 12).toBase32();
            this.subGeoHash = this.geoHash.substring(0, upperRange);

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
