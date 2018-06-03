import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import ch.hsr.geohash.GeoHash;
import org.apache.ignite.lang.IgniteRunnable;


import java.io.*;

public class GeoHashAsKey {


    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-fabric-2.2.0-bin/examples/config/example-ignite.xml")) {
            try (IgniteCache<Integer, String> cache = ignite.createCache("SpatialDataCache")) {
                // Clear caches before running example.
                cache.clear();

                FileInputStream fstream = new FileInputStream("/s/chopin/b/grad/bbkstha/Downloads/NewFolder/us/or/portland_metro.csv");
                BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

                String strLine;


                //Read File Line By Line
                while ((strLine = br.readLine()) != null) {
                    // Print the content on the console
                    //System.out.println(strLine);

                    if (!strLine.startsWith("LON")) {

                        String lon = strLine.split(",")[0];
                        String lat = strLine.split(",")[1];
                        String geohashString = GeoHash.withCharacterPrecision(Double.parseDouble(lat), -Double.parseDouble(lon), 8).toBase32();
                        //System.out.println(strLine);
                       // System.out.println(geohashString);
                        cache.put(geohashString.substring(0,7).hashCode(), strLine);
                        visitUsingAffinityRun(geohashString.substring(0,7).hashCode());

                    }
                }
                br.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }


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
