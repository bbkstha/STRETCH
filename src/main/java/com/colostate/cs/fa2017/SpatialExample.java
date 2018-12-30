package com.colostate.cs.fa2017;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import javax.cache.Cache;
import java.util.Collection;
import java.util.Random;

public class SpatialExample {

    private static final String CACHE_NAME = "MyCache";

    public static void main(String[] args) throws Exception {
        // Starting Ignite node.
        // Preparing the cache configuration.
        CacheConfiguration<Integer, SpatialExample.MapPoint> cc = new CacheConfiguration<>(CACHE_NAME);

        Ignite ignite = Ignition.start("/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/src/main/resources/example-ignite.xml");

        // Setting the indexed types.
        cc.setIndexedTypes(Integer.class, SpatialExample.MapPoint.class);
        //IgniteConfiguration igcfg = new IgniteConfiguration();
        ignite.addCacheConfiguration(cc);

        //try (Ignite ignite = Ignition.start(igcfg)) {

            System.out.println("Creating Cache...");
            // Starting the cache.
            try (IgniteCache<Integer, SpatialExample.MapPoint> cache = ignite.getOrCreateCache(CACHE_NAME)) {

                System.out.println("Created Cache.");
                Random rnd = new Random();

                WKTReader r = new WKTReader();

                // Adding geometry points into the cache.
                for (int i = 0; i < 1000; i++) {
                    int x = rnd.nextInt(10000);
                    int y = rnd.nextInt(10000);

                    Geometry geo = r.read("POINT(" + x + " " + y + ")");

                    cache.put(i, new SpatialExample.MapPoint(geo));
                }

                // Query to fetch the points that fit into a specific polygon.
                SqlQuery<Integer, SpatialExample.MapPoint> query = new SqlQuery<>(SpatialExample.MapPoint.class, "coords && ?");

                // Selecting points that fit into a specific polygon.
                for (int i = 0; i < 10; i++) {
                    // Defining the next polygon boundaries.
                    Geometry cond = r.read("POLYGON((0 0, 0 " + rnd.nextInt(10000) + ", " +
                            rnd.nextInt(10000) + " " + rnd.nextInt(10000) + ", " +
                            rnd.nextInt(10000) + " 0, 0 0))");

                    // Executing the query.
                    //Collection<Cache.Entry<Integer, SpatialExample.MapPoint>> entries = cache.query(query.setArgs(cond)).getAll();

                    // Printing number of points that fit into the area defined by the polygon.
                    //System.out.println("Fetched points [cond=" + cond + ", cnt=" + entries.size() + ']');
                }
            }
        }
   // }

    /**
     * MapPoint with indexed coordinates.
     */
    private static class MapPoint {
        /** Coordinates. */
        @QuerySqlField(index = true)
        private Geometry coords;

        /**
         * @param coords Coordinates.
         */
        private MapPoint(Geometry coords) {
            this.coords = coords;
        }
    }
}

