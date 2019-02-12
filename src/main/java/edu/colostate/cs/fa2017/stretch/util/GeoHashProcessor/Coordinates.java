package edu.colostate.cs.fa2017.stretch.util.GeoHashProcessor;


public class Coordinates {
    private float lat;
    private float lon;

    /**
     * Create Coordinates at the specified latitude and longitude.
     *
     * @param lat
     *     Latitude for this coordinate pair, in degrees.
     * @param lon
     *     Longitude for this coordinate pair, in degrees.
     */
    public Coordinates(float lat, float lon) {
        if(lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180){
            this.lat = lat;
            this.lon = lon;
        } else if(lat >= -180 && lat <=180 && lon >= -90 && lon <= 90){
            this.lat = lon;
            this.lon = lat;
        } else {
            throw new IllegalArgumentException("Illegal location. Valid range is Latitude [-90, 90] and Longitude[-180, 180].");
        }
    }

    /**
     * Get the latitude of this coordinate pair.
     *
     * @return latitude, in degrees.
     */
    public float getLatitude() {
        return lat;
    }

    /**
     * Get the longitude of this coordinate pair.
     *
     * @return longitude, in degrees
     */
    public float getLongitude() {
        return lon;
    }

    /**
     * Print this coordinate pair's String representation:
     * (lat, lon).
     *
     * @return String representation of the Coordinates
     */
    @Override
    public String toString() {
        return "(" + lat + ", " + lon + ")";
    }

}