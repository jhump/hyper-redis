package com.bluegosling.redis.values;

public final class GeoCoordinates {
   private final double longitude;
   private final double lattitude;
   
   public GeoCoordinates(double longitude, double lattitude) {
      this.longitude = longitude;
      this.lattitude = lattitude;
   }
   
   public double longitude() {
      return longitude;      
   }
   
   public double lattitude() {
      return lattitude;
   }
   
   @Override public boolean equals(Object o) {
      if (o instanceof GeoCoordinates) {
         GeoCoordinates other = (GeoCoordinates) o;
         return longitude == other.longitude && lattitude == other.lattitude;
      }
      return false;
   }
   
   @Override public int hashCode() {
      return Double.hashCode(longitude) + Double.hashCode(lattitude);
   }

   @Override public String toString() {
      return "lat=" + lattitude + ", long=" + longitude;
   }
}
