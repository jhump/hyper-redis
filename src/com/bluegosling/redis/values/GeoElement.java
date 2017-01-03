package com.bluegosling.redis.values;

import java.util.Objects;

public final class GeoElement<T> {
   private final double longitude;
   private final double lattitude;
   private final T value;
   
   public GeoElement(double longitude, double lattitude, T value) {
      this.longitude = longitude;
      this.lattitude = lattitude;
      this.value = value;
   }

   public GeoElement(GeoCoordinates coords, T value) {
      this(coords.longitude(), coords.lattitude(), value);
   }
   
   public double longitude() {
      return longitude;      
   }
   
   public double lattitude() {
      return lattitude;
   }
   
   public T get() {
      return value;
   }
   
   @Override public boolean equals(Object o) {
      if (o instanceof GeoElement) {
         GeoElement<?> other = (GeoElement<?>) o;
         return longitude == other.longitude && lattitude == other.lattitude
               && Objects.equals(value, other.value);
      }
      return false;
   }
   
   @Override public int hashCode() {
      return Double.hashCode(longitude) + Double.hashCode(lattitude) + Objects.hashCode(value);
   }

   @Override public String toString() {
      return String.valueOf(value) + " @ lat=" + lattitude + ", long=" + longitude;
   }
}
