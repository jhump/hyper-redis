package com.bluegosling.redis.values;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

public final class GeoResult<T> {
   private final T value;
   private final OptionalLong geoHash;
   private final OptionalDouble distance;
   private final Optional<GeoCoordinates> coords;
   
   GeoResult(T value, OptionalLong geoHash, OptionalDouble distance,
         Optional<GeoCoordinates> coords) {
      this.value = value;
      this.geoHash = geoHash;
      this.distance = distance;
      this.coords = coords;
   }
   
   public T get() {
      return value;
   }
   
   public OptionalLong geoHash() {
      return geoHash;
   }
   
   public OptionalDouble distance() {
      return distance;
   }
   
   public Optional<GeoCoordinates> coordinates() {
      return coords;
   }

   // TODO: hashCode, equals, etc
   
   public static final class Builder<T> {
      private final T value;
      private OptionalLong geoHash = OptionalLong.empty();
      private OptionalDouble distance = OptionalDouble.empty();
      private Optional<GeoCoordinates> coords = Optional.empty();
      
      public Builder(T value) {
         this.value = value;
      }
      
      public Builder<T> geoHash(long geoHash) {
         this.geoHash = OptionalLong.of(geoHash);
         return this;
      }

      public Builder<T> distance(double distance) {
         this.distance = OptionalDouble.of(distance);
         return this;
      }
      
      public Builder<T> coordinates(GeoCoordinates coords) {
         this.coords = Optional.of(coords);
         return this;
      }
      
      public Builder<T> coordinates(double longitude, double lattitude) {
         return coordinates(new GeoCoordinates(longitude, lattitude));
      }

      public GeoResult<T> build() {
         return new GeoResult<>(value, geoHash, distance, coords);
      }
   }
}
