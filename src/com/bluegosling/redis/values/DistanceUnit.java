package com.bluegosling.redis.values;

public enum DistanceUnit {
   METERS("m") {
      @Override public double toMeters(double distance) {
         return distance;
      }
      @Override public double toKilometers(double distance) {
         return distance / 1000.0;
      }
      @Override public double toMiles(double distance) {
         return FEET.toMiles(toFeet(distance));
      }
      @Override public double toFeet(double distance) {
         return distance * 1.0936 * 3;
      }
      @Override public double convert(double distance, DistanceUnit unit) {
         return unit.toMeters(distance);
      }
   },
   KILOMETERS("km") {
      @Override public double toMeters(double distance) {
         return distance * 1000.0;
      }
      @Override public double toKilometers(double distance) {
         return distance;
      }
      @Override public double toMiles(double distance) {
         return METERS.toMiles(toMeters(distance));
      }
      @Override public double toFeet(double distance) {
         return METERS.toFeet(toMeters(distance));
      }
      @Override public double convert(double distance, DistanceUnit unit) {
         return unit.toKilometers(distance);
      }
   }, 
   MILES("mi") {
      @Override public double toMeters(double distance) {
         return FEET.toMeters(toFeet(distance));
      }
      @Override public double toKilometers(double distance) {
         return METERS.toKilometers(toMeters(distance));
      }
      @Override public double toMiles(double distance) {
         return distance;
      }
      @Override public double toFeet(double distance) {
         return distance * 5280.0;
      }
      @Override public double convert(double distance, DistanceUnit unit) {
         return unit.toMiles(distance);
      }
   },
   FEET("ft") {
      @Override public double toMeters(double distance) {
         return distance * 3 / 1.0936;
      }
      @Override public double toKilometers(double distance) {
         return METERS.toKilometers(toMeters(distance));
      }
      @Override public double toMiles(double distance) {
         return distance / 5280.0;
      }
      @Override public double toFeet(double distance) {
         return distance;
      }
      @Override public double convert(double distance, DistanceUnit unit) {
         return unit.toFeet(distance);
      }
   };
   
   private final String abbrev;
   
   DistanceUnit(String abbrev) {
      this.abbrev = abbrev;
   }
   
   @Override public String toString() {
      return abbrev;
   }
   
   public abstract double toMeters(double distance);
   public abstract double toKilometers(double distance);
   public abstract double toMiles(double distance);
   public abstract double toFeet(double distance);
   public abstract double convert(double distance, DistanceUnit otherUnit);
}
