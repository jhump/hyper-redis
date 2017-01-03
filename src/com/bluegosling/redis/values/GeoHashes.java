package com.bluegosling.redis.values;

import java.util.HashMap;
import java.util.Map;

public final class GeoHashes {
   private GeoHashes() { throw new AssertionError(); }
   
   /**
    * The largest size of an encoded lattitude or longitude component when using 52-bit hashes.
    * With a 52-bit hash, each component gets 26 bits. In 26 bits, the largest value we can
    * represent is 2<sup>26</sup>-1.
    */
   private static final long MAX_COMPONENT = (1L << 26L) - 1;
   
   enum ProjectionType {
      MERCATOR(85.05112878),
      STANDARD(90);
      
      final double lattitudeLimit;
      
      ProjectionType(double lattitudeLimit) {
         this.lattitudeLimit = lattitudeLimit;
      }
   }

   static final long MAX = (1L << 52L) - 1L;
   private static final char[] ALPHABET = "0123456789bcdefghjkmnpqrstuvwxyz".toCharArray();
   private static final Map<Character, Integer> ALPHABET_INVERSE;
   static {
      ALPHABET_INVERSE = new HashMap<>();
      for (int i = 0; i < ALPHABET.length; i++) {
         ALPHABET_INVERSE.put(ALPHABET[i], i);
      }
   }

   public static GeoCoordinates coordsFromNumericHash(long hash) {
      return decodeFromLong(hash, ProjectionType.MERCATOR);
   }
   
   private static GeoCoordinates decodeFromLong(long hash, ProjectionType proj) {
      // split lat and long in the hash into low and high 32-bit words
      long components = deinterleave(hash);
      long latComponent = components & 0xffffffffL;
      long lonComponent = components >>> 32L;
      // compute lattitude
      double lattitudeLimit = proj.lattitudeLimit;
      double latMin = (latComponent * 1.0 / MAX_COMPONENT) * (lattitudeLimit * 2) - lattitudeLimit;
      double latMax = ((latComponent + 1) * 1.0 / MAX_COMPONENT) * (lattitudeLimit * 2)
            - lattitudeLimit;
      // and longitude
      double lonMin = (lonComponent * 1.0 / MAX_COMPONENT) * 360 - 180;
      double lonMax = ((lonComponent + 1) * 1.0 / MAX_COMPONENT) * 360 - 180;
      // use middle of geohash tile as the coordinate
      return new GeoCoordinates((lonMin + lonMax) / 2, (latMin + latMax) / 2);
   }
   
   public static GeoCoordinates coordsFromStringHash(String hash) {
      return decodeFromLong(decodeFromString(hash), ProjectionType.STANDARD);
   }

   public static long numericHashFromCoords(GeoCoordinates coords) {
      return numericHashFromCoords(coords.longitude(), coords.lattitude());
   }

   public static long numericHashFromCoords(double longitude, double lattiude) {
      return encodeToLong(longitude, lattiude, ProjectionType.MERCATOR);
   }

   private static long encodeToLong(double longitude, double lattitude,
         ProjectionType proj) {
      if (lattitude >= proj.lattitudeLimit || lattitude < -proj.lattitudeLimit) {
         throw new IllegalArgumentException("given geohash cannot be correctly represented because"
               + " lattitude " + lattitude + " is not in the acceptable range: "
               + "-" + proj.lattitudeLimit + " -> " + proj.lattitudeLimit);
      }
      if (longitude >= 180 || longitude < -180) {
         throw new IllegalArgumentException("given geohash cannot be correctly represented because"
               + " longitude " + longitude + " is not in the acceptable range: -180 -> 180");
      }
      // map to projection
      lattitude = (lattitude + proj.lattitudeLimit) / (proj.lattitudeLimit * 2);
      assert lattitude >= 0 && lattitude < 1.0;
      longitude = (longitude + 180) / 360;
      assert longitude >= 0 && longitude < 1.0;
      // put lat/long into a long, long in the high bits
      long components = (long) (longitude * MAX_COMPONENT);
      components <<= 32L;
      // and lat in the low bits
      components |= (long) (lattitude * MAX_COMPONENT);
      // combine the low and high words into a hash
      return interleave(components);
   }

   public static String stringHashFromCoords(GeoCoordinates coords) {
      return stringHashFromCoords(coords.longitude(), coords.lattitude());
   }

   public static String stringHashFromCoords(double longitude, double lattiude) {
      return encodeToString(encodeToLong(longitude, lattiude, ProjectionType.STANDARD));
   }
   
   public static long numericHashFromStringHash(String hash) {
      long result = decodeFromString(hash);
      // String geohashes use a normal projection. But Redis's numeric hash format uses a mercator
      // projection, clipping the lattitude range (due to the projection, areas near the poles are
      // not indexable).
      return reencode(ProjectionType.STANDARD, ProjectionType.MERCATOR, result);
   }
   
   private static long decodeFromString(String hash) {
      long result = 0;
      for (int i = 0; i < 11; i++) {
         result <<= 5L;
         if (i < hash.length()) {
            Integer val = ALPHABET_INVERSE.get(hash.charAt(i));
            if (val == null) {
               throw new IllegalArgumentException(
                     "Geohash contains invalid character: " + hash.charAt(i));
            }
            result |= val.intValue();
         }
      }
      // 11 characters == 55 bits, but Redis uses only 52 bits. So discard 3 least-significant bits.
      result >>= 3L;
      return result;
   }
   
   public static String stringHashFromNumericHash(long hash) {
      // Redis's numeric hash is encoded with different projection than string geohashes.
      hash = reencode(ProjectionType.MERCATOR, ProjectionType.STANDARD, hash);
      return encodeToString(hash);
   }
   
   private static String encodeToString(long hash) {
      if (hash < 0) {
         throw new IllegalArgumentException(
               "Geohash should be unsigned 52-bit value, not negative: " + hash);
      }
      if (hash > MAX) {
         throw new IllegalArgumentException(
               "Geohash should be unsigned 52-bit value not more than " + MAX + ": " + hash);
      }
      hash <<= 3; // pad 52-bit hash to multiple of 5 (since each character is 5 bits)
      StringBuilder sb = new StringBuilder(11);
      for (int offs = 50; offs >= 0; offs -= 5) {
         long v = (hash >> offs) & 31;
         char ch = ALPHABET[(int) v];
         sb.append(ch);
      }
      return sb.toString();
   }
   
   static long reencode(ProjectionType src, ProjectionType dest, long hash) {
      // split lat and long in the hash into low and high 32-bit words
      long components = deinterleave(hash);
      long latComponent = components & 0xffffffffL;
      // compute lattitude from source projection
      double latMin = (latComponent * 1.0 / MAX_COMPONENT) * (src.lattitudeLimit * 2)
            - src.lattitudeLimit;
      double latMax = ((latComponent + 1) * 1.0 / MAX_COMPONENT) * (src.lattitudeLimit * 2)
            - src.lattitudeLimit;
      // use middle of geohash tile as the coordinate to re-encode
      double lat = (latMin + latMax) / 2;
      if (lat >= dest.lattitudeLimit || lat < -dest.lattitudeLimit) {
         throw new IllegalArgumentException("given geohash cannot be correctly represented because"
               + " lattitude " + lat + " is not in the acceptable range: "
               + "-" + dest.lattitudeLimit + " -> " + dest.lattitudeLimit);
      }
      // and transform to dest projection
      lat = (lat + dest.lattitudeLimit) / (dest.lattitudeLimit * 2);
      assert lat >= 0 && lat < 1.0;
      // clear out old lat component
      components = components & 0xffffffff00000000L;
      // and get the new component in
      components |= (long) (lat * MAX_COMPONENT);
      // recombine the low and high words into a hash
      return interleave(components);
   }
   
   private static final long[] INTERLEAVE_B = {
         0x5555555555555555L, 0x3333333333333333L, 0x0f0f0f0f0f0f0f0fL,
         0x00ff00ff00ff00ffL, 0x0000ffff0000ffffL };
   private static final long[] INTERLEAVE_S = { 1, 2, 4, 8, 16 };
   
   static long interleave(long l) {
      long x = l & 0xffffffffL;
      long y = l >>> 32L;

      x = (x | (x << INTERLEAVE_S[4])) & INTERLEAVE_B[4];
      y = (y | (y << INTERLEAVE_S[4])) & INTERLEAVE_B[4];
      
      x = (x | (x << INTERLEAVE_S[3])) & INTERLEAVE_B[3];
      y = (y | (y << INTERLEAVE_S[3])) & INTERLEAVE_B[3];
      
      x = (x | (x << INTERLEAVE_S[2])) & INTERLEAVE_B[2];
      y = (y | (y << INTERLEAVE_S[2])) & INTERLEAVE_B[2];
      
      x = (x | (x << INTERLEAVE_S[1])) & INTERLEAVE_B[1];
      y = (y | (y << INTERLEAVE_S[1])) & INTERLEAVE_B[1];
      
      x = (x | (x << INTERLEAVE_S[0])) & INTERLEAVE_B[0];
      y = (y | (y << INTERLEAVE_S[0])) & INTERLEAVE_B[0];

      return x | (y << 1);
   }

   private static final long[] DEINTERLEAVE_B = {
         0x5555555555555555L, 0x3333333333333333L, 0x0f0f0f0f0f0f0f0fL,
         0x00ff00ff00ff00ffL, 0x0000ffff0000ffffL, 0x00000000ffffffffL };
   private static final long[] DEINTERLEAVE_S = { 0, 1, 2, 4, 8, 16 };
   
   static long deinterleave(long l) {
      long x = l;
      long y = l >>> 1;

      x = (x | (x >>> DEINTERLEAVE_S[0])) & DEINTERLEAVE_B[0];
      y = (y | (y >>> DEINTERLEAVE_S[0])) & DEINTERLEAVE_B[0];

      x = (x | (x >>> DEINTERLEAVE_S[1])) & DEINTERLEAVE_B[1];
      y = (y | (y >>> DEINTERLEAVE_S[1])) & DEINTERLEAVE_B[1];

      x = (x | (x >>> DEINTERLEAVE_S[2])) & DEINTERLEAVE_B[2];
      y = (y | (y >>> DEINTERLEAVE_S[2])) & DEINTERLEAVE_B[2];

      x = (x | (x >>> DEINTERLEAVE_S[3])) & DEINTERLEAVE_B[3];
      y = (y | (y >>> DEINTERLEAVE_S[3])) & DEINTERLEAVE_B[3];

      x = (x | (x >>> DEINTERLEAVE_S[4])) & DEINTERLEAVE_B[4];
      y = (y | (y >>> DEINTERLEAVE_S[4])) & DEINTERLEAVE_B[4];

      x = (x | (x >>> DEINTERLEAVE_S[5])) & DEINTERLEAVE_B[5];
      y = (y | (y >>> DEINTERLEAVE_S[5])) & DEINTERLEAVE_B[5];

      return x | (y << 32);
   }
}
