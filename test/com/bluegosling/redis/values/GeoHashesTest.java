package com.bluegosling.redis.values;

import static com.bluegosling.redis.MoreAsserts.assertThrows;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class GeoHashesTest {
   
   @Test public void conversions() {
      // Numeric to string conversions (and vice versa) incur a re-encoding from mercator projection
      // (limited to -85.05112878 to 85.05112878 lattitude) to standard projection (-90 to 90).
      
      assertEquals("sqdtr74hyu0", GeoHashes.stringHashFromNumericHash(3479447370796909L));
      assertEquals(3479447370796909L, GeoHashes.numericHashFromStringHash("sqdtr74hyu0"));

      // 86 lattitude out of range for numeric hashes
      String outOfRange = GeoHashes.stringHashFromCoords(56.0, 86.0);
      assertThrows(IllegalArgumentException.class,
            () -> GeoHashes.numericHashFromStringHash(outOfRange));
   }
}
