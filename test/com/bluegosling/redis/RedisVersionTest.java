package com.bluegosling.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class RedisVersionTest {

   @Test public void properVersion() {
      RedisVersion v = RedisVersion.parse("3.2.0");
      assertEquals(3, v.majorVersion());
      assertEquals(2, v.minorVersion());
      assertEquals(0, v.pointVersion());
      assertEquals("3.2.0", v.toString());
      assertEquals(v, v.canonicalize());
      
      v = RedisVersion.parse("2.6.9");
      assertEquals(2, v.majorVersion());
      assertEquals(6, v.minorVersion());
      assertEquals(9, v.pointVersion());
      assertEquals("2.6.9", v.toString());
      assertEquals(v, v.canonicalize());
   }
   
   @Test public void nonCanonicalVersion() {
      RedisVersion v = RedisVersion.parse("3");
      assertEquals(3, v.majorVersion());
      assertEquals(0, v.minorVersion());
      assertEquals(0, v.pointVersion());
      assertEquals("3", v.toString());
      assertEquals("3.0.0", v.canonicalize().toString());

      v = RedisVersion.parse("3.2");
      assertEquals(3, v.majorVersion());
      assertEquals(2, v.minorVersion());
      assertEquals(0, v.pointVersion());
      assertEquals("3.2", v.toString());
      assertEquals("3.2.0", v.canonicalize().toString());
      
      v = RedisVersion.parse("2.6.9.1");
      assertEquals(2, v.majorVersion());
      assertEquals(6, v.minorVersion());
      assertEquals(9, v.pointVersion());
      assertEquals("2.6.9.1", v.toString());
      assertEquals("2.6.9", v.canonicalize().toString());

      v = RedisVersion.parse("2.6.9.1.9");
      assertEquals(2, v.majorVersion());
      assertEquals(6, v.minorVersion());
      assertEquals(9, v.pointVersion());
      assertEquals("2.6.9.1.9", v.toString());
      assertEquals("2.6.9", v.canonicalize().toString());
   }
   
   @Test public void compare() {
      // equal versions
      assertEquals(0, RedisVersion.parse("3").compareTo(RedisVersion.parse("3")));
      assertEquals(0, RedisVersion.parse("3.2").compareTo(RedisVersion.parse("3.2")));
      assertEquals(0, RedisVersion.parse("3.2.0").compareTo(RedisVersion.parse("3.2.0")));
      assertEquals(0, RedisVersion.parse("3.2.0.1").compareTo(RedisVersion.parse("3.2.0.1")));
      // missing components is less than with components
      assertTrue(RedisVersion.parse("3.0").compareTo(RedisVersion.parse("3")) > 0);
      assertTrue(RedisVersion.parse("3").compareTo(RedisVersion.parse("3.0")) < 0);
      assertTrue(RedisVersion.parse("3.2.0").compareTo(RedisVersion.parse("3.2")) > 0);
      assertTrue(RedisVersion.parse("3.2").compareTo(RedisVersion.parse("3.2.0")) < 0);
      assertTrue(RedisVersion.parse("3.2.0.1").compareTo(RedisVersion.parse("3.2.0")) > 0);
      assertTrue(RedisVersion.parse("3.2.0").compareTo(RedisVersion.parse("3.2.0.1")) < 0);
      // major version differs
      assertTrue(RedisVersion.parse("3.2").compareTo(RedisVersion.parse("2.6")) > 0);
      assertTrue(RedisVersion.parse("2.6").compareTo(RedisVersion.parse("3.2")) < 0);
      assertTrue(RedisVersion.parse("3").compareTo(RedisVersion.parse("2.6")) > 0);
      assertTrue(RedisVersion.parse("2.6").compareTo(RedisVersion.parse("3")) < 0);
      assertTrue(RedisVersion.parse("3.2").compareTo(RedisVersion.parse("2")) > 0);
      assertTrue(RedisVersion.parse("2").compareTo(RedisVersion.parse("3.2")) < 0);
      // minor version differs
      assertTrue(RedisVersion.parse("2.6.1").compareTo(RedisVersion.parse("2.2.8")) > 0);
      assertTrue(RedisVersion.parse("2.2.8").compareTo(RedisVersion.parse("2.6.1")) < 0);
      assertTrue(RedisVersion.parse("2.6").compareTo(RedisVersion.parse("2.2.8")) > 0);
      assertTrue(RedisVersion.parse("2.2.8").compareTo(RedisVersion.parse("2.6")) < 0);
      assertTrue(RedisVersion.parse("2.6.1").compareTo(RedisVersion.parse("2.2")) > 0);
      assertTrue(RedisVersion.parse("2.2").compareTo(RedisVersion.parse("2.6.1")) < 0);
      // point version differs
      assertTrue(RedisVersion.parse("2.6.9").compareTo(RedisVersion.parse("2.6.1")) > 0);
      assertTrue(RedisVersion.parse("2.6.1").compareTo(RedisVersion.parse("2.6.9")) < 0);
   }
}
