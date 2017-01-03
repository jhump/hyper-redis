package com.bluegosling.redis;

import java.util.Arrays;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

public class RedisVersion implements Comparable<RedisVersion> {
   public static RedisVersion parse(String versionString) {
      int[] components = Arrays.stream(versionString.split("\\."))
            .mapToInt(Integer::valueOf)
            .toArray();
      Preconditions.checkArgument(components.length > 0,
            "Version string should have at least one numerical component: %s", versionString);
      return new RedisVersion(components);
   }
   
   private static String normalizedString(int[] versionComponents) {
      return Arrays.stream(versionComponents)
            .mapToObj(Integer::toString)
            .collect(Collectors.joining("."));
   }
   
   private final int[] components;
   private final String string;
   
   private RedisVersion(int[] components) {
      this.components = components;
      this.string = normalizedString(components);
   }
   
   public RedisVersion canonicalize() {
      if (components.length == 3) {
         return this;
      }
      if (components.length > 3) {
         return new RedisVersion(Arrays.copyOf(components, 3));
      }
      int[] newComponents = new int[3];
      System.arraycopy(components, 0, newComponents, 0, components.length);
      Arrays.fill(newComponents, components.length, newComponents.length, 0);
      return new RedisVersion(newComponents);
   }
   
   public int majorVersion() {
      return components[0]; 
   }

   public int minorVersion() {
      return components.length > 1 ? components[1] : 0; 
   }

   public int pointVersion() {
      return components.length > 2 ? components[2] : 0; 
   }

   @Override
   public boolean equals(Object o) {
      if (o instanceof RedisVersion) {
         RedisVersion other = (RedisVersion) o;
         return Arrays.equals(components, other.components);
      }
      return false;
   }
   
   @Override
   public int hashCode() {
      return string.hashCode();
   }
   
   @Override
   public String toString() {
      return string;
   }

   @Override
   public int compareTo(RedisVersion o) {
      int[] n1 = components;
      int[] n2 = o.components;
      for (int i = 0, len = Math.max(n1.length, n2.length); i < len; i++) {
         if (i >= n1.length) {
            return -1;
         }
         if (i >= n2.length) {
            return 1;
         }
         int c = Integer.compare(n1[i], n2[i]);
         if (c != 0) {
            return c;
         }
      }
      return 0;
   }
}
