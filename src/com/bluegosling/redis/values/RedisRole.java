package com.bluegosling.redis.values;

@FunctionalInterface
public interface RedisRole {
   String roleName();
   
   enum KnownTypes implements RedisRole {
      MASTER, SLAVE, SENTINEL;

      @Override
      public String roleName() {
         return name().toLowerCase();
      }
   }
   
   static RedisRole fromRoleName(String roleName) {
      try {
         return KnownTypes.valueOf(roleName.toUpperCase());
      } catch (IllegalArgumentException e) {
         // TODO: hashCode, equals, etc
         return () -> roleName;
      }
   }
}
