package com.bluegosling.redis.values;

@FunctionalInterface
public interface EntryType {
   String typeName();
   
   enum KnownTypes implements EntryType {
      STRING, LIST, SET, ZSET, HASH;

      @Override
      public String typeName() {
         return name().toLowerCase();
      }
   }
   
   static EntryType fromTypeName(String typeName) {
      try {
         return KnownTypes.valueOf(typeName.toUpperCase());
      } catch (IllegalArgumentException e) {
         // TODO: hashCode, equals, etc
         return () -> typeName;
      }
   }
}
