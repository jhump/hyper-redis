package com.bluegosling.redis.values;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

@FunctionalInterface
public interface EncodingType {
   String typeName();
   
   default Set<EntryType> applicableEntryTypes() {
      return Collections.emptySet();
   }
   
   enum KnownTypes implements EncodingType {
      RAW(EntryType.KnownTypes.STRING),
      INT(EntryType.KnownTypes.STRING),
      ZIPLIST(EntryType.KnownTypes.LIST, EntryType.KnownTypes.HASH, EntryType.KnownTypes.ZSET),
      LINKEDLIST(EntryType.KnownTypes.LIST),
      INTSET(EntryType.KnownTypes.SET),
      HASHTABLE(EntryType.KnownTypes.SET, EntryType.KnownTypes.HASH),
      SKIPLIST(EntryType.KnownTypes.ZSET);
      
      private final Set<EntryType> entryTypes;

      KnownTypes(EntryType.KnownTypes... entryTypes) {
         EnumSet<EntryType.KnownTypes> set = EnumSet.noneOf(EntryType.KnownTypes.class);
         for (EntryType.KnownTypes type : entryTypes) {
            set.add(type);
         }
         this.entryTypes = Collections.unmodifiableSet(set);
      }
      
      @Override
      public String typeName() {
         return name().toLowerCase();
      }
      
      @Override
      public Set<EntryType> applicableEntryTypes() {
         return entryTypes;
      }
   }
   
   static EncodingType fromTypeName(String typeName) {
      try {
         return KnownTypes.valueOf(typeName.toUpperCase());
      } catch (IllegalArgumentException e) {
         // TODO: hashCode, equals, etc
         return () -> typeName;
      }
   }
}
