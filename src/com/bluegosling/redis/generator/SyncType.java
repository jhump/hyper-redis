package com.bluegosling.redis.generator;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import com.bluegosling.redis.transaction.Promise;

/**
 * The synchronization types of interfaces and classes generated.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
enum SyncType {
   /**
    * All commands are blocking/synchronous.
    */
   BLOCKING("blocking", false, RedisKind.TRANSACTING),
   /**
    * All commands are asynchronous and completion is communicated via callbacks.
    */
   CALLBACKS("async", false, RedisKind.TRANSACTING),
   /**
    * All commands are asynchronous and return {@link Future}s.
    */
   FUTURES("future", false, RedisKind.TRANSACTING),
   /**
    * All commands are queued and return {@link Promise}s. This is interface is for transactions
    * and only the main {@code Redis} block gets interfaces of this type.
    */
   PROMISES("transaction", true, RedisKind.TRANSACTING);
   
   private final String subPackage;
   private final Set<RedisKind> applicableKinds;
   
   SyncType(String subPackage, boolean includeKinds, RedisKind... kinds) {
      this.subPackage = subPackage;
      EnumSet<RedisKind> applicable;
      if (includeKinds) {
         applicable = EnumSet.noneOf(RedisKind.class);
         for (RedisKind k : kinds) {
            applicable.add(k);
         }
      } else {
         applicable = EnumSet.allOf(RedisKind.class);
         for (RedisKind k : kinds) {
            applicable.remove(k);
         }
      }
      this.applicableKinds = Collections.unmodifiableSet(applicable);
   }
   
   public String subPackage() {
      return subPackage;
   }
   
   public Set<RedisKind> applicableKinds() {
      return applicableKinds;
   }
}