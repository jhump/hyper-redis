package com.bluegosling.redis;

import com.bluegosling.redis.channel.RedisChannel;

public abstract class RedisFactoryVersion<K, V, R extends Redis<K, V>> {
   abstract RedisVersion minimumVersion();
   abstract R newInstance(RedisChannel channel, Marshaller<K> keyMarshaller,
         Marshaller<V> valueMarshaller);
   
   RedisFactoryVersion() {
      // TODO: this class and RedisFactory both need to depend on real generated code (instead of
      // placeholders), which would allow the code below to reference a class token, not a string
      if (!this.getClass().getEnclosingClass().getName()
            .equals("com.bluegosling.redis.RedisVersions")) {
         throw new AssertionError();
      }
   }
   
   // Example generated static method in RedisVersions:

//   public static <K, V> RedisFactoryVersion<K, V, Redis10<K, V>> v1_0() {
//      return new RedisFactoryVersion<K, V, Redis10<K,V>>() {
//         @Override
//         RedisVersion minimumVersion() {
//            return Redis10.MIN_VERSION;
//         }
//
//         @Override
//         Redis10<K, V> newInstance(RedisChannel channel, Marshaller<K> keyMarshaller,
//               Marshaller<V> valueMarshaller) {
//            return Redis10.newInstance(channel, keyMarshaller, valueMarshaller);
//         }
//      };
//   }
}
