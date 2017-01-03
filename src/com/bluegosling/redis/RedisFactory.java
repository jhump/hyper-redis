package com.bluegosling.redis;

import java.nio.charset.Charset;

import com.bluegosling.redis.channel.RedisChannel;
import com.google.common.base.Charsets;

public final class RedisFactory<K, V> {
   
   public static RedisFactory<String, String> stringRedis(RedisChannel channel) {
      return stringRedis(channel, Charsets.UTF_8);
   }

   public static RedisFactory<String, String> stringRedis(RedisChannel channel, Charset charset) {
      Marshaller<String> m = Marshaller.ofStrings(charset);
      return typedRedis(channel, m, m);
   }

   public static RedisFactory<byte[], byte[]> binaryRedis(RedisChannel channel) {
      Marshaller<byte[]> m = Marshaller.ofBytes();
      return typedRedis(channel, m, m);
   }
   
   public static <K, V> RedisFactory<K, V> typedRedis(RedisChannel channel, Marshaller<K> keys,
         Marshaller<V> values) {
      return new RedisFactory<>(channel, keys, values);
   }
   
   private final RedisChannel channel;
   private final Marshaller<K> keyMarshaller;
   private final Marshaller<V> valMarshaller;
   
   private RedisFactory(RedisChannel channel, Marshaller<K> keyMarshaller,
         Marshaller<V> valMarshaller) {
      this.channel = channel;
      this.keyMarshaller = keyMarshaller;
      this.valMarshaller = valMarshaller;
   }
   
   public Redis<K, V> newRedis() {
      return new RedisImpl<>(channel, keyMarshaller, valMarshaller);
   }
   
//   public <R extends Redis<K, V>> R newRedis(RedisFactoryVersion<K, V, R> version) {
//      RedisVersion minVersion = version.minimumVersion();
//      
//      // check version
//      RedisInfo info = newRedis().withBlocking().adminOperations().info();
//      RedisVersion actualVersion = info.serverSection().redisVersion().canonicalize();
//      if (actualVersion.compareTo(minVersion) < 0) {
//         throw new RedisException(String.format(
//               "Redis server is version %s but should be at least %s", actualVersion, minVersion));
//      }
//      return version.newInstance(channel, keyMarshaller, valMarshaller);
//   }
   
//   public Transactor<K, V> newTransactor() {
//      return null;
//   }
   
//   public static void main(String[] args) {
//      Redis<String, String> r = stringRedis(null).newRedis_v2_0_0(Redis.class);
//      System.out.println(r.getClass());
//   }
}
