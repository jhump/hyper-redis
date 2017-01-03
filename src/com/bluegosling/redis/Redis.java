package com.bluegosling.redis;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.bluegosling.redis.channel.RedisChannel;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.protocol.ArrayOfLongListener;
import com.bluegosling.redis.protocol.RequestStreamer;
import com.bluegosling.redis.values.BitFieldType;
import com.bluegosling.redis.values.GeoElement;
import com.bluegosling.redis.values.OverflowType;
import com.bluegosling.redis.values.Response;

public interface Redis<K, V> {
   // placeholder
   
   String MIN_VERSION = "1.0.0";
   
   static <K, V> Redis<K, V> newInstance(RedisChannel channel, Marshaller<K> keyMarshaller,
         Marshaller<V> valueMarshaller) {
      return new RedisImpl<>(channel, keyMarshaller, valueMarshaller);
   }
   
   // DatabaseMetadata<K> databaseMetadata();
//   FuturesRedis<K, V> withFutures();
//   BlockingRedis<K, V> withBlocking();
   // void validate_v2_2_0(Callback<Redis22<K, V>> callback);
   // RedisTransactor<K, V> transactor();
   // SubscriptionController subscriptionController();

//   interface RedisTransactor<K, V> {
//      <X extends Throwable> void runTransaction(TransactionBlock<K, V, X> block) throws X;
//      <T, X extends Throwable> T callInTransaction(TransactionCallable<K, V, T, X> block) throws X;
//      <X extends Throwable >void runTransaction(WatchingTransactionBlock<K, V, X> block) throws X;
//      <T, X extends Throwable> T callInTransaction(WatchingTransactionCallable<K, V, T, X> block)
//            throws X;
//   }
//   interface TransactionBlock<K, V, X extends Throwable> {
//      void run(TransactingRedis<K, V> redis) throws X;
//   }
//   interface TransactionCallable<K, V, T, X extends Throwable> {
//      T call(TransactingRedis<K, V> redis) throws X;
//   }
//   interface WatchingTransactionBlock<K, V, X extends Throwable> {
//      void run(TransactingRedis<K, V> redis, WatchingRedis<K, V> watcher) throws X;
//   }
//   interface WatchingTransactionCallable<K, V, T, X extends Throwable> {
//      T call(TransactingRedis<K, V> redis, WatchingRedis<K, V> watcher) throws X;
//   }
//   interface TransactingRedis<K, V> {
//   }
//   interface WatchingRedis<K, V> {
//   }
   
   void append(K key, V value, Callback.OfLong callback);

   void eval(String script, Collection<K> keys, Collection<String> args, Callback<Response> callback);

   void geoAdd(K key, Collection<GeoElement<V>> elements, Callback.OfInt callback);

   BitFieldOperation bitField(K key);

   final class BitFieldOperation {
      private final RedisChannel channel;
      private final List<Object> tokens = new ArrayList<>();
      
      BitFieldOperation(RedisChannel channel, Object... tokens) {
         this.channel = channel;
         for (Object t : tokens) {
            this.tokens.add(requireNonNull(t));
         }
      }
      
      // :get type=bitfieldtype offset=long
      public BitFieldOperation get(BitFieldType type, long offset) {
         tokens.add("GET");
         tokens.add(requireNonNull(type));
         tokens.add(Long.toString(offset));
         return this;
      }

      // :set type=BitFieldType offset=long value=long
      public BitFieldOperation set(BitFieldType type, long offset, long value) {
         tokens.add("SET");
         tokens.add(requireNonNull(type));
         tokens.add(Long.toString(offset));
         tokens.add(Long.toString(value));
         return this;
      }

      // :incrementBy=incrby type=BitFieldType offset=long inc=long
      public BitFieldOperation incrementBy(BitFieldType type, long offset, long inc) {
         tokens.add("INCRBY");
         tokens.add(requireNonNull(type));
         tokens.add(Long.toString(offset));
         tokens.add(Long.toString(inc));
         return this;
      }
      
      // :overflow OverflowType
      public BitFieldOperation overflow(OverflowType overflowType) {
         tokens.add("OVERFLOW");
         tokens.add(requireNonNull(overflowType));
         return this;
      }

      public void execute(Callback<long[]> callback) {
         RequestStreamer cmd = channel.newCommand(new ArrayOfLongListener(callback));
         for (Object t : tokens) {
            if (t instanceof Marshallable) {
               cmd.nextToken(((Marshallable) t).marshall(channel.allocator()));
            } else {
               cmd.nextToken(String.valueOf(t));
            }
         }
         cmd.finish();
      }
   }
}
