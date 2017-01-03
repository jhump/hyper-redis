package com.bluegosling.redis;

import java.util.Collection;

import com.bluegosling.redis.channel.RedisChannel;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.protocol.IntListener;
import com.bluegosling.redis.protocol.LongListener;
import com.bluegosling.redis.protocol.RequestStreamer;
import com.bluegosling.redis.protocol.ResponseListener;
import com.bluegosling.redis.values.GeoElement;
import com.bluegosling.redis.values.Response;

class RedisImpl<K, V> implements Redis<K, V> {
   // placeholder
   
   private final RedisChannel channel;
   private final Marshaller<K> keys;
   private final Marshaller<V> values;

   RedisImpl(RedisChannel channel, Marshaller<K> keys, Marshaller<V> values) {
      this.channel = channel;
      this.keys = keys;
      this.values = values;
   }
   
   // append key value=str -> long @2.0.0
   @Override
   public void append(K key, V value, Callback.OfLong callback) {
      RequestStreamer cmd = channel.newCommand(new LongListener(callback));
      cmd.nextToken("APPEND");
      cmd.nextToken(keys.toBytes(key, channel.allocator()));
      cmd.nextToken(values.toBytes(value, channel.allocator()));
      cmd.finish();
   }
   
   //eval script=String K[]::{ numKeys=int:`keys.length` K[]:`keys` } args=String[] -> Response @2.6.0
   @Override
   public void eval(String script, Collection<K> keys, Collection<String> args, Callback<Response> callback) {
      RequestStreamer cmd = channel.newCommand(new ResponseListener(callback));
      cmd.nextToken("EVAL");
      cmd.nextToken(script);
      cmd.nextToken(Integer.toString(keys.size()));
      for (K k : keys) {
         cmd.nextToken(this.keys.toBytes(k, channel.allocator()));
      }
      for (String s : args) {
         cmd.nextToken(s);
      }
      cmd.finish();
   }

   //geoAdd K elements=GeoElement<V>[]::{ longitude=double:`e.getLongitude()` lattitude=double:`e.getLattitude()` V:`e.getValue()` }... -> int @3.2.0
   @Override
   public void geoAdd(K key, Collection<GeoElement<V>> elements, Callback.OfInt callback) {
      RequestStreamer cmd = channel.newCommand(new IntListener(callback));
      cmd.nextToken("GEOADD");
      for (GeoElement<V> e : elements) {
         cmd.nextToken(Double.toString(e.longitude()));
         cmd.nextToken(Double.toString(e.lattitude()));
         cmd.nextToken(this.values.toBytes(e.get(), channel.allocator()));
      }
      cmd.finish();      
   }
   
   // bitField key {:get type=bitfieldtype offset=long, :set type=BitFieldType offset=long value=long, :incrementBy=incrby type=BitFieldType offset=long inc=long, :overflow OverflowType}... -> Array<long> @3.2.0
   @Override
   public BitFieldOperation bitField(K key) {
      return new BitFieldOperation(channel,
            "BITFIELD",
            new Marshalled<K>(key, keys));
   }
}
