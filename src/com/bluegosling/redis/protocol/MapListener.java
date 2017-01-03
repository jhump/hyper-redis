package com.bluegosling.redis.protocol;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import com.bluegosling.redis.concurrent.Callback;

public class MapListener<K, V> extends BaseReplyListener<Map<K, V>> {
   private final Function<Callback<K>, ReplyListener> keyListener;
   private final Function<Callback<V>, ReplyListener> valueListener;
   private Map<K, V> map;
   private int expectedSize;
   
   public MapListener(Callback<Map<K, V>> callback,
         Function<Callback<K>, ReplyListener> keyListener,
         Function<Callback<V>, ReplyListener> valueListener) {
      super(callback);
      this.keyListener = keyListener;
      this.valueListener = valueListener;
   }
   
   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      if (numberOfElements < 0) {
         callbackSuccess(null);
         return NO_OP;
      }
      if (numberOfElements == 0) {
         callbackSuccess(Collections.emptyMap());
         return NO_OP;
      }
      if ((numberOfElements & 1) != 0) {
         callbackFailure(new RedisResponseException(
               "Array must have even number of elements for key,value pairs; instead got "
                     + numberOfElements));
         return NO_OP;
      }
      expectedSize = numberOfElements >> 1;
      map = new LinkedHashMap<>();
      // TODO: could also support non-flattened map representation by just returning
      // an EntryListener here...
      return new EntryListener.ArrayElementListener<K, V>(new Callback<Entry<K, V>>() {
         @Override
         public void onSuccess(Entry<K, V> e) {
            map.put(e.getKey(), e.getValue());
            if (map.size() == expectedSize) {
               callbackSuccess(map);
            }
         }

         @Override
         public void onFailure(Throwable th) {
            callbackFailure(th);
         }
      }, keyListener, valueListener);
   }
}
