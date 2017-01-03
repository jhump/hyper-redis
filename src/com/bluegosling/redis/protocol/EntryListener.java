package com.bluegosling.redis.protocol;

import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.function.Function;

import com.bluegosling.redis.concurrent.Callback;

import io.netty.buffer.ByteBuf;

public class EntryListener<K, V> extends BaseReplyListener<Entry<K, V>> {
   private final Function<Callback<K>, ReplyListener> keyListener;
   private final Function<Callback<V>, ReplyListener> valueListener;

   public EntryListener(Callback<Entry<K, V>> callback,
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
      if (numberOfElements != 2) {
         callbackFailure(new RedisResponseException(
               "Expecting an array of exactly two elements, got " + numberOfElements));
         return NO_OP;
      }
      return new ArrayElementListener<>(getCallback(), keyListener, valueListener);
   }
   
   public static class ArrayElementListener<K, V> extends BaseReplyListener<Entry<K, V>> {
      private final ReplyListener keyListener;
      private final ReplyListener valueListener;
      private K key;
      private boolean wantValue;
      
      public ArrayElementListener(Callback<Entry<K, V>> callback,
            Function<Callback<K>, ReplyListener> keyListener,
            Function<Callback<V>, ReplyListener> valueListener) {
         super(callback);
         this.keyListener = keyListener.apply(new Callback<K>() {
            @Override
            public void onSuccess(K k) {
               key = k;
               wantValue = true;
            }

            @Override
            public void onFailure(Throwable th) {
               callbackFailure(th);
            }
         });
         this.valueListener = valueListener.apply(new Callback<V>() {
            @Override
            public void onSuccess(V v) {
               callbackSuccess(new AbstractMap.SimpleImmutableEntry<>(key, v));
               wantValue = false;
            }

            @Override
            public void onFailure(Throwable th) {
               callbackFailure(th);
            }
         });
      }
      
      private ReplyListener current() {
         return wantValue ? valueListener : keyListener;
      }

      @Override
      public void onSimpleReply(String string) {
         current().onSimpleReply(string);
      }

      @Override
      public void onBulkReply(ByteBuf bytes) {
         current().onBulkReply(bytes);
      }

      @Override
      public void onErrorReply(String errorMsg) {
         current().onErrorReply(errorMsg);
      }

      @Override
      public void onIntegerReply(long value) {
         current().onIntegerReply(value);
      }

      @Override
      public ReplyListener onArrayReply(int numberOfElements) {
         return current().onArrayReply(numberOfElements);
      }
   }
}
