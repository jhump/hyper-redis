package com.bluegosling.redis.protocol;

import java.util.Arrays;
import java.util.Map.Entry;

import com.bluegosling.redis.Marshaller;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.values.ScanCursor;
import com.bluegosling.redis.values.ScanResult;
import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;

public class EntryScanResultListener<K, V> extends BaseReplyListener<ScanResult<Entry<K, V>>> {
   private final Marshaller<K> keyMarshaller;
   private final Marshaller<V> valueMarshaller;
   
   public EntryScanResultListener(Callback<ScanResult<Entry<K, V>>> callback,
         Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
      super(callback);
      this.keyMarshaller = keyMarshaller;
      this.valueMarshaller = valueMarshaller;
   }

   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      if (numberOfElements != 2) {
         callbackFailure(new RedisResponseException(
               "Expecting scan response to have elements (cursor and data), instead got "
                     + numberOfElements));
         return NO_OP;
      }
      return new ArrayElementListener<>(getCallback(), keyMarshaller, valueMarshaller);
   }
   
   private static class ArrayElementListener<K, V>
   extends BaseReplyListener<ScanResult<Entry<K, V>>> {
      private final Marshaller<K> keyMarshaller;
      private final Marshaller<V> valueMarshaller;
      private boolean awaitingData;
      private String cursor;
      
      protected ArrayElementListener(Callback<ScanResult<Entry<K, V>>> callback,
            Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
         super(callback);
         this.keyMarshaller = keyMarshaller;
         this.valueMarshaller = valueMarshaller;
      }

      @Override
      public void onBulkReply(ByteBuf bytes) {
         if (awaitingData) {
            super.onBulkReply(bytes);
         } else {
            cursor = bytes.toString(Charsets.UTF_8);
            awaitingData = false;
         }
      }

      @SuppressWarnings("unchecked")
      @Override
      public ReplyListener onArrayReply(int numberOfElements) {
         if (!awaitingData) {
            return super.onArrayReply(numberOfElements);
         }
         return new ArrayListener<Entry<K, V>>(
               new Callback<Entry<K, V>[]>() {
                  @Override
                  public void onSuccess(Entry<K, V>[] t) {
                     callbackSuccess(
                           new ScanResult<Entry<K, V>>(ScanCursor.of(cursor), Arrays.asList(t)));
                  }

                  @Override
                  public void onFailure(Throwable th) {
                     callbackFailure(th);
                  }
               },
               len -> (Entry<K, V>[]) new Entry<?, ?>[len],
               cb -> new EntryListener.ArrayElementListener<>(cb,
                     kcb -> new MarshallingListener<K>(kcb, keyMarshaller),
                     vcb -> new MarshallingListener<V>(vcb, valueMarshaller)));
      }
   }
}
