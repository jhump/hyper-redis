package com.bluegosling.redis.protocol;

import java.util.Arrays;
import java.util.List;

import com.bluegosling.redis.Marshaller;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.values.ScanCursor;
import com.bluegosling.redis.values.ScanResult;
import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;

public class ScanResultListener<T> extends BaseReplyListener<ScanResult<T>> {
   private final Marshaller<T> marshaller;
   
   public ScanResultListener(Callback<ScanResult<T>> callback, Marshaller<T> marshaller) {
      super(callback);
      this.marshaller = marshaller;
   }

   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      if (numberOfElements != 2) {
         callbackFailure(new RedisResponseException(
               "Expecting scan response to have elements (cursor and data), instead got "
                     + numberOfElements));
         return NO_OP;
      }
      return new ArrayElementListener<>(getCallback(), marshaller);
   }
   
   private static class ArrayElementListener<T> extends BaseReplyListener<ScanResult<T>> {
      private final Marshaller<T> marshaller;
      private boolean awaitingData;
      private String cursor;
      
      protected ArrayElementListener(Callback<ScanResult<T>> callback, Marshaller<T> marshaller) {
         super(callback);
         this.marshaller = marshaller;
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

      @Override
      public ReplyListener onArrayReply(int numberOfElements) {
         if (!awaitingData) {
            return super.onArrayReply(numberOfElements);
         }
         return new ArrayListener<Object>(
               new Callback<Object[]>() {
                  @SuppressWarnings("unchecked")
                  @Override
                  public void onSuccess(Object[] t) {
                     callbackSuccess(
                           new ScanResult<T>(ScanCursor.of(cursor), (List<T>) Arrays.asList(t)));
                  }

                  @Override
                  public void onFailure(Throwable th) {
                     callbackFailure(th);
                  }
               },
               Object[]::new,
               cb -> new MarshallingListener<Object>(cb, marshaller));
      }
   }
}
