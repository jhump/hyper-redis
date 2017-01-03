package com.bluegosling.redis.protocol;

import com.bluegosling.redis.Marshaller;
import com.bluegosling.redis.concurrent.Callback;

import io.netty.buffer.ByteBuf;

public class MarshallingListener<T> extends BaseReplyListener<T> {
   private final Marshaller<? extends T> marshaller;

   public MarshallingListener(Callback<T> callback, Marshaller<? extends T> marshaller) {
      super(callback);
      this.marshaller = marshaller;
   }

   @Override
   public void onBulkReply(ByteBuf bytes) {
      T value;
      try {
         value = marshaller.fromBytes(bytes);
      } catch (Throwable th) {
         callbackFailure(th);
         return;
      }
      callbackSuccess(value);
   }
}
