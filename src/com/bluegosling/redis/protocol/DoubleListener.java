package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;
import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;

public class DoubleListener extends BaseReplyListener<Double> {

   public DoubleListener(Callback<Double> callback) {
      super(callback);
   }

   @Override
   public void onBulkReply(ByteBuf bytes) {
      double d;
      try {
         d = Double.parseDouble(bytes.toString(Charsets.UTF_8));
      } catch (Throwable th) {
         callbackFailure(th);
         return;
      }
      callbackSuccess(d);
   }
}
