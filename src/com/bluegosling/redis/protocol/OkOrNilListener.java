package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;

import io.netty.buffer.ByteBuf;

public class OkOrNilListener extends BaseReplyListener<Boolean> {
   public OkOrNilListener(Callback<Boolean> callback) {
      super(callback);
   }

   @Override
   public void onSimpleReply(String string) {
      if ("ok".equalsIgnoreCase(string)) {
         callbackSuccess(true);
      } else {
         super.onSimpleReply(string);
      }
   }

   @Override
   public void onBulkReply(ByteBuf bytes) {
      if (bytes == null) {
         callbackSuccess(false);
      } else {
         super.onBulkReply(bytes);
      }
   }
}
