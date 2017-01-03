package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;
import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;

public class BulkStringListener extends BaseReplyListener<String> {
   public BulkStringListener(Callback<String> callback) {
      super(callback);
   }

   @Override
   public void onBulkReply(ByteBuf reply) {
      callbackSuccess(reply == null ? null : reply.toString(Charsets.UTF_8));
   }
}
