package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;

import io.netty.buffer.ByteBuf;

public class BytesListener extends BaseReplyListener<byte[]> {
   public BytesListener(Callback<byte[]> callback) {
      super(callback);
   }

   @Override
   public void onBulkReply(ByteBuf reply) {
      byte[] bytes = new byte[reply.readableBytes()];
      reply.readBytes(bytes);
      callbackSuccess(bytes);
   }
}
