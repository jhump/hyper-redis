package com.bluegosling.redis.channel;

import static java.util.Objects.requireNonNull;

import com.bluegosling.redis.protocol.ReplyListener;

import io.netty.buffer.ByteBuf;

class PubSubMessage {
   static final PubSubMessage END_PUB_SUB = new PubSubMessage(false);
   
   final ByteBuf request;
   final ReplyListener eventListener;
   
   public PubSubMessage(ByteBuf request, ReplyListener eventListener) {
      this.request = requireNonNull(request);
      this.eventListener = requireNonNull(eventListener);
   }
   
   private PubSubMessage(boolean dummy) {
      this.request = null;
      this.eventListener = null;
   }
}
