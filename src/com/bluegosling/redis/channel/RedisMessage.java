package com.bluegosling.redis.channel;

import static java.util.Objects.requireNonNull;

import java.util.function.BooleanSupplier;

import com.bluegosling.redis.protocol.ResponseProtocol;

import io.netty.buffer.ByteBuf;

class RedisMessage {
   final ByteBuf request;
   final ResponseProtocol responseHandler;
   final BooleanSupplier shouldFlush;

   RedisMessage(ByteBuf request, ResponseProtocol responseHandler) {
      this(request, responseHandler, () -> true);
   }

   RedisMessage(ByteBuf request, ResponseProtocol responseHandler, BooleanSupplier shouldFlush) {
      this.request = requireNonNull(request);
      this.responseHandler = requireNonNull(responseHandler);
      this.shouldFlush = requireNonNull(shouldFlush);
   }
}
