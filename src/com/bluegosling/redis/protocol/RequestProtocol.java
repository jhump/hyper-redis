package com.bluegosling.redis.protocol;

import java.nio.CharBuffer;

import com.bluegosling.redis.concurrent.Callback;
import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

/**
 * Implements the Redis request protocol by accumulating tokens and encoding them in a
 * {@link ByteBuf}.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class RequestProtocol implements RequestStreamer {
   private static final ByteBuf ASTERISK =
         Unpooled.unreleasableBuffer(ByteBufUtil.encodeString(
               UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap("*"), Charsets.US_ASCII))
         .asReadOnly();
   private static final ByteBuf DOLLAR =
         Unpooled.unreleasableBuffer(ByteBufUtil.encodeString(
               UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap("$"), Charsets.US_ASCII))
         .asReadOnly();
   private static final ByteBuf CRLF =
         Unpooled.unreleasableBuffer(ByteBufUtil.encodeString(
               UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap("\r\n"), Charsets.US_ASCII))
         .asReadOnly();
   
   private final Callback<ByteBuf> callback;
   private final ByteBufAllocator allocator;
   private int numTokens;
   private CompositeByteBuf buffer;
   
   /**
    * Constructs a new request protocol that sends the encoded request to the given callback. The
    * given allocator is used to instantiate buffers.
    * 
    * <p>The callback must consume the buffer in a fashion that will eventually
    * {@linkplain ByteBuf#release() release} it to avoid resource leaks. Writing the buffer to a
    * channel will consume and release the buffer correctly.
    * 
    * @param allocator a buffer allocator
    * @param callback a callback that will receive the encoded request
    */
   public RequestProtocol(ByteBufAllocator allocator, Callback<ByteBuf> callback) {
      this.allocator = allocator;
      buffer = allocator.compositeBuffer();
      this.callback = callback;
   }

   @Override
   public RequestStreamer nextToken(String token) {
      nextToken(ByteBufUtil.encodeString(allocator, CharBuffer.wrap(token), Charsets.UTF_8));
      return this;
   }

   @Override
   public RequestStreamer nextToken(ByteBuf token) {
      String len = Integer.toString(token.readableBytes());
      buffer.addComponent(true, DOLLAR)
            .addComponent(true, ByteBufUtil.encodeString(allocator, CharBuffer.wrap(len),
                  Charsets.US_ASCII))
            .addComponent(true, CRLF)
            .addComponent(true, token)
            .addComponent(true, CRLF);
      numTokens++;
      return this;
   }

   @Override
   public void finish() {
      // add prefix that indicates the number of components
      String num = Integer.toString(numTokens);
      CompositeByteBuf complete = allocator.compositeBuffer();
      complete.addComponent(true, ASTERISK)
            .addComponent(true, ByteBufUtil.encodeString(allocator, CharBuffer.wrap(num),
                  Charsets.US_ASCII))
            .addComponent(true, CRLF)
            .addComponent(true, buffer);
      callback.onSuccess(complete);
      buffer = null;
   }

   @Override
   public void error(Throwable th) {
      callback.onFailure(th);
      buffer.release();
      buffer = null;
   }
}
