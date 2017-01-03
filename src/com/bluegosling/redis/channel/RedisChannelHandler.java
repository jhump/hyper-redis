package com.bluegosling.redis.channel;

import java.util.LinkedList;
import java.util.Queue;

import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.protocol.ResponseListener;
import com.bluegosling.redis.protocol.ResponseProtocol;
import com.bluegosling.redis.values.Response;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;

class RedisChannelHandler extends ChannelDuplexHandler {
   private final Queue<RedisMessage> bufferedRequests = new LinkedList<>();
   private final Queue<ResponseProtocol> pending = new LinkedList<>();
   private ByteBuf currentReply;
   
   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      // flush the buffer of pending requests
      if (!bufferedRequests.isEmpty()) {
         do {
            ctx.write(bufferedRequests.remove());
         } while (!bufferedRequests.isEmpty());
      }
   }
   
   @Override
   public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
         throws Exception {
      // TODO: PubSubMessage, Monitor mode
      RedisMessage message = (RedisMessage) msg;
      if (!ctx.channel().isActive()) {
         // not yet connected, defer until active
         bufferedRequests.add(message);
         return;
      }
      pending.add(message.responseHandler);
      promise.addListener(f -> {
         if (!f.isSuccess()) {
            pending.remove(message.responseHandler);
            message.responseHandler.onFailure(new RedisChannelException(f.cause()));
         }
      });
      super.write(ctx, message.request, promise);
      if (message.shouldFlush.getAsBoolean()) {
         ctx.flush();
      }
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (currentReply == null) {
         currentReply = (ByteBuf) msg;
      } else {
         currentReply = ByteToMessageDecoder.COMPOSITE_CUMULATOR
               .cumulate(ctx.alloc(), currentReply, (ByteBuf) msg);
      }
      
      while (true) {
         ResponseProtocol resp = pending.peek();
         
         if (resp == null) {
            // use a dummy listener to consume errant response
            resp = new ResponseProtocol(new ResponseListener(new Callback<Response>() {
               @Override
               public void onSuccess(Response t) {
                  // TODO: Something went wrong. Log an error.
               }
               
               @Override
               public void onFailure(Throwable th) {
                  // TODO: Something went wrong. Log an error.
               }
            }));
            pending.offer(resp);
         }
         
         if (!resp.parseReply(currentReply)) {
            break;
         }
         if (!currentReply.isReadable()) {
            currentReply.release();
            currentReply = null;
            break;
         }
         currentReply.discardReadBytes();
         ResponseProtocol removed = pending.remove();
         assert removed == resp;
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      ctx.channel().close();
      while (!pending.isEmpty()) {
         ResponseProtocol response = pending.poll();
         response.onFailure(cause);
      }
      while (!bufferedRequests.isEmpty()) {
         RedisMessage message = bufferedRequests.poll();
         message.responseHandler.onFailure(new RedisChannelException(cause));
      }
      super.exceptionCaught(ctx, cause);
   }
}
