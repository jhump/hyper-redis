package com.bluegosling.redis.channel;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.protocol.ReplyListener;
import com.bluegosling.redis.protocol.RequestProtocol;
import com.bluegosling.redis.protocol.RequestStreamer;
import com.bluegosling.redis.protocol.ResponseProtocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;

public class SimpleRedisChannel implements RedisChannel {
   private final Channel channel;
   private final AtomicInteger pendingRequests = new AtomicInteger();
   private final BooleanSupplier shouldFlush = () -> {
      return pendingRequests.decrementAndGet() == 0;
   };
   
   public SimpleRedisChannel(Channel channel) {
      this.channel = channel;
      RedisChannelHandler handler = channel.pipeline().get(RedisChannelHandler.class);
      if (handler == null) {
         channel.pipeline().addLast(new RedisChannelHandler());
      }
   }

   @Override
   public RequestStreamer newCommand(ReplyListener callback) {
      return new RequestProtocol(channel.alloc(), new Callback<ByteBuf>() {
         @Override
         public void onSuccess(ByteBuf t) {
            synchronized (SimpleRedisChannel.this) {
               pendingRequests.incrementAndGet();
               channel.write(new RedisMessage(t, new ResponseProtocol(callback), shouldFlush));
            }
         }

         @Override
         public void onFailure(Throwable th) {
            callback.onFailure(th);
         }
      });
   }
   
   @Override
   public <T> T exclusive(Callable<T> callable) throws Exception {
      synchronized (this) {
         return callable.call();
      }
   }
   
   @Override
   public ByteBufAllocator allocator() {
      return channel.alloc();
   }

   @Override
   public SubscriberChannel asSubscriberChannel(ReplyListener eventListener) {
      return new SubscriberChannelImpl(eventListener);
   }
   
   private class SubscriberChannelImpl implements SubscriberChannel {
      private final ReplyListener eventListener;
      boolean closed;
      
      SubscriberChannelImpl(ReplyListener eventListener) {
         this.eventListener = eventListener;
      }
      
      @Override
      public synchronized RequestStreamer newCommand() {
         if (closed) {
            throw new IllegalStateException("pub-sub mode has been closed");
         }
         return new RequestProtocol(channel.alloc(), new Callback<ByteBuf>() {
            @Override
            public void onSuccess(ByteBuf t) {
               synchronized (SubscriberChannelImpl.this) {
                  if (closed) {
                     throw new IllegalStateException("pub-sub mode has been closed");
                  }
                  channel.write(new PubSubMessage(t, eventListener))
                        .addListener(f -> {
                           if (!f.isSuccess()) {
                              eventListener.onFailure(f.cause());
                           }
                        });
               }
            }

            @Override
            public void onFailure(Throwable th) {
               eventListener.onFailure(th);
            }
         });
      }

      @Override
      public synchronized void close() {
         closed = true;
      }
   }
}
