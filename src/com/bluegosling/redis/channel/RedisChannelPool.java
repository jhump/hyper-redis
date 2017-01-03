package com.bluegosling.redis.channel;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BooleanSupplier;

import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.protocol.ReplyListener;
import com.bluegosling.redis.protocol.RequestProtocol;
import com.bluegosling.redis.protocol.RequestStreamer;
import com.bluegosling.redis.protocol.ResponseProtocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.SucceededFuture;

public class RedisChannelPool implements RedisChannel, Lifecycle {
   private static final ThreadLocal<SimpleRedisChannel> exclusiveChannel = new ThreadLocal<>();
   private static final AtomicReferenceFieldUpdater<RedisChannelPool, Pool> channelsUpdater =
         AtomicReferenceFieldUpdater.newUpdater(RedisChannelPool.class, Pool.class, "channels");
   
   private final Builder builder;
   private volatile Pool channels;
   private AtomicInteger pendingRequests = new AtomicInteger();
   private final BooleanSupplier shouldFlush = () -> {
      return pendingRequests.decrementAndGet() == 0;
   };
   
   RedisChannelPool(Builder builder) {
      this.builder = builder;
   }

   @Override
   public RequestStreamer newCommand(ReplyListener callback) {
      checkState(channels != null, "Channel pool never started");

      SimpleRedisChannel exclusive = exclusiveChannel.get();
      if (exclusive != null) {
         return exclusive.newCommand(callback);
      }

      return new RequestProtocol(allocator(), new Callback<ByteBuf>() {
         @Override
         public void onSuccess(ByteBuf t) {
            Future<Channel> ch = acquire();
            ch.addListener(f -> {
               if (f.isSuccess()) {
                  // write the message to the acquired channel
                  Channel channel = ch.getNow();
                  try {
                     pendingRequests.incrementAndGet();
                     channel.write(
                           new RedisMessage(t, new ResponseProtocol(callback), shouldFlush));
                  } finally {
                     // don't forget to release the channel when done
                     release(channel);
                  }
               } else {
                  callback.onFailure(new RedisChannelAcquisitionException(f.cause()));
               }
            });
         }

         @Override
         public void onFailure(Throwable th) {
            callback.onFailure(th);
         }
      });      
   }

   private Future<Channel> acquire() {
      return channels.acquire();
   }
   
   private Channel acquireNow() {
      Future<Channel> f = acquire();
      try {
         f.await();
      } catch (InterruptedException e) {
         f.cancel(false);
         throw new RedisChannelAcquisitionException(e);
      }
      if (!f.isSuccess()) {
         throw new RedisChannelAcquisitionException(f.cause());
      }
      return f.getNow();
   }
   
   private void release(Channel ch) {
      channels.release(ch);
   }

   @Override
   public <T> T exclusive(Callable<T> callable) throws Exception {
      checkState(channels != null, "Channel pool never started");

      SimpleRedisChannel exclusive = exclusiveChannel.get();
      if (exclusive != null) {
         // already have an exclusive channel
         return callable.call();
      }

      Channel ch = acquireNow();
      SimpleRedisChannel channel = new SimpleRedisChannel(ch);
      exclusiveChannel.set(channel);
      try {
         return callable.call();
      } finally {
         exclusiveChannel.remove();
         release(ch);
      }
   }
   
   @Override
   public ByteBufAllocator allocator() {
      checkState(channels != null, "Channel pool never started");
      return channels.allocator();
   }
   
   @Override
   public SubscriberChannel asSubscriberChannel(ReplyListener eventListener) {
      // TODO: provide dedicated channel
      return null;
   }
   
   @Override
   public Future<?> start() {
      if (channels == null) {
         Pool p = new Pool(builder);
         if (!channelsUpdater.compareAndSet(this, null, p)) {
            // lost race to initialize; just destroy the temp one we created
            p.close();
         }
      }
      return new SucceededFuture<Void>(ImmediateEventExecutor.INSTANCE, null);
   }
   
   public void warmUp() {
      Pool pool = channels;
      checkState(pool != null, "Channel pool never started");
      pool.warmUp().syncUninterruptibly();
   }

   @Override
   public Future<?> shutdown() {
      Pool pool = channels;
      checkState(pool != null, "Channel pool never started");
      Future<?> closed = pool.closeAsync();
      if (builder.channelBuilder.shouldShutdownWorkerGroup()) {
         closed.addListener(f -> builder.channelBuilder.workerGroup().shutdownGracefully());
         return builder.channelBuilder.workerGroup().terminationFuture();
      } else {
         return closed;
      }
   }
   
   public static final class Builder implements Cloneable {
      final RedisChannel.Builder channelBuilder;
      int maxChannels = 10;
      int minChannels = 1;
      int maxWaitingToAcquire = Integer.MAX_VALUE;
      long idleTimeout = 30;
      TimeUnit idleTimeoutUnit = TimeUnit.SECONDS;
      long healthCheckTimeout = 10;
      TimeUnit healthCheckTimeoutUnit = TimeUnit.SECONDS;
      long acquireTimeout = 3;
      TimeUnit acquireTimeoutUnit = TimeUnit.SECONDS;
      
      Builder(RedisChannel.Builder channelBuilder) {
         this.channelBuilder = channelBuilder;
      }
      
      // TODO: setters for all fields
      
      @Override
      protected Builder clone() {
         try {
            return (Builder) super.clone();
         } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
         }
      }
      
      public RedisChannelPool build() {
         return new RedisChannelPool(clone());
      }
   }
}
