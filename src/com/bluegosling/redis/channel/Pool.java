package com.bluegosling.redis.channel;

import static com.google.common.base.Preconditions.checkState;

import java.nio.CharBuffer;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.bluegosling.redis.channel.RedisChannel.ChannelCustomizer;
import com.bluegosling.redis.channel.RedisChannelPool.Builder;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.protocol.RedisResponseException;
import com.bluegosling.redis.protocol.ResponseProtocol;
import com.bluegosling.redis.protocol.SimpleStringListener;
import com.google.common.base.Charsets;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

class Pool implements ChannelPool {
   private static final ByteBuf PING_COMMAND =
         Unpooled.unreleasableBuffer(ByteBufUtil.encodeString(
               UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap("*1\r\n$4\r\nPING\r\n"),
               Charsets.US_ASCII))
         .asReadOnly();

   private final Builder builder;
   private final Bootstrap bootstrap;
   private final EventLoop executor;
   private final Promise<Void> closeFuture;
   private final Deque<Channel> channelQueue = new LinkedList<>();
   private final Queue<Promise<Channel>> waiting = new LinkedList<>();
   private final Map<Channel, ChannelInfo> channels = new HashMap<>();
   private boolean closed;
   private volatile ByteBufAllocator allocator;
   
   Pool(Builder builder) {
      this.builder = builder;
      this.bootstrap = builder.channelBuilder.createBootstrap();
      this.executor = this.bootstrap.config().group().next();
      this.closeFuture = executor.newPromise();
   }
   
   public ByteBufAllocator allocator() {
      if (allocator == null) {
         // if no channel yet created, need to let one be created and ask it for its allocator
         executor.submit(() -> {
            if (allocator == null) {
               enqueue(create());
            }
         }).syncUninterruptibly();
      }
      return allocator;
   }
   
   public Future<?> warmUp() {
      return executor.submit(() -> {
         while (channels.size() < builder.minChannels) {
            enqueue(create());
         }
      });
   }

   @Override
   public Future<Channel> acquire() {
      return acquire(executor.newPromise());
   }

   @Override
   public Future<Channel> acquire(Promise<Channel> promise) {
      executor.execute(() -> doAcquire(promise));
      executor.schedule(
            () -> promise.tryFailure(new RedisChannelAcquisitionException(
                  "Failed to acquire channel after " + builder.acquireTimeout + " "
                        + builder.acquireTimeoutUnit)),
            builder.acquireTimeout, builder.acquireTimeoutUnit);
      return promise;
   }
   
   private boolean doAcquire(Promise<Channel> promise) {
      while (true) {
         checkState(!closed, "Channel pool has been closed");
         if (channels.size() < builder.minChannels
               || (channelQueue.isEmpty() && channels.size() < builder.maxChannels)) {
            return satisfy(promise, create());
         }
         
         if (channelQueue.isEmpty()) {
            if (waiting.size() > builder.maxWaitingToAcquire) {
               promise.tryFailure(new RedisChannelAcquisitionException(
                     "Failed to acquire: too many acquisitions already waiting ("
                           + waiting.size() + " > " + builder.maxWaitingToAcquire + ")"));
               return false;
            }
            waiting.add(promise);
            promise.addListener(f -> {
               if (!f.isSuccess()) {
                  waiting.remove(promise);
               }
            });
            return false;
         }
         
         Channel ch = channelQueue.pop();
         ChannelInfo info = channels.get(ch);
         info.cancelPendingTasks();
         if (!ch.isActive()) {
            destroy(ch);
            continue;
         }
         return satisfy(promise, ch);
      }
   }

   @Override
   public Future<Void> release(Channel channel) {
      return release(channel, executor.newPromise());
   }

   @Override
   public Future<Void> release(Channel channel, Promise<Void> promise) {
      executor.execute(() -> doRelease(channel, promise));
      return promise;
   }
   
   private void doRelease(Channel channel, Promise<Void> promise) {
      if (closed) {
         destroy(channel);
         return;
      }
      
      if (!channel.isActive()) {
         destroy(channel);
      } else {
         enqueue(channel);
      }
      trySatisfyWaiter();
   }
   
   private Channel create() {
      Channel ch = bootstrap.connect().channel();
      if (allocator == null) {
         allocator = ch.alloc();
      }
      channels.put(ch, new ChannelInfo(ch));
      ch.closeFuture().addListener(f -> executor.execute(() -> {
         if (destroy(ch)) {
            channelQueue.remove(ch);
         }
      }));
      ChannelCustomizer customizer = builder.channelBuilder.getCustomizer();
      if (customizer != null) {
         // TODO: run this in separate event loop so blocking can't deadlock
         customizer.customize(new SimpleRedisChannel(ch));
      }
      return ch;
   }
   
   private boolean destroy(Channel ch) {
      if (channels.containsKey(ch)) {
         channels.remove(ch).cancelPendingTasks();
         ch.close();
         return true;
      }
      return false;
   }
   
   private void trySatisfyWaiter() {
      while (!waiting.isEmpty()) {
         Promise<Channel> waiter = waiting.poll();
         if (!waiter.isDone() && doAcquire(waiter)) {
            return;
         }
      }
   }
   
   private boolean satisfy(Promise<Channel> promise, Channel ch) {
      if (!promise.trySuccess(ch)) {
         // immediately return the channel if promise was already cancelled
         enqueue(ch);
         return false;
      }
      return true;
   }
   
   private void enqueue(Channel ch) {
      channelQueue.push(ch);
      channels.get(ch).scheduleTasks();
   }

   @Override
   public void close() {
      closeAsync().syncUninterruptibly();
   }
   
   public Future<?> closeAsync() {
      executor.execute(() -> {
         if (closed) {
            return;
         }
         
         closed = true;
         // have to cancel pending acquisitions
         for (Promise<Channel> promise : waiting) {
            promise.cancel(false);
         }
         boolean deferClose = !channels.isEmpty();
         if (deferClose) {
            // book-keeping to complete the close future when the pool is terminated
            // (e.g. after all connections are closed)
            GenericFutureListener<Future<Void>> terminationListener =
                  new GenericFutureListener<Future<Void>>() {
                     int remaining = channels.size();
                     
                     @Override
                     public void operationComplete(Future<Void> future) throws Exception {
                        executor.execute(() -> {
                           if (--remaining == 0) {
                              closeFuture.trySuccess(null);
                           }
                        });
                     }
                  };
            for (Channel ch : channels.keySet()) {
               ch.closeFuture().addListener(terminationListener);
            }
         }
         
         // go ahead and tear down unused channels
         while (!channelQueue.isEmpty()) {
            Channel ch = channelQueue.pop();
            destroy(ch);
         }
         
         if (!deferClose) {
            closeFuture.trySuccess(null);
         }
      });
      return closeFuture;
   }
   
   public Future<?> closeFuture() {
      return closeFuture;
   }
   
   private void healthCheck(Channel ch, Promise<Void> promise) {
      Callback<String> callback = Callback.of(
            s -> {
               if ("PONG".equalsIgnoreCase(s)) {
                  promise.trySuccess(null);
               } else {
                  promise.tryFailure(
                        new RedisResponseException("PING did not return PONG but instead " + s));
               }
            },
            th -> promise.tryFailure(th));
      ch.write(new RedisMessage(PING_COMMAND,
            new ResponseProtocol(new SimpleStringListener(callback))));
   }
   
   private class ChannelInfo {
      final Channel ch;
      Future<?> healthCheckTask;
      Future<?> healthCheckResult;
      Future<?> idleTimeoutTask;
      
      ChannelInfo(Channel ch) {
         this.ch = ch;
      }
      
      public void cancelPendingTasks() {
         if (healthCheckTask != null) {
            healthCheckTask.cancel(false);
            healthCheckTask = null;
         }
         if (healthCheckResult != null) {
            healthCheckResult.cancel(false);
            healthCheckResult = null;
         }
         if (idleTimeoutTask != null) {
            idleTimeoutTask.cancel(false);
            idleTimeoutTask = null;
         }
      }
      
      public void scheduleTasks() {
         // schedule health check for the channel if so configured
         if (builder.healthCheckTimeout > 0) {
            scheduleNextHealthCheck();
         }

         // maybe allow the channel to timeout if idle
         if (builder.idleTimeout > 0 && channels.size() > builder.minChannels) {
            idleTimeoutTask = executor.schedule(() -> {
               boolean removed = channelQueue.remove(ch);
               assert removed;
               destroy(ch);
            }, builder.idleTimeout, builder.idleTimeoutUnit);
         }
      }
      
      private void scheduleNextHealthCheck() {
         healthCheckTask = executor.schedule(() -> {
            Promise<Void> p = executor.newPromise();
            p.addListener(f -> {
               // if cancelled, we don't care about the result
               if (f.isCancelled()) {
                  return;
               }
               
               // if we do care and the result is bad, tear down this channel
               if (!f.isSuccess()) {
                  boolean removed = channelQueue.remove(ch);
                  assert removed;
                  destroy(ch);
                  return;
               }
               
               // if all is well, schedule a subsequent check
               scheduleNextHealthCheck();
            });
            
            healthCheck(ch, p);
            healthCheckResult = p;
            
         }, builder.healthCheckTimeout, builder.healthCheckTimeoutUnit);
      }
   }
}