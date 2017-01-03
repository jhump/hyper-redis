package com.bluegosling.redis.channel;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.protocol.ReplyListener;
import com.bluegosling.redis.protocol.RequestProtocol;
import com.bluegosling.redis.protocol.RequestStreamer;
import com.bluegosling.redis.protocol.ResponseProtocol;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;

public class PersistentRedisChannel implements RedisChannel, Lifecycle {
   private final Supplier<ChannelFuture> init;
   private final Supplier<Channel> create;
   private final Function<Channel, Future<?>> close;
   private final ChannelCustomizer customizer;
   
   private final Object lock = new Object();
   private Channel currentChannel;
   private AtomicInteger pendingRequests = new AtomicInteger();
   private final BooleanSupplier shouldFlush = () -> {
      return pendingRequests.decrementAndGet() == 0;
   };
   private ByteBufAllocator allocator;
   private ChannelFuture start;
   private Future<?> shutdown;
   
   public PersistentRedisChannel(Bootstrap channelBootstrap) {
      this(channelBootstrap, null);
   }

   public PersistentRedisChannel(Bootstrap channelBootstrap, ChannelCustomizer customizer) {
      init = () -> ensureHandler(channelBootstrap.connect());
      create = () -> ensureHandler(channelBootstrap.connect().channel());
      close = ch -> ch.close();
      this.customizer = customizer;
   }

   public PersistentRedisChannel(ChannelFactory<?> channelFactory) {
      this(channelFactory, null);
   }
   
   public PersistentRedisChannel(ChannelFactory<?> channelFactory, ChannelCustomizer customizer) {
      init = () -> {
         Channel ch = ensureHandler(channelFactory.newChannel());
         // must a return a future that lets caller know when connection is ready
         ChannelPromise connected = ch.newPromise();
         ch.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
               ctx.channel().pipeline().remove(this);
               connected.setSuccess();
               super.channelActive(ctx);
            }

            @Override
            public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
               if (ctx.channel().isActive()) {
                  channelActive(ctx);
               }
            }
         });
         return connected;
      };
      create = () -> ensureHandler(channelFactory.newChannel());
      close = ch -> ch.close();
      this.customizer = customizer;
   }
   
   PersistentRedisChannel(RedisChannel.Builder channelBuilder) {
      init = () -> channelBuilder.createBootstrap().connect();
      create = () -> channelBuilder.createBootstrap().connect().channel();
      close = ch -> {
         ChannelFuture closed = ch.close();
         if (channelBuilder.shouldShutdownWorkerGroup()) {
            closed.addListener(f -> channelBuilder.workerGroup().shutdownGracefully());
            return channelBuilder.workerGroup().terminationFuture();
         } else {
            return closed;
         }
      };
      customizer = channelBuilder.getCustomizer();
   }
   
   private Channel ensureHandler(Channel ch) {
      RedisChannelHandler handler =
            ch.pipeline().get(RedisChannelHandler.class);
      if (handler == null) {
         ch.pipeline().addLast(new RedisChannelHandler());
      }
      return ch;
   }
   
   private ChannelFuture ensureHandler(ChannelFuture f) {
      ensureHandler(f.channel());
      return f;
   }
   
   @Override
   public Future<?> start() {
      synchronized (lock) {
         checkState(shutdown == null, "Channel has already been shutdown");
         if (start == null) {
            start = init.get();
            currentChannel = start.channel();
            allocator = currentChannel.alloc();
         }
         return start;
      }
   }
   
   private Channel getChannel() {
      // TODO: pro-actively reconnect on failure with exponential backoff
      synchronized (lock) {
         checkState(start != null, "Channel not started");
         checkState(shutdown == null, "Channel has been shutdown");
         assert currentChannel != null;
         if (!currentChannel.isOpen()) {
            currentChannel = create.get();
            if (customizer != null) {
               // TODO: run this in separate event loop so blocking can't deadlock
               customizer.customize(this);
            }
         }
         return currentChannel;
      }
   }
   
   @Override
   public Future<?> shutdown() {
      synchronized (lock) {
         checkState(start != null, "Channel never started");
         if (shutdown == null) {
            assert currentChannel != null;
            close.apply(currentChannel);
            currentChannel = null;
         }
         return shutdown;
      }
   }

   @Override
   public RequestStreamer newCommand(ReplyListener callback) {
      return new RequestProtocol(allocator(), new Callback<ByteBuf>() {
         @Override
         public void onSuccess(ByteBuf t) {
            synchronized (lock) {
               pendingRequests.incrementAndGet();
               getChannel().write(new RedisMessage(t, new ResponseProtocol(callback), shouldFlush));
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
      synchronized (lock) {
         return callable.call();
      }
   }
   
   @Override
   public ByteBufAllocator allocator() {
      synchronized (lock) {
         checkState(allocator != null, "Channel not started");
         return allocator;
      }
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
      
      // TODO: handle connection errors by auto-closing the subscriber? or perhaps acquire
      // channel in ctor and pin in member variable, so that we need not worry about it
      
      @Override
      public RequestStreamer newCommand() {
         synchronized (lock) {
            if (closed) {
               throw new IllegalStateException("pub-sub mode has been closed");
            }
            return new RequestProtocol(allocator(), new Callback<ByteBuf>() {
               @Override
               public void onSuccess(ByteBuf t) {
                  synchronized (lock) {
                     if (closed) {
                        throw new IllegalStateException("pub-sub mode has been closed");
                     }
                     getChannel().write(new PubSubMessage(t, eventListener))
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
      }

      @Override
      public synchronized void close() {
         closed = true;
      }
   }
}
