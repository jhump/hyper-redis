package com.bluegosling.redis.channel;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.protocol.ReplyListener;
import com.bluegosling.redis.protocol.RequestStreamer;
import com.bluegosling.redis.protocol.VoidListener;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public interface RedisChannel {
   RequestStreamer newCommand(ReplyListener callback);

   SubscriberChannel asSubscriberChannel(ReplyListener eventCallback);

   default <T> T exclusive(Callable<T> callable) throws Exception {
      return callable.call();
   }
   
   default ByteBufAllocator allocator() {
      return UnpooledByteBufAllocator.DEFAULT;
   }

   interface SubscriberChannel extends AutoCloseable {
      RequestStreamer newCommand();
      @Override void close();
   }

   interface ChannelCustomizer {
      void customize(RedisChannel channel);
      
      static ChannelCustomizer select(int databaseIndex) {
         return channel -> {
            // issue a 'select' command for the given database
            SettableFuture<Void> done = SettableFuture.create();
            channel.newCommand(new VoidListener(new Callback<Void>() {
               @Override
               public void onSuccess(Void t) {
                  done.set(null);
               }

               @Override
               public void onFailure(Throwable th) {
                  done.setException(th);
               }
            })).nextToken("SELECT").nextToken(Integer.toString(databaseIndex)).finish();
            // wait for select command to complete
            try {
               done.get();
            } catch (ExecutionException | InterruptedException e) {
               Throwable t = e instanceof ExecutionException ? e.getCause() : e;
               if (t instanceof RuntimeException) {
                  throw (RuntimeException) t;
               } else if (t instanceof Error) {
                  throw (Error) t;
               } else {
                  throw new RuntimeException(t);
               }
            }
         };
      }
   }
   
   final class Builder implements Cloneable {
      private static int defaultNumberOfWorkerThreads() {
         return Math.min(2, Runtime.getRuntime().availableProcessors());
      }
      
      private final SocketAddress remoteAddress;
      private int numIoWorkerThreads = defaultNumberOfWorkerThreads();
      private EventLoopGroup eventLoopGroup;
      private EventLoopGroup workerGroup;
      private ChannelCustomizer customizer;
      
      public Builder(SocketAddress address) {
         this.remoteAddress = address;
      }

      public Builder(String hostname) {
         this(hostname, 6379);
      }

      public Builder(String hostname, int port) {
         this(new InetSocketAddress(hostname, port));
      }

      @Override
      protected Builder clone() {
         try {
            return (Builder) super.clone();
         } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
         }
      }
      
      public Builder numIoWorkerThreads(int numThreads) {
         checkArgument(numThreads > 0);
         this.numIoWorkerThreads = numThreads;
         this.eventLoopGroup = null;
         return this;
      }
      
      public Builder eventLoopGroup(EventLoopGroup group) {
         this.eventLoopGroup = requireNonNull(group);
         return this;
      }
      
      public Builder customizer(ChannelCustomizer customizer) {
         this.customizer = customizer;
         return this;
      }
      
      public RedisChannelPool.Builder forPool() {
         return new RedisChannelPool.Builder(clone());
      }

      public PersistentRedisChannel build() {
         // TODO: validate
         return new PersistentRedisChannel(clone());
      }
      
      synchronized EventLoopGroup workerGroup() {
         if (workerGroup == null) {
            workerGroup = eventLoopGroup == null
                  ? new NioEventLoopGroup(numIoWorkerThreads)
                  : eventLoopGroup;
         }
         return workerGroup;
      }
      
      boolean shouldShutdownWorkerGroup() {
         return eventLoopGroup == null;
      }
      
      Bootstrap createBootstrap() {
         return new Bootstrap()
               .group(workerGroup())
               .option(ChannelOption.TCP_NODELAY, true)
               .handler(new RedisChannelHandler())
               .channel(NioSocketChannel.class)
               .remoteAddress(remoteAddress);
      }
      
      ChannelCustomizer getCustomizer() {
         return customizer;
      }
   }
}
