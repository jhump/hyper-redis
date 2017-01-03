package com.bluegosling.redis.transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.bluegosling.redis.Marshaller;
import com.bluegosling.redis.channel.RedisChannel;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.concurrent.CompletableFuture;
import com.bluegosling.redis.concurrent.CompletionStageFuture;
import com.bluegosling.redis.protocol.RequestStreamer;
import com.bluegosling.redis.protocol.VoidListener;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.buffer.ByteBufAllocator;

/**
 * Manages watched keys, used to implement optimistic concurrency in a Redis transaction. This class
 * provides watch and unwatch operations for all three types of completion signaling: callbacks,
 * futures, and blocking.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <K> the type of keys
 */
public final class Watcher<K> {
   private final RedisChannel channel;
   private final Marshaller<K> keyMarshaller;
   private final Set<K> watchedKeys = new HashSet<>();
   
   /**
    * Constructs a new watcher. The given channel is used to issue {@code WATCH} commands. The given
    * marshaller is used to encode keys.
    * 
    * @param channel a channel to a Redis server
    * @param keyMarshaller marshals key objects to bytes
    */
   public Watcher(RedisChannel channel, Marshaller<K> keyMarshaller) {
      this.channel = channel;
      this.keyMarshaller = keyMarshaller;
   }
   
   private void checkState() {
      if (!TransactionController.isWatcherCurrentlyValid(this)) {
         throw new IllegalStateException(
               "The transaction associated with this watcher is no longer in progress");
      }
   }
   
   /**
    * Watches the given keys. If they are already being watched, redundant {@code WATCH} commands
    * are elided.
    * 
    * @param callback a callback invoked when the watch operation is acknowledged by the server
    * @param keys the keys to watch
    */
   @SafeVarargs
   public final void watch(Callback.OfVoid callback, K... keys) {
      watch(callback, Arrays.asList(keys));
   }
   
   /**
    * Watches the given keys. If they are already being watched, redundant {@code WATCH} commands
    * are elided.
    * 
    * @param callback a callback invoked when the watch operation is acknowledged by the server
    * @param keys the keys to watch
    */
   public void watch(Callback.OfVoid callback, Collection<? extends K> keys) {
      checkState();
      boolean needToWatch = false;
      for (K k : keys) {
         if (!watchedKeys.contains(k)) {
            needToWatch = true;
            break;
         }
      }
      if (!needToWatch) {
         callback.onSuccess();
         return;
      }
      RequestStreamer req = channel.newCommand(new VoidListener(callback));
      req.nextToken("WATCH");
      ByteBufAllocator alloc = channel.allocator();
      for (K k : keys) {
         if (!watchedKeys.contains(k)) {
            req.nextToken(keyMarshaller.toBytes(k, alloc));
         }
      }
      req.finish();
   }
   
   /**
    * Clears the list of watched keys. This does not issue any command to the server. It is used by
    * a {@link TransactionController} to clear the set after a transaction completes (since keys
    * are automatically unwatched upon completion of an {@code EXEC} command).
    */
   void clear() {
      watchedKeys.clear();
   }
   
   /**
    * Unwatches all watched keys. This issues an {@code UNWATCH} command to the server.
    * 
    * @param callback a callback invoked when the unwatch operation is acknowledged by the server
    */
   public void unwatch(Callback.OfVoid callback) {
      checkState();
      if (watchedKeys.isEmpty()) {
         callback.onSuccess();
         return;
      }
      RequestStreamer req = channel.newCommand(new VoidListener(callback));
      req.nextToken("UNWATCH").finish();
      watchedKeys.clear();
   }
   
   /**
    * Watches the given keys, returning a future that completes when the operation is acknowledged
    * by the server.
    * 
    * @param keys the keys to watch
    * @return a future that completes when the operation is acknowledged by the server
    */
   @SafeVarargs
   public final CompletionStageFuture<Void> watchFuture(K... keys) {
      return watchFuture(Arrays.asList(keys));
   }
   
   /**
    * Watches the given keys, returning a future that completes when the operation is acknowledged
    * by the server.
    * 
    * @param keys the keys to watch
    * @return a future that completes when the operation is acknowledged by the server
    */
   public final CompletionStageFuture<Void> watchFuture(Collection<? extends K> keys) {
      CompletableFuture<Void> f = new CompletableFuture<>();
      watch(new Callback.OfVoid() {
         @Override
         public void onSuccess() {
            f.complete(null);
         }

         @Override
         public void onFailure(Throwable th) {
            f.completeExceptionally(th);
            
         }
      }, keys);
      return f;
   }

   /**
    * Unwatches all watched keys, returning a future that completes when the operation is
    * acknowledged by the server.
    * 
    * @return a future that completes when the operation is acknowledged by the server
    */
   public CompletionStageFuture<Void> unwatchFuture() {
      CompletableFuture<Void> f = new CompletableFuture<>();
      unwatch(new Callback.OfVoid() {
         @Override
         public void onSuccess() {
            f.complete(null);
         }

         @Override
         public void onFailure(Throwable th) {
            f.completeExceptionally(th);
            
         }
      });
      return f;
   }

   /**
    * Watches the given keys, blocking until the operation is acknowledged by the server.
    * 
    * @param keys the keys to watch
    */
   @SafeVarargs
   public final void watchBlocking(K... keys) {
      watchBlocking(Arrays.asList(keys));
   }

   /**
    * Watches the given keys, blocking until the operation is acknowledged by the server.
    * 
    * @param keys the keys to watch
    */
   public final void watchBlocking(Collection<? extends K> keys) {
      try {
         Uninterruptibles.getUninterruptibly(watchFuture(keys));
      } catch (ExecutionException e) {
         throw Throwables.propagate(e.getCause());
      }
   }
   
   /**
    * Unwatches all watched keys, blocking until the operation is acknowledged by the server.
    */
   public void unwatchBlocking() {
      try {
         Uninterruptibles.getUninterruptibly(unwatchFuture());
      } catch (ExecutionException e) {
         throw Throwables.propagate(e.getCause());
      }
   }
}
