package com.bluegosling.redis.transaction;

import static java.util.Objects.requireNonNull;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.bluegosling.redis.channel.RedisChannel;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.protocol.BaseReplyListener;
import com.bluegosling.redis.protocol.RedisResponseException;
import com.bluegosling.redis.protocol.ReplyListener;
import com.bluegosling.redis.protocol.RequestStreamer;
import com.bluegosling.redis.protocol.VoidListener;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.buffer.ByteBuf;

/**
 * Controls the lifecycle of a Redis transaction (e.g. {@code MULTI}-{@code EXEC} block). This
 * controller buffers all operations locally, only sending them when the transaction is ready to be
 * executed. This can narrow the duration in which a channel must be held exclusively (to prevent
 * interleaving of other operations between the {@code MULTI} and {@code EXEC} calls). This also
 * avoids load on the Redis server for discarded transactions as they are simply never sent to the
 * server (and thus an actual {@code DISCARD} command is unnecessary).
 * 
 * <p>The transaction controller can also interact with a {@link Watcher}, to let it know when a
 * transaction is completed or discarded so it can keep its state of watched keys properly sync'ed
 * with the actual state in the Redis server.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public final class TransactionController {
   private static final ThreadLocal<TransactionController> IN_TRANSACTION = new ThreadLocal<>();
   
   /**
    * Returns true if the given watcher is associated with an in-progress transaction on the current
    * thread.
    *
    * @param watcher a watcher
    * @return true if the watcher is presently associated with an in-progress transaction
    * 
    * @see #runInTransaction(Supplier)
    */
   static boolean isWatcherCurrentlyValid(Watcher<?> watcher) {
      TransactionController c = IN_TRANSACTION.get();
      return c != null && c.watcher == watcher;
   }
   
   private final RedisChannel channel;
   private final Watcher<?> watcher;
   private final LinkedList<Operation> queue = new LinkedList<>();
   
   /**
    * Creates a new controller.
    * 
    * @param channel a channel for communicating with a Redis server
    * @param watcher an optional watcher that will be updated when the transaction is executed or
    *       discarded
    */
   public TransactionController(RedisChannel channel, Watcher<?> watcher) {
      this.channel = requireNonNull(channel);
      this.watcher = watcher;
   }
   
   /**
    * Runs the given block in a transaction and returns its result. This simply manages thread-local
    * state to indicate that a transaction is in-progress for the current controller.
    * 
    * @param block the block to run
    * @return the result produced by the block
    */
   public <T> T runInTransaction(Supplier<T> block) {
      if (IN_TRANSACTION.get() != null) {
         throw new IllegalStateException(
               "Cannot start a new transaction when one is already in progress");
      }
      
      boolean done = false;
      IN_TRANSACTION.set(this);
      try {
         T result = block.get();
         done = true;
         execute();
         return result;
      } catch (Throwable th) {
         if (!done) {
            discard();
         }
         throw Throwables.propagate(th);
      } finally {
         IN_TRANSACTION.remove();
      }
   }

   /**
    * Checks that a transaction is in progress for this controller.
    * 
    * @see #runInTransaction(Supplier)
    */
   private void checkTransaction() {
      if (IN_TRANSACTION.get() != this) {
         throw new IllegalStateException(
               "This transaction is not in progress on the current thread");
      }
   }
   
   /**
    * Adds an operation to the transaction.
    * 
    * @param requestWriter an action that will write the request tokens for the operation
    * @param listener a function that creates the listener that is invoked with the response of the
    *       operation, which typically constructs a result and passes it to the given callback
    * @return a promise that is fulfilled when the transaction completes or is broken if an error
    *       occurs
    */
   <T> Promise<T> enqueue(Consumer<RequestStreamer> requestWriter,
         Function<Callback<T>, ReplyListener> listener) {
      checkTransaction();
      Promise<T> promise = new Promise<>();
      queue.add(new Operation(requestWriter, listener.apply(promise.callback())));
      return promise;
   }

   /**
    * Executes the transaction. All enqueued operations are sent to the server in between
    * {@code MULTI} and {@code EXEC} commands. As the response to the {@code EXEC} command is
    * parsed, promises made for operations in the transaction are fulfilled with the operation
    * results. 
    */
   void execute() {
      checkTransaction();
      if (queue.isEmpty()) {
         // nothing to execute so just bail
         unwatch();
         return;
      }
      try {
         channel.exclusive(() -> {
            Throwable[] failure = new Throwable[1];
            VoidListener listener = new VoidListener(new Callback.OfVoid() {
               @Override
               public void onSuccess() {
               }
            
               @Override
               public void onFailure(Throwable th) {
                  failure[0] = th;
               }
            });
            channel.newCommand(listener).nextToken("MULTI").finish();
            for (Operation o : queue) {
               RequestStreamer req = channel.newCommand(listener);
               o.requestWriter.accept(req);
            }
            CountDownLatch latch = new CountDownLatch(1);
            QueueCompleter completer = new QueueCompleter(queue, new Callback<Void>() {
               @Override
               public void onSuccess(Void t) {
                  latch.countDown();
               }
            
               @Override
               public void onFailure(Throwable th) {
                  failure[0] = th;
                  latch.countDown();
               }
            });
            channel.newCommand(completer).nextToken("EXEC").finish();
            Uninterruptibles.awaitUninterruptibly(latch);
            if (!queue.isEmpty()) {
               Throwable th = failure[0] != null
                     ? failure[0]
                     : new RedisResponseException(
                              "Received fewer responses to EXEC than queued operations!");
               for (Operation o : queue) {
                  o.responseListener.onFailure(th);
               }
               queue.clear();
            }
            if (watcher != null) {
               watcher.clear();
            }
            if (failure[0] instanceof TransactionAbortedException) {
               throw (TransactionAbortedException) failure[0];
            } else if (failure[0] != null) {
               throw new TransactionFailedException(failure[0]);
            }
            return null;
         });
      } catch (Exception e) {
         if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
         } else {
            throw new RuntimeException(e);
         }
      }
   }

   /**
    * Discards the transaction. Any queued operations are abandoned and promises made are broken
    * with a {@link TransactionDiscardedException}.
    */
   void discard() {
      checkTransaction();
      if (!queue.isEmpty()) {
         Throwable th = new TransactionDiscardedException();
         for (Operation o : queue) {
            o.responseListener.onFailure(th);
         }
         queue.clear();
      }
      unwatch();
   }
   
   void unwatch() {
      if (watcher != null) {
         watcher.unwatchBlocking();
      }
   }
   
   /**
    * Represents a single operation in a transaction.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    */
   private static class Operation {
      final Consumer<RequestStreamer> requestWriter;
      final ReplyListener responseListener;
      
      Operation(Consumer<RequestStreamer> requestWriter, ReplyListener responseListener) {
         this.requestWriter = requestWriter;
         this.responseListener = responseListener;
      }
   }
   
   /**
    * A reply listener that handles the array reply of an {@code EXEC} command. As elements are
    * parsed, they are used to fulfill promises made for the corresponding operation.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    */
   private static class QueueCompleter extends BaseReplyListener<Void> {
      private final Queue<Operation> queue = new LinkedList<>();
      
      /**
       * Constructs a listener that will parse replies for operations in the given queue.
       * 
       * @param queue the queue of operations
       * @param callback a callback that is invoked when parsing the reply either fails or
       *       fully completes
       */
      QueueCompleter(Queue<Operation> queue, Callback<Void> callback) {
         super(callback);
      }
      
      @Override
      public void onBulkReply(ByteBuf bytes) {
         if (bytes == null) {
            // nil reply means EXEC aborted due to watched keys being modified
            callbackFailure(new TransactionAbortedException());
         } else {
            super.onBulkReply(bytes);
         }
      }
      
      @Override
      public ReplyListener onArrayReply(int numberOfElements) {
         if (numberOfElements < 0) {
            // nil reply means EXEC aborted due to watched keys being modified
            callbackFailure(new TransactionAbortedException());
            return NO_OP;
         } else if (numberOfElements == 0) {
            callbackSuccess(null);
            return NO_OP;
         }
         return new ArrayElementListener(
               numberOfElements,
               () -> {
                  Operation o = queue.poll();
                  return o != null ? o.responseListener : NO_OP;
               },
               getCallback());
      }
      
      /**
       * Handles an element in the response array.
       * 
       * @author Joshua Humphries (jhumphries131@gmail.com)
       */
      private static class ArrayElementListener extends BaseReplyListener<Void> {
         private final Supplier<ReplyListener> delegate;
         private int outstanding;
         
         ArrayElementListener(int count, Supplier<ReplyListener> delegate,
               Callback<Void> callback) {
            super(callback);
            outstanding = count;
            this.delegate = delegate;
         }

         private void countDown() {
            if (--outstanding == 0) {
               callbackSuccess(null);
            }
         }

         @Override
         public void onSimpleReply(String reply) {
            delegate.get().onSimpleReply(reply);
            countDown();
         }

         @Override
         public void onErrorReply(String errorMsg) {
            delegate.get().onErrorReply(errorMsg);
            countDown();
         }

         @Override
         public void onIntegerReply(long value) {
            delegate.get().onIntegerReply(value);
            countDown();
         }

         @Override
         public void onBulkReply(ByteBuf bytes) {
            delegate.get().onBulkReply(bytes);
            countDown();
         }

         @Override
         public ReplyListener onArrayReply(int numberOfElements) {
            if (numberOfElements <= 0) {
               countDown();
               return NO_OP;
            }
            // we have to wrap the real listener in order to learn when
            // the array response is complete
            ReplyListener l = delegate.get().onArrayReply(numberOfElements); 
            return new ArrayElementListener(
                  numberOfElements,
                  () -> l,
                  new Callback<Void>() {
                     @Override
                     public void onSuccess(Void v) {
                        countDown();
                     }

                     @Override
                     public void onFailure(Throwable th) {
                        callbackFailure(th);
                     }
                  });
         }
      }
   }
}
