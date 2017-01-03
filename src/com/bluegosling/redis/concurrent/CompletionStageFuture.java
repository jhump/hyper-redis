package com.bluegosling.redis.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link Future} that also provides the full API of {@link CompletionStage}. This basically
 * extracts the useful interface from {@link CompletableFuture}.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of future value
 */
public interface CompletionStageFuture<T> extends Future<T>, CompletionStage<T> {
   /**
    * {@inheritDoc}
    * 
    * <p>The default implementation returns a new {@link CompletableFuture}, even if this instance
    * is already backed by a {@link CompletableFuture}. This prevents callers from mutating the
    * source future, like by completing or obtruding results.
    */
   @Override
   default CompletableFuture<T> toCompletableFuture() {
      CompletableFuture<T> ret = new CompletableFuture<T>();
      this.whenComplete((val, failure) -> {
         if (failure != null) {
            ret.completeExceptionally(failure);
         } else {
            ret.complete(val);
         }
      });
      return ret;
   }
   
   /**
    * Returns a future that is already completed with the given value.
    * 
    * @param result the result value of the future
    * @return a future that is already completed with the given value
    */
   static <T> CompletionStageFuture<T> completedFuture(T result) {
      CompletableFutureCallback<T> cf = new CompletableFutureCallback<>();
      cf.complete(result);
      return cf;
   }

   /**
    * Returns a future that has already failed with the given cause.
    * 
    * @param cause the cause of failure
    * @return a future that has already failed with the given cause
    */
   static <T> CompletionStageFuture<T> failedFuture(Throwable cause) {
      CompletableFutureCallback<T> cf = new CompletableFutureCallback<>();
      cf.completeExceptionally(cause);
      return cf;
   }
   
   /**
    * Wraps the given completion stage. The returned future will complete with the same disposition,
    * either with a successful value or a cause of failure, as the given stage.
    * 
    * @param stage a completion future
    * @return a future that mirrors the given stage and implements {@link CompletionStageFuture}
    */   
   static <T> CompletionStageFuture<T> fromCompletionStage(CompletionStage<T> stage) {
      if (stage instanceof CompletionStageFuture) {
         return (CompletionStageFuture<T>) stage;
      } else if (stage instanceof CompletableFuture) {
         return fromCompletableFuture((CompletableFuture<T>) stage);
      } else {
         try {
            return fromCompletableFuture(stage.toCompletableFuture());
         } catch (UnsupportedOperationException e) {
            com.bluegosling.redis.concurrent.CompletableFuture<T> f =
                  new com.bluegosling.redis.concurrent.CompletableFuture<>();
            stage.whenComplete((v, th) -> {
               if (th != null) {
                  f.completeExceptionally(th);
               } else {
                  f.complete(v);
               }
            });
            return f;
         }
      }
   }
   
   /**
    * Wraps the given future. The returned future will delegate all methods to the given future
    * <em>except</em> for {@link #toCompletableFuture()}, which uses the default implementation
    * defined in this class.
    * 
    * @param future a future
    * @return a wrapper around the given future that implements {@link CompletionStageFuture}
    */
   static <T> CompletionStageFuture<T> fromCompletableFuture(CompletableFuture<T> future) {
      if (future instanceof CompletionStageFuture) {
         // maybe unlikely to come across a future that already implements, but no sense wrapping
         @SuppressWarnings("unchecked") // silly compiler -- this is safe
         CompletionStageFuture<T> ret = (CompletionStageFuture<T>) future; 
         return ret;
      }
      return new CompletionStageFuture<T>() {
         @Override
         public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
         }

         @Override
         public boolean isCancelled() {
            return future.isCancelled();
         }

         @Override
         public boolean isDone() {
            return future.isDone();
         }

         @Override
         public T get() throws InterruptedException, ExecutionException {
            return future.get();
         }

         @Override
         public T get(long timeout, TimeUnit unit)
               throws InterruptedException, ExecutionException, TimeoutException {
            return future.get(timeout, unit);
         }

         @Override
         public <U> CompletionStage<U> thenApply(Function<? super T, ? extends U> fn) {
            return future.thenApply(fn);
         }

         @Override
         public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
            return future.thenApplyAsync(fn);
         }

         @Override
         public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn,
               Executor executor) {
            return future.thenApplyAsync(fn, executor);
         }

         @Override
         public CompletionStage<Void> thenAccept(Consumer<? super T> action) {
            return future.thenAccept(action);
         }

         @Override
         public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action) {
            return future.thenAcceptAsync(action);
         }

         @Override
         public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action,
               Executor executor) {
            return future.thenAcceptAsync(action, executor);
         }

         @Override
         public CompletionStage<Void> thenRun(Runnable action) {
            return future.thenRun(action);
         }

         @Override
         public CompletionStage<Void> thenRunAsync(Runnable action) {
            return future.thenRunAsync(action);
         }

         @Override
         public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
            return future.thenRunAsync(action, executor);
         }

         @Override
         public <U, V> CompletionStage<V> thenCombine(CompletionStage<? extends U> other,
               BiFunction<? super T, ? super U, ? extends V> fn) {
            return future.thenCombine(other, fn);
         }

         @Override
         public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other,
               BiFunction<? super T, ? super U, ? extends V> fn) {
            return future.thenCombineAsync(other, fn);
         }

         @Override
         public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other,
               BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
            return future.thenCombineAsync(other, fn, executor);
         }

         @Override
         public <U> CompletionStage<Void> thenAcceptBoth(CompletionStage<? extends U> other,
               BiConsumer<? super T, ? super U> action) {
            return future.thenAcceptBoth(other, action);
         }

         @Override
         public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
               BiConsumer<? super T, ? super U> action) {
            return future.thenAcceptBothAsync(other, action);
         }

         @Override
         public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
               BiConsumer<? super T, ? super U> action, Executor executor) {
            return future.thenAcceptBothAsync(other, action, executor);
         }

         @Override
         public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
            return future.runAfterBoth(other, action);
         }

         @Override
         public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
            return future.runAfterBothAsync(other, action);
         }

         @Override
         public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action,
               Executor executor) {
            return future.runAfterBothAsync(other, action, executor);
         }

         @Override
         public <U> CompletionStage<U> applyToEither(CompletionStage<? extends T> other,
               Function<? super T, U> fn) {
            return future.applyToEither(other, fn);
         }

         @Override
         public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other,
               Function<? super T, U> fn) {
            return future.applyToEitherAsync(other, fn);
         }

         @Override
         public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other,
               Function<? super T, U> fn, Executor executor) {
            return future.applyToEitherAsync(other, fn, executor);
         }

         @Override
         public CompletionStage<Void> acceptEither(CompletionStage<? extends T> other,
               Consumer<? super T> action) {
            return future.acceptEither(other, action);
         }

         @Override
         public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other,
               Consumer<? super T> action) {
            return future.acceptEitherAsync(other, action);
         }

         @Override
         public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other,
               Consumer<? super T> action, Executor executor) {
            return future.acceptEitherAsync(other, action, executor);
         }

         @Override
         public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
            return future.runAfterEither(other, action);
         }

         @Override
         public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other,
               Runnable action) {
            return future.runAfterEitherAsync(other, action);
         }

         @Override
         public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action,
               Executor executor) {
            return future.runAfterEitherAsync(other, action, executor);
         }

         @Override
         public <U> CompletionStage<U> thenCompose(
               Function<? super T, ? extends CompletionStage<U>> fn) {
            return future.thenCompose(fn);
         }

         @Override
         public <U> CompletionStage<U> thenComposeAsync(
               Function<? super T, ? extends CompletionStage<U>> fn) {
            return future.thenComposeAsync(fn);
         }

         @Override
         public <U> CompletionStage<U> thenComposeAsync(
               Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
            return future.thenComposeAsync(fn, executor);
         }

         @Override
         public CompletionStage<T> exceptionally(Function<Throwable, ? extends T> fn) {
            return future.exceptionally(fn);
         }

         @Override
         public CompletionStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
            return future.whenComplete(action);
         }

         @Override
         public CompletionStage<T> whenCompleteAsync(
               BiConsumer<? super T, ? super Throwable> action) {
            return future.whenCompleteAsync(action);
         }

         @Override
         public CompletionStage<T> whenCompleteAsync(
               BiConsumer<? super T, ? super Throwable> action, Executor executor) {
            return future.whenCompleteAsync(action, executor);
         }

         @Override
         public <U> CompletionStage<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
            return future.handle(fn);
         }

         @Override
         public <U> CompletionStage<U> handleAsync(
               BiFunction<? super T, Throwable, ? extends U> fn) {
            return future.handleAsync(fn);
         }

         @Override
         public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn,
               Executor executor) {
            return future.handleAsync(fn, executor);
         }
      };
   }
}
