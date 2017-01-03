package com.bluegosling.redis.concurrent;

/**
 * A future that also implements {@link Callback}, whose methods can be used to complete the
 * future.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of future value
 */
public class CompletableFutureCallback<T> extends CompletableFuture<T>
implements Callback<T> {
   @Override
   public void onSuccess(T t) {
      complete(t);
   }

   @Override
   public void onFailure(Throwable th) {
      completeExceptionally(th);
   }
}
