package com.bluegosling.redis.transaction;

import java.util.NoSuchElementException;

import com.bluegosling.redis.concurrent.Callback;

/**
 * A placeholder for a value that will be provided later. Promises are returned for write operations
 * in a transaction. The promises will be filled when the transaction succeeds. If the transaction
 * fails, the promise is broken and the cause of failure can be retrieved.
 * 
 * <p>This is similar in API to a {@link Future} but it is important to keep in mind that a
 * {@link Promise} is <em>not thread-safe</em>. The promises are expected to be used only by the
 * thread that created them, but after completion of a transaction.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of result promised
 */
public class Promise<T> {
   private enum State {
      PENDING, FULFILLED, BROKEN
   }
   
   private State state = State.PENDING;
   private Object result;
   
   /**
    * Returns true if the promise is still pending. This indicates that the operation that produced
    * the promise has not concluded.
    * 
    * @return true if the promise is still pending
    */
   public boolean isStillPending() {
      return state == State.PENDING;
   }

   /**
    * Returns true if the promise has been fulfilled with a value. This indicates that the operation
    * that produced the promise completed successfully.
    * 
    * @return true if the promise has been fulfilled
    */
   public boolean isFulfilled() {
      return state == State.FULFILLED;
   }

   /**
    * Returns true if the promise has been broken. This indicates that the operation that produced
    * the promise failed.
    * 
    * @return true if the promise has been broken
    */
   public boolean isBroken() {
      return state == State.BROKEN;
   }
   
   /**
    * Retrieves the promised result.
    * 
    * @return the promised result
    * @throws NoSuchElementException if the promise is not fulfilled
    */
   @SuppressWarnings("unchecked")
   public T get() {
      if (state != State.FULFILLED) {
         throw new NoSuchElementException();
      }
      return (T) result;
   }
   
   /**
    * Retrieves the promised result, propagating the cause of failure if the promise is broken. If
    * the cause of failure is a checked exception, it is wrapped in a {@link RuntimeException}.
    * 
    * @return the promised result
    * @throws PromiseStillPendingException if the promise is still pending
    * @throws Error if the promise was broken and the cause was an {@link Error}
    * @throws RuntimeException if the promise was broken and the cause was a
    *       {@link RuntimeException} or if the promise was broken and the cause was a checked
    *       exception
    */
   public T checkedGet() {
      if (isFulfilled()) {
         return get();
      }
      Throwable th = isBroken() ? failure() : new PromiseStillPendingException();
      if (th instanceof RuntimeException) {
         throw (RuntimeException) th;
      } else if (th instanceof Error) {
         throw (Error) th;
      } else {
         throw new RuntimeException(th);
      }
   }
   
   /**
    * Retrieves the cause of failure of a broken promise.
    * 
    * @return the cause of failure of a broken promise
    * @throws IllegalStateException if the promise is not broken
    */
   public Throwable failure() {
      if (state != State.BROKEN) {
         throw new IllegalStateException();
      }
      return (Throwable) result;
   }
   
   /**
    * Returns a {@link Callback} that will fulfill or break the promise when invoked.
    * 
    * @return a callback that can be used to complete the promise
    */
   public Callback<T> callback() {
      return Callback.of(this::fulfill, this::breakPromise);
   }
   
   private void fulfill(T t) {
      if (state == State.PENDING) {
         result = t;
         state = State.FULFILLED;
      }
   }
   
   private void breakPromise(Throwable th) {
      if (state == State.PENDING) {
         result = th;
         state = State.BROKEN;
      }
   }
}
