package com.bluegosling.redis.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * A subject is used to create a stream of elements. Its API is the same as {@link Observer}, which
 * is how calling code provides data and notices into the produced stream.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of elements in the stream
 */
public class Subject<T> implements SizedObserver<T> {
   private final Observable<T> observable;
   private final SizedObserver<T> driver;

   /**
    * Creates a subject that does not cache any emitted items and uses the {@linkplain
    * ForkJoinPool#commonPool() common pool} to dispatch events to subscribers.
    * 
    * @see Observable#Observable()
    */
   public Subject() {
      this(0);
   }
   
   /**
    * Creates a subject that caches up to the given number of emitted items and uses the
    * {@linkplain ForkJoinPool#commonPool() common pool} to dispatch events to subscribers.
    * 
    * @see Observable#Observable(int)
    */
   public Subject(int cacheSize) {
      this(cacheSize, ForkJoinPool.commonPool());
   }

   /**
    * Creates a subject that does not cache any emitted items and uses the given executor to
    * dispatch events to subscribers.
    * 
    * @see Observable#Observable(Executor)
    */
   public Subject(Executor executor) {
      this(0, executor);
   }
   
   /**
    * Creates a subject that caches up to the given number of emitted items and uses the
    * given executor to dispatch events to subscribers.
    * 
    * @see Observable#Observable(int, Executor)
    */
   public Subject(int cacheSize, Executor executor) {
      this.observable = new Observable<>(cacheSize, executor);
      this.driver = this.observable.getDriver();
   }

   /**
    * Returns a view of the produced stream as an {@link Observable}.
    * 
    * @return a view of the produced stream as an {@link Observable}
    */
   public Observable<T> asObservable() {
      return observable;
   }

   @Override
   public void onNext(T element) {
      driver.onNext(element);
   }

   @Override
   public void onFinish() {
      driver.onFinish();
   }

   @Override
   public void onFailure(Throwable failure) {
      driver.onFailure(failure);
   }

   @Override
   public void initialize(int numberOfElements) {
      driver.initialize(numberOfElements);
   }
}
