package com.bluegosling.redis.concurrent;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * An callback for observing the elements in a stream. This is similar to {@link Callback} except
 * that it explicitly supports cases where there are zero or more than one com.bluegosling.redis.values. This is to
 * {@link Callback} as {@link Iterator} is to unary com.bluegosling.redis.values, except asynchronous and push-based
 * instead of synchronous and pull-based.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of elements in the stream
 */
public interface Observer<T> {
   /**
    * Signals arrival of a new element in the stream.
    * 
    * @param element the element
    */
   void onNext(T element);
   
   /**
    * Signals the successful completion of the stream. No more elements will arrive after this.
    */
   void onFinish();
   
   /**
    * Signals failure of the stream. No more elements will arrive after this.
    * 
    * @param failure the cause of failure
    */
   void onFailure(Throwable failure);
   
   /**
    * Constructs an observer from functional interfaces, suitable for building observers from lambda
    * expressions.
    * 
    * @param onNext an action invoked for each element in the stream
    * @param onFinish an action invoked when the stream is finished
    * @param onFailure an action invoked on operation failure
    * @return an observer that invokes the given functions when elements arrive and when the stream
    *       completes
    */
   static <T> Observer<T> of(Consumer<? super T> onNext, Runnable onFinish,
         Consumer<? super Throwable> onFailure) {
      return new Observer<T>() {
         @Override
         public void onNext(T element) {
            onNext.accept(element);
         }

         @Override
         public void onFinish() {
            onFinish.run();
         }

         @Override
         public void onFailure(Throwable failure) {
            onFailure.accept(failure);
         }
      };
   }
   
   /**
    * Constructs an observer from functional interfaces when notice of successful end of stream is
    * not needed. This is suitable for building observers from lambda expressions. Notices of
    * successful completion are ignored.
    * 
    * @param onNext an action invoked for each element in the stream
    * @param onFailure an action invoked on operation failure
    * @return an observer that invokes the given functions when elements arrive and when the stream
    *       completes
    */
   static <T> Observer<T> of(Consumer<? super T> onNext, Consumer<? super Throwable> onFailure) {
      return new Observer<T>() {
         @Override
         public void onNext(T element) {
            onNext.accept(element);
         }

         @Override
         public void onFinish() {
         }

         @Override
         public void onFailure(Throwable failure) {
            onFailure.accept(failure);
         }
      };
   }
   
   /**
    * Adapts a consumer to the observer interface. Notifications for errors and stream completion
    * are ignored.
    * 
    * @param consumer an action invoked for each element in the stream
    * @return an observer that invokes the given function when elements arrive
    */
   static <T> Observer<T> of(Consumer<? super T> consumer) {
      return new Observer<T>() {
         @Override
         public void onNext(T element) {
            consumer.accept(element);
         }

         @Override
         public void onFinish() {
         }

         @Override
         public void onFailure(Throwable failure) {
         }
      };
   }
}
