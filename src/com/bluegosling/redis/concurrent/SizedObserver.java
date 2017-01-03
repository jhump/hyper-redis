package com.bluegosling.redis.concurrent;

import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * An observer that can optionally be initialized with a count of the message coming in the stream.
 * Note that there is no guarantee that the method will be called -- for example, if the source
 * does not know the number of messages coming. If it is called, it will be called exactly once and
 * prior to the first call to {@link #onNext(Object)}.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of messages in the stream
 */
public interface SizedObserver<T> extends Observer<T> {
   /**
    * Indicates the number of elements that the observer can expect in the stream. With this
    * method, the observer can size things like buffers/caches ahead of time and knows when the
    * stream is done after observing the given number of elements, instead of having to wait for an
    * {@link #onFinish} notification.
    * 
    * <p>Note that this method is optional. Some observables may not know the expected size of the
    * stream and therefore may never invoke this method. If the size is known, this method will be
    * invoked exactly once before any calls to {@link #onNext(Object)}. Thus this method might or
    * might not be invoked prior to {@link #onFinish()} or {@link #onFailure(Throwable)} for
    * observables that never emit any elements.
    * 
    * @param numberOfElements the number of elements that the observer can expect in the stream
    */
   void initialize(int numberOfElements);

   /**
    * Constructs an observer from functional interfaces, suitable for building observers from lambda
    * expressions.
    * 
    * @param initialize an action invoked if/when the observer is initialized
    * @param onNext an action invoked for each element in the stream
    * @param onFinish an action invoked when the stream is finished
    * @param onFailure an action invoked on operation failure
    * @return an observer that invokes the given functions when elements arrive and when the stream
    *       completes
    */
   static <T> SizedObserver<T> of(IntConsumer initialize, Consumer<? super T> onNext,
         Runnable onFinish, Consumer<? super Throwable> onFailure) {
      return new SizedObserver<T>() {
         @Override
         public void initialize(int numberOfElements) {
            initialize.accept(numberOfElements);
         }
         
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
}
