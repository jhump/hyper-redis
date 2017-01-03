package com.bluegosling.redis.concurrent;

import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * An interface for asynchronous callbacks. A callback receives a notice of completion of an
 * asynchronous operation which, on success, produces a result value.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of result value produced
 */
public interface Callback<T> {
   /**
    * Signals successful completion of the asynchronous task.
    * 
    * @param t the result of the asynchronous operation
    */
   void onSuccess(T t);

   /**
    * Signals unsuccessful completion of the asynchronous task.
    * 
    * @param th the cause of failure of the asynchronous operation
    */
   void onFailure(Throwable th);
   
   /**
    * Maps the type of this callback from another type. The given mapping function is used to
    * transform the source type to the type this callback expects. The returned callback, when
    * invoked, will apply the function and then send the result to this callback.
    * 
    * @param function function that adapts another type to this callback's type
    * @return a callback that will transform its given value and the send result to this callback
    */
   default <U> Callback<U> map(Function<? super U, ? extends T> function) {
      Callback<T> self = this;
      return new Callback<U>() {
         @Override
         public void onSuccess(U u) {
            self.onSuccess(function.apply(u));
         }

         @Override
         public void onFailure(Throwable th) {
            self.onFailure(th);
         }
      };
   }
   
   /**
    * Constructs a callback from functional interfaces, suitable for building callbacks from lambda
    * expressions.
    * 
    * @param onSuccess an action invoked on operation success
    * @param onFailure an action invoked on operation failure
    * @return a callback that invokes the given functions when the asynchronous operation completes
    */
   static <T> Callback<T> of(Consumer<? super T> onSuccess, Consumer<? super Throwable> onFailure) {
      return new Callback<T>() {
         @Override
         public void onSuccess(T t) {
            onSuccess.accept(t);
         }

         @Override
         public void onFailure(Throwable th) {
            onFailure.accept(th);
         }
      };
   }
   
   /**
    * A primitive specialization of {@link Callback} for {@code long} result values.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    */
   interface OfLong extends Callback<Long> {
      /**
       * Signals successful completion of the asynchronous task via a primitive {@code long} result.
       * 
       * @param l the result of the asynchronous operation
       */
      void onSuccess(long l);
      
      /**
       * Adapts the referenced-based {@link Callback} to primitive values. This default
       * implementation unboxes the given value and delegates to the {@linkplain #onSuccess(long)
       * primitive form}.
       */
      default void onSuccess(Long l) {
         onSuccess(l.longValue());
      }
      
      /**
       * Maps the type of this callback from another type. The given mapping function is used to
       * transform the source type to the type this callback expects. The returned callback, when
       * invoked, will apply the function and then send the result to this callback.
       * 
       * @param function function that adapts another type to this callback's type
       * @return a callback that will transform its given value and the send result to this callback
       */
      default <U> Callback<U> map(ToLongFunction<? super U> function) {
         OfLong self = this;
         return new Callback<U>() {
            @Override
            public void onSuccess(U u) {
               self.onSuccess(function.applyAsLong(u));
            }

            @Override
            public void onFailure(Throwable th) {
               self.onFailure(th);
            }
         };
      }
      
      /**
       * Constructs a callback from functional interfaces, suitable for building callbacks from
       * lambda expressions.
       * 
       * @param onSuccess an action invoked on operation success
       * @param onFailure an action invoked on operation failure
       * @return a callback that invokes the given functions when the asynchronous operation
       *       completes
       */
      static OfLong of(LongConsumer onSuccess, Consumer<? super Throwable> onFailure) {
         return new OfLong() {
            @Override
            public void onSuccess(long l) {
               onSuccess.accept(l);
            }

            @Override
            public void onFailure(Throwable th) {
               onFailure.accept(th);
            }
         };
      }
   }

   /**
    * A primitive specialization of {@link Callback} for {@code int} result values.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    */
   interface OfInt extends Callback<Integer> {
      /**
       * Signals successful completion of the asynchronous task via a primitive {@code int} result.
       * 
       * @param i the result of the asynchronous operation
       */
      void onSuccess(int i);
      
      /**
       * Adapts the referenced-based {@link Callback} to primitive values. This default
       * implementation unboxes the given value and delegates to the {@linkplain #onSuccess(int)
       * primitive form}.
       */
      default void onSuccess(Integer i) {
         onSuccess(i.intValue());
      }

      /**
       * Maps the type of this callback from another type. The given mapping function is used to
       * transform the source type to the type this callback expects. The returned callback, when
       * invoked, will apply the function and then send the result to this callback.
       * 
       * @param function function that adapts another type to this callback's type
       * @return a callback that will transform its given value and the send result to this callback
       */
      default <U> Callback<U> map(ToIntFunction<? super U> function) {
         OfInt self = this;
         return new Callback<U>() {
            @Override
            public void onSuccess(U u) {
               self.onSuccess(function.applyAsInt(u));
            }

            @Override
            public void onFailure(Throwable th) {
               self.onFailure(th);
            }
         };
      }
      
      /**
       * Constructs a callback from functional interfaces, suitable for building callbacks from
       * lambda expressions.
       * 
       * @param onSuccess an action invoked on operation success
       * @param onFailure an action invoked on operation failure
       * @return a callback that invokes the given functions when the asynchronous operation
       *       completes
       */
      static OfInt of(IntConsumer onSuccess, Consumer<? super Throwable> onFailure) {
         return new OfInt() {
            @Override
            public void onSuccess(int i) {
               onSuccess.accept(i);
            }

            @Override
            public void onFailure(Throwable th) {
               onFailure.accept(th);
            }
         };
      }
   }

   /**
    * A primitive specialization of {@link Callback} for {@code boolean} result values.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    */
   interface OfBoolean extends Callback<Boolean> {
      /**
       * Signals successful completion of the asynchronous task via a primitive {@code boolean}
       * result.
       * 
       * @param b the result of the asynchronous operation
       */
      void onSuccess(boolean b);
      
      /**
       * Adapts the referenced-based {@link Callback} to primitive values. This default
       * implementation unboxes the given value and delegates to the {@linkplain #onSuccess(boolean)
       * primitive form}.
       */
      default void onSuccess(Boolean b) {
         onSuccess(b.booleanValue());
      }
   }
   
   /**
    * A primitive specialization of {@link Callback} for {@code double} result values.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    */
   interface OfDouble extends Callback<Double> {
      /**
       * Signals successful completion of the asynchronous task via a primitive {@code double}
       * result.
       * 
       * @param d the result of the asynchronous operation
       */
      void onSuccess(double d);
      
      /**
       * Adapts the referenced-based {@link Callback} to primitive values. This default
       * implementation unboxes the given value and delegates to the {@linkplain #onSuccess(double)
       * primitive form}.
       */
      default void onSuccess(Double d) {
         onSuccess(d.doubleValue());
      }

      /**
       * Maps the type of this callback from another type. The given mapping function is used to
       * transform the source type to the type this callback expects. The returned callback, when
       * invoked, will apply the function and then send the result to this callback.
       * 
       * @param function function that adapts another type to this callback's type
       * @return a callback that will transform its given value and the send result to this callback
       */
      default <U> Callback<U> map(ToDoubleFunction<? super U> function) {
         OfDouble self = this;
         return new Callback<U>() {
            @Override
            public void onSuccess(U u) {
               self.onSuccess(function.applyAsDouble(u));
            }

            @Override
            public void onFailure(Throwable th) {
               self.onFailure(th);
            }
         };
      }
      
      /**
       * Constructs a callback from functional interfaces, suitable for building callbacks from
       * lambda expressions.
       * 
       * @param onSuccess an action invoked on operation success
       * @param onFailure an action invoked on operation failure
       * @return a callback that invokes the given functions when the asynchronous operation
       *       completes
       */
      static OfDouble of(DoubleConsumer onSuccess, Consumer<? super Throwable> onFailure) {
         return new OfDouble() {
            @Override
            public void onSuccess(double d) {
               onSuccess.accept(d);
            }

            @Override
            public void onFailure(Throwable th) {
               onFailure.accept(th);
            }
         };
      }
   }
   
   /**
    * A primitive specialization of {@link Callback} for {@code void} results.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    */
   interface OfVoid extends Callback<Void> {
      /**
       * Signals successful completion of the asynchronous task.
       */
      void onSuccess();
      
      /**
       * Adapts the referenced-based {@link Callback} to this primitive specialization. This default
       * implementation simply invokes the {@linkplain #onSuccess() primitive form}.
       */
      default void onSuccess(Void v) {
         onSuccess();
      }
      
      /**
       * Maps the type of this callback from another type. The source value is simply ignored
       * since the only valid {@code Void} value is {@code null}.
       * 
       * @return a callback that will forward completions to this callback, ignoring any value
       */
      default <U> Callback<U> map() {
         OfVoid self = this;
         return new Callback<U>() {
            @Override
            public void onSuccess(U u) {
               self.onSuccess();
            }

            @Override
            public void onFailure(Throwable th) {
               self.onFailure(th);
            }
         };
      }
      
      /**
       * Constructs a callback from functional interfaces, suitable for building callbacks from
       * lambda expressions.
       * 
       * @param onSuccess an action invoked on operation success
       * @param onFailure an action invoked on operation failure
       * @return a callback that invokes the given functions when the asynchronous operation
       *       completes
       */
      static OfVoid of(Runnable onSuccess, Consumer<? super Throwable> onFailure) {
         return new OfVoid() {
            @Override
            public void onSuccess() {
               onSuccess.run();
            }

            @Override
            public void onFailure(Throwable th) {
               onFailure.accept(th);
            }
         };
      }
   }
}
