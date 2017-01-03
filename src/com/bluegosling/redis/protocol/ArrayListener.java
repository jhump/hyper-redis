package com.bluegosling.redis.protocol;

import java.util.function.Function;
import java.util.function.IntFunction;

import com.bluegosling.redis.concurrent.Callback;

public class ArrayListener<T> extends BaseReplyListener<T[]> {
   private final IntFunction<T[]> arrayConstructor;
   private final Function<Callback<T>, ReplyListener> elementListener;

   public ArrayListener(Callback<T[]> callback,
         IntFunction<T[]> arrayConstructor,
         Function<Callback<T>, ReplyListener> elementListener) {
      super(callback);
      this.arrayConstructor = arrayConstructor;
      this.elementListener = elementListener;
   }

   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      if (numberOfElements <= 0) {
         callbackSuccess(arrayConstructor.apply(0));
         return NO_OP;
      }
      Callback<T> elementCallback = new Callback<T>() {
         private T[] array = arrayConstructor.apply(numberOfElements);
         private int index;

         @Override
         public void onSuccess(T t) {
            array[index++] = t;
            if (index == array.length) {
               callbackSuccess(array);
               array = null;
            }
         }

         @Override
         public void onFailure(Throwable th) {
            callbackFailure(th);
         }
      };
      return elementListener.apply(elementCallback);
   }
}
