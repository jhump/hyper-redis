package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;

public class ArrayOfLongListener extends BaseReplyListener<long[]> {
   private static final long[] EMPTY = new long[0];
   
   public ArrayOfLongListener(Callback<long[]> callback) {
      super(callback);
   }

   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      if (numberOfElements <= 0) {
         callbackSuccess(EMPTY);
         return NO_OP;
      }
      Callback.OfLong elementCallback = new Callback.OfLong() {
         private long[] array = new long[numberOfElements];
         private int index;

         @Override
         public void onSuccess(long l) {
            array[index++] = l;
            if (index == array.length) {
               ArrayOfLongListener.this.callbackSuccess(array);
               array = null;
            }
         }

         @Override
         public void onFailure(Throwable th) {
            ArrayOfLongListener.this.callbackFailure(th);
         }
      };
      return new LongListener(elementCallback);
   }
}
