package com.bluegosling.redis.protocol;

import static com.google.common.base.Preconditions.checkState;

import com.bluegosling.redis.concurrent.Callback;

public class IntListener extends BaseReplyListener<Integer> {
   public IntListener(Callback<Integer> callback) {
      super(callback);
   }

   @Override
   public void onIntegerReply(long value) {
      checkState(value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE);
      callbackSuccess((int) value);
   }
}
