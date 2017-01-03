package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;

public class LongListener extends BaseReplyListener<Long> {
   public LongListener(Callback<Long> callback) {
      super(callback);
   }

   @Override
   public void onIntegerReply(long value) {
      callbackSuccess(value);
   }
}
