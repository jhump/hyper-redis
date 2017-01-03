package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;

public class BooleanListener extends BaseReplyListener<Boolean> {
   public BooleanListener(Callback<Boolean> callback) {
      super(callback);
   }

   @Override
   public void onIntegerReply(long value) {
      callbackSuccess(value != 0);
   }
}
