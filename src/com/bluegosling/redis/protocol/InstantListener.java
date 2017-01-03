package com.bluegosling.redis.protocol;

import java.time.Instant;

import com.bluegosling.redis.concurrent.Callback;

public class InstantListener extends BaseReplyListener<Instant> {

   public InstantListener(Callback<Instant> callback) {
      super(callback);
   }

   @Override
   public void onIntegerReply(long value) {
      callbackSuccess(Instant.ofEpochSecond(value));
   }
}
