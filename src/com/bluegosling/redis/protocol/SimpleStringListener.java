package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;

public class SimpleStringListener extends BaseReplyListener<String> {
   public SimpleStringListener(Callback<String> callback) {
      super(callback);
   }

   @Override
   public void onSimpleReply(String reply) {
      callbackSuccess(reply);
   }
}
