package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;

public class OkOrNoKeyListener extends BaseReplyListener<Boolean> {
   public OkOrNoKeyListener(Callback<Boolean> callback) {
      super(callback);
   }

   @Override
   public void onSimpleReply(String string) {
      if ("ok".equalsIgnoreCase(string)) {
         callbackSuccess(true);
      } else if ("nokey".equalsIgnoreCase(string)) {
         callbackSuccess(false);
      } else {
         super.onSimpleReply(string);
      }
   }
}
