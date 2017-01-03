package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;

public class VoidListener extends BaseReplyListener<Void> {
   public VoidListener(Callback<Void> callback) {
      super(callback);
   }

   @Override
   public void onSimpleReply(String string) {
      // TODO: require that string is "OK"?
      callbackSuccess(null);
   }
}
