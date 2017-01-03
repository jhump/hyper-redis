package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.values.Response;

import io.netty.buffer.ByteBuf;

public class ResponseListener extends BaseReplyListener<Response> {
   public ResponseListener(Callback<Response> callback) {
      super(callback);
   }
   
   @Override
   public void onSimpleReply(String resp) {
      callbackSuccess(Response.simpleResponse(resp));
   }

   @Override
   public void onBulkReply(ByteBuf bytes) {
      byte[] array = new byte[bytes.readableBytes()];
      bytes.getBytes(bytes.readerIndex(), array);
      callbackSuccess(Response.bulkResponse(array));
   }

   @Override
   public void onErrorReply(String errorMsg) {
      callbackSuccess(Response.errorResponse(errorMsg));
   }

   @Override
   public void onIntegerReply(long value) {
      callbackSuccess(Response.intResponse(value));
   }
   
   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      if (numberOfElements < 0) {
         callbackSuccess(Response.nilArrayResponse());
         return NO_OP;
      } else if (numberOfElements == 0) {
         callbackSuccess(Response.emptyArrayResponse());
         return NO_OP;
      }
      Callback<Response> elementCallback = new Callback<Response>() {
         private Response[] array = new Response[numberOfElements];
         private int index;

         @Override
         public void onSuccess(Response r) {
            array[index++] = r;
            if (index == array.length) {
               ResponseListener.this.callbackSuccess(Response.arrayResponse(array));
               array = null;
            }
         }

         @Override
         public void onFailure(Throwable th) {
            ResponseListener.this.callbackFailure(th);
         }
      };
      return new ResponseListener(elementCallback);
   }
}
