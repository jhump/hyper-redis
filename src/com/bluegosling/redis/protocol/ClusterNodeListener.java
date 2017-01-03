package com.bluegosling.redis.protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.values.ClusterNode;
import com.bluegosling.redis.values.Response;

import io.netty.buffer.ByteBuf;

public class ClusterNodeListener extends BaseReplyListener<ClusterNode> {
   public ClusterNodeListener(Callback<ClusterNode> callback) {
      super(callback);
   }

   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      if (numberOfElements < 0) {
         callbackSuccess(null);
         return NO_OP;
      } else if (numberOfElements == 0) {
         callbackSuccess(new ClusterNode("", null, Collections.emptyList()));
         return NO_OP;
      }
      return new ArrayElementListener(getCallback(), numberOfElements);
   }
   
   public static class ArrayElementListener extends BaseReplyListener<ClusterNode> {
      private final ReplyListener stringListener;
      private final ReplyListener intListener;
      private final ReplyListener responseListener;
      private int tokensReceived;
      private int tokensExpected;
      private String hostname;
      private int port;
      private String nodeId;
      private List<Response> unknownFields = new ArrayList<>();
      
      public ArrayElementListener(Callback<ClusterNode> callback, int tokensExpected) {
         super(callback);
         this.tokensExpected = tokensExpected;
         this.stringListener = new StringListener(new Callback<String>() {
            @Override
            public void onSuccess(String s) {
               if (tokensReceived == 0) {
                  hostname = s;
               } else {
                  assert tokensReceived == 2;
                  nodeId = s;
               }
               tokenReceived();
            }

            @Override
            public void onFailure(Throwable th) {
               callback.onFailure(th);
            }
         });
         this.intListener = new IntListener(new Callback.OfInt() {
            @Override
            public void onSuccess(int i) {
               assert tokensReceived == 1;
               port = i;
               tokenReceived();
            }

            @Override
            public void onFailure(Throwable th) {
               callback.onFailure(th);
            }
         });
         this.responseListener = new ResponseListener(new Callback<Response>() {
            @Override
            public void onSuccess(Response r) {
               assert tokensReceived >= 3;
               unknownFields.add(r);
               tokenReceived();
            }

            @Override
            public void onFailure(Throwable th) {
               callback.onFailure(th);
            }
         });
      }
      
      private void tokenReceived() {
         tokensReceived++;
         if (tokensReceived == tokensExpected) {
            callbackSuccess(new ClusterNode(hostname + port, nodeId, unknownFields));
         }
      }
      
      private ReplyListener current() {
         if (tokensReceived == 1) {
            return intListener;
         } else if (tokensReceived >= 3) {
            return responseListener;
         } else {
            return stringListener;
         }
      }

      @Override
      public void onSimpleReply(String string) {
         current().onSimpleReply(string);
      }

      @Override
      public void onBulkReply(ByteBuf bytes) {
         current().onBulkReply(bytes);
      }

      @Override
      public void onErrorReply(String errorMsg) {
         current().onErrorReply(errorMsg);
      }

      @Override
      public void onIntegerReply(long value) {
         current().onIntegerReply(value);
      }

      @Override
      public ReplyListener onArrayReply(int numberOfElements) {
         return current().onArrayReply(numberOfElements);
      }
   }
}
