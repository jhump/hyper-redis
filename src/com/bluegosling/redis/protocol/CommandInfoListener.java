package com.bluegosling.redis.protocol;

import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.values.CommandInfo;
import com.bluegosling.redis.values.Response;

import io.netty.buffer.ByteBuf;

public class CommandInfoListener extends BaseReplyListener<CommandInfo> {
   public CommandInfoListener(Callback<CommandInfo> callback) {
      super(callback);
   }

   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      if (numberOfElements < 0) {
         callbackSuccess(null);
         return NO_OP;
      } else if (numberOfElements == 0) {
         callbackSuccess(new CommandInfo.Builder().build());
         return NO_OP;
      }
      return new ArrayElementListener(getCallback(), numberOfElements);
   }
   
   public static class ArrayElementListener extends BaseReplyListener<CommandInfo> {
      private final ReplyListener stringListener;
      private final ReplyListener intListener;
      private final ReplyListener flagsListener;
      private final ReplyListener responseListener;
      private int tokensReceived;
      private int tokensExpected;
      private final CommandInfo.Builder builder = new CommandInfo.Builder();
      
      public ArrayElementListener(Callback<CommandInfo> callback, int tokensExpected) {
         super(callback);
         this.tokensExpected = tokensExpected;
         this.stringListener = new StringListener(new Callback<String>() {
            @Override
            public void onSuccess(String s) {
               assert tokensReceived == 0;
               builder.name(s);
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
               if (tokensReceived == 1) {
                  builder.arity(i);
               } else if (tokensReceived == 3) {
                  builder.firstKeyPosition(i);
               } else if (tokensReceived == 4) {
                  builder.lastKeyPosition(i);
               } else if (tokensReceived == 5) {
                  builder.stepCountForRepeatingKeys(i);
               } else {
                  throw new AssertionError();
               }
               tokenReceived();
            }

            @Override
            public void onFailure(Throwable th) {
               callback.onFailure(th);
            }
         });
         this.flagsListener = new ArrayListener<>(new Callback<String[]>() {
            @Override
            public void onSuccess(String[] a) {
               assert tokensReceived == 2;
               for (String s : a) {
                  builder.flag(s);
               }
               tokenReceived();
            }

            @Override
            public void onFailure(Throwable th) {
               callback.onFailure(th);
            }
         }, String[]::new, StringListener::new);
         this.responseListener = new ResponseListener(new Callback<Response>() {
            @Override
            public void onSuccess(Response r) {
               assert tokensReceived >= 6;
               builder.unknownAttribute(r);
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
            callbackSuccess(builder.build());
         }
      }
      
      private ReplyListener current() {
         if (tokensReceived == 0) {
            return stringListener;
         } else if (tokensReceived == 2) {
            return flagsListener;
         } else if (tokensReceived >= 6) {
            return responseListener;
         } else {
            return intListener;
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
