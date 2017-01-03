package com.bluegosling.redis.protocol;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import com.bluegosling.redis.concurrent.Callback;
import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;

public class InstantWithMicrosListener extends BaseReplyListener<Instant> {
   private int expectingElements = -1;
   private long epochSeconds;

   public InstantWithMicrosListener(Callback<Instant> callback) {
      super(callback);
   }

   @Override
   public void onIntegerReply(long value) {
      if (expectingElements == -1) {
         super.onIntegerReply(value);
         return;
      }
      
      if (--expectingElements == 0) {
         callbackSuccess(Instant.ofEpochSecond(epochSeconds, TimeUnit.MICROSECONDS.toNanos(value)));
      } else {
         assert expectingElements == 1;
         epochSeconds = value;
      }
   }

   @Override
   public void onBulkReply(ByteBuf bytes) {
      if (expectingElements == -1) {
         super.onBulkReply(bytes);
         return;
      }
      
      long val;
      try {
         val = Long.parseLong(bytes.toString(Charsets.UTF_8));
      } catch (Throwable th) {
         callbackFailure(th);
         return;
      }
      onIntegerReply(val);
   }

   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      if (expectingElements != -1 || numberOfElements != 2) {
         return super.onArrayReply(numberOfElements);
      }
      expectingElements = 2;
      return this;
   }

}
