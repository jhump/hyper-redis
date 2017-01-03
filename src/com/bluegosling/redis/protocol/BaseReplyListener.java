package com.bluegosling.redis.protocol;

import com.bluegosling.redis.RedisException;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.values.Response;

import io.netty.buffer.ByteBuf;

/**
 * A base implementation for listeners of replies to Redis commands. Sub-classes should override at
 * least one method, optionally transform the given value, and invoke {@link #callbackSuccess}.
 * 
 * <p>This base class propagates {@linkplain #onFailure(Throwable) failures} to the callback and
 * also ensures that the callback is not called after a failure (useful for array element listeners
 * which can be invoked multiple times, once for each element). The other methods have a default
 * implementation that fails the operation with a {@link RedisUnexpectedResponseException}. So if
 * a given command only expects status replies, the listener can override just
 * {@link #onSimpleReply} and leave the others so that they indicate protocol failures.
 * 
 * <p>Error replies are handled slightly differently. Instead of failing with a
 * {@link RedisUnexpectedResponseException} an error reply notifies the callback of failure with
 * a {@link RedisException} whose error message comes from the error reply.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of value being constructed by the listener and sent to the callback
 */
public abstract class BaseReplyListener<T> implements ReplyListener {
   private final Callback<T> callback;
   private boolean failed;
   
   /**
    * Constructs a new listener which will notify the given callback when a reply is received.
    * 
    * @param callback a callback
    */
   protected BaseReplyListener(Callback<T> callback) {
      this.callback = callback;
   }
   
   /**
    * Gets the current callback. The callback given at construction time is actually wrapped so
    * that if a call to {@link Callback#onSuccess} is made after a call to
    * {@link Callback#onFailure}, it is ignored. (Failures are considered final.)
    * 
    * @return the current callback
    */
   protected final Callback<T> getCallback() {
      return Callback.of(this::callbackSuccess, this::callbackFailure);
   }
   
   /**
    * Invokes {@link Callback#onSuccess} on the current callback. The call is skipped if
    * {@link #onFailure} has already been invoked.
    * 
    * @param t the value to send to the callback
    * @return true on success or false if the call was skipped because a failure has already been
    *       reported
    */
   protected final boolean callbackSuccess(T t) {
      if (failed) {
         return false;
      }
      callback.onSuccess(t);
      return true;
   }
   
   /**
    * Invokes {@link Callback#onFailure} on the current callback. The call is skipped if it has
    * already been invoked.
    * 
    * @param th the cause of failure to send to the callback
    * @return true on success or false if the call was skipped because a failure has already been
    *       reported
    */
   protected final boolean callbackFailure(Throwable th) {
      if (failed) {
         return false;
      }
      failed = true;
      callback.onFailure(th);
      return true;
   }
   
   @Override
   public void onSimpleReply(String string) {
      callbackFailure(new RedisUnexpectedResponseException(Response.simpleResponse(string)));
   }

   @Override
   public void onBulkReply(ByteBuf bytes) {
      callbackFailure(new RedisUnexpectedResponseException(Response.bulkResponse(bytes)));
   }

   @Override
   public void onErrorReply(String errorMsg) {
      callbackFailure(new RedisException(errorMsg));
   }

   @Override
   public void onIntegerReply(long value) {
      callbackFailure(new RedisUnexpectedResponseException(Response.intResponse(value)));
   }

   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      // accumulate array elements in order to report the actual response in the exception
      if (numberOfElements < 0) {
         callbackFailure(new RedisUnexpectedResponseException(Response.nilArrayResponse()));
         return ReplyListener.NO_OP;
      } else if (numberOfElements == 0) {
         callbackFailure(new RedisUnexpectedResponseException(Response.emptyArrayResponse()));
         return ReplyListener.NO_OP;
      }
      Response[] responses = new Response[numberOfElements];
      return new ResponseListener(new Callback<Response>() {
         int index = 0;
         
         @Override
         public void onSuccess(Response t) {
            responses[index++] = t;
            if (index == numberOfElements) {
               callbackFailure(
                     new RedisUnexpectedResponseException(Response.arrayResponse(responses)));
            }
         }

         @Override
         public void onFailure(Throwable th) {
            callbackFailure(th);
         }
      });
   }

   @Override
   public void onFailure(Throwable th) {
      callbackFailure(th);
   }
}