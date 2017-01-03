package com.bluegosling.redis.protocol;

import com.bluegosling.redis.Marshaller;
import com.bluegosling.redis.RedisException;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.concurrent.Observer;
import com.bluegosling.redis.concurrent.SizedObserver;
import com.bluegosling.redis.values.Response;

import io.netty.buffer.ByteBuf;

public class ObserverListener<T> implements ReplyListener {
   private final Observer<T> observer;
   private final Marshaller<T> marshaller;
   private int elementsRemaining = -1;
   private boolean done;

   protected ObserverListener(Observer<T> observer, Marshaller<T> marshaller) {
      this.observer = observer;
      this.marshaller = marshaller;
   }

   @Override
   public void onSimpleReply(String reply) {
      if (!done) {
         done = true;
         observer.onFailure(
               new RedisUnexpectedResponseException(Response.simpleResponse(reply)));
      }
   }

   @Override
   public void onErrorReply(String errorMsg) {
      if (!done) {
         done = true;
         observer.onFailure(new RedisException(errorMsg));
      }
   }

   @Override
   public void onIntegerReply(long value) {
      if (!done) {
         done = true;
         observer.onFailure(
               new RedisUnexpectedResponseException(Response.intResponse(value)));
      }
   }

   @Override
   public void onBulkReply(ByteBuf bytes) {
      if (done) {
         return;
      } else if (elementsRemaining >= 0) {
         T value;
         try {
            value = marshaller.fromBytes(bytes);
         } catch (Throwable th) {
            done = true;
            observer.onFailure(th);
            return;
         }
         observer.onNext(value);
         if (--elementsRemaining == 0) {
            observer.onFinish();
         }
      } else {
         done = true;
         observer.onFailure(
               new RedisUnexpectedResponseException(Response.bulkResponse(bytes)));
      }
   }

   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      if (done) {
         return NO_OP;
      } else if (elementsRemaining == -1) {
         if (numberOfElements < 0) {
            done = true;
            observer.onFailure(new RedisUnexpectedResponseException(Response.nilArrayResponse()));
            return NO_OP;
         } else if (numberOfElements == 0) {
            done = true;
            observer.onFinish();
            return NO_OP;
         } else {
            elementsRemaining = numberOfElements;
            if (observer instanceof SizedObserver) {
               ((SizedObserver<T>) observer).initialize(numberOfElements);
            }
            return this;
         }
      } else {
         // accumulate array elements in order to report the actual response in the exception
         done = true;
         if (numberOfElements < 0) {
            observer.onFailure(
                  new RedisUnexpectedResponseException(Response.nilArrayResponse()));
            return NO_OP;
         } else if (numberOfElements == 0) {
            observer.onFailure(
                  new RedisUnexpectedResponseException(Response.emptyArrayResponse()));
            return NO_OP;
         }
         Response[] responses = new Response[numberOfElements];
         return new ResponseListener(new Callback<Response>() {
            int index = 0;
            
            @Override
            public void onSuccess(Response t) {
               responses[index++] = t;
               if (index == numberOfElements) {
                  observer.onFailure(
                        new RedisUnexpectedResponseException(Response.arrayResponse(responses)));
               }
            }

            @Override
            public void onFailure(Throwable th) {
               observer.onFailure(th);
            }
         });
      }
   }
   
   @Override
   public void onFailure(Throwable th) {
      if (!done) {
         done = true;
         observer.onFailure(th);
      }
   }
}
