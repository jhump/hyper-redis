package com.bluegosling.redis.pubsub;

import java.util.function.Consumer;

public interface PSubscriber<M> {
   void onSubscribe(String pattern);
   void onSubscriptionFailure(String pattern, Throwable th);
   void onMessage(String pattern, String topic, M message);
   void onUnsubscribe(String pattern);
   
   static <M> PSubscriber<M> of(Consumer<? super M> consumer) {
      return new PSubscriber<M>() {
         @Override
         public void onSubscribe(String pattern) {
         }

         @Override
         public void onSubscriptionFailure(String pattern, Throwable th) {
         }

         @Override
         public void onMessage(String pattern, String topic, M message) {
            consumer.accept(message);
         }

         @Override
         public void onUnsubscribe(String pattern) {
         }
      };
   }

   static <M> PSubscriber<M> of(Consumer<? super M> consumer, Consumer<? super Throwable> onError) {
      return new PSubscriber<M>() {
         @Override
         public void onSubscribe(String pattern) {
         }

         @Override
         public void onSubscriptionFailure(String pattern, Throwable th) {
            onError.accept(th);
         }

         @Override
         public void onMessage(String pattern, String topic, M message) {
            consumer.accept(message);
         }

         @Override
         public void onUnsubscribe(String pattern) {
         }
      };
   }
}
