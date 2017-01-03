package com.bluegosling.redis.pubsub;

import java.util.function.Consumer;

public interface Subscriber<M> {
   void onSubscribe(String topic);
   void onSubscriptionFailure(String topic, Throwable th);
   void onMessage(String topic, M message);
   void onUnsubscribe(String topic);

   static <M> Subscriber<M> of(Consumer<? super M> consumer) {
      return new Subscriber<M>() {
         @Override
         public void onSubscribe(String topic) {
         }

         @Override
         public void onSubscriptionFailure(String topic, Throwable th) {
         }

         @Override
         public void onMessage(String topic, M message) {
            consumer.accept(message);
         }

         @Override
         public void onUnsubscribe(String topic) {
         }
      };
   }

   static <M> Subscriber<M> of(Consumer<? super M> consumer, Consumer<? super Throwable> onError) {
      return new Subscriber<M>() {
         @Override
         public void onSubscribe(String topic) {
         }

         @Override
         public void onSubscriptionFailure(String topic, Throwable th) {
            onError.accept(th);
         }

         @Override
         public void onMessage(String topic, M message) {
            consumer.accept(message);
         }

         @Override
         public void onUnsubscribe(String topic) {
         }
      };
   }
}
