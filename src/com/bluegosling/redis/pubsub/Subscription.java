package com.bluegosling.redis.pubsub;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

public class Subscription {
   private final SubscriptionController<?> controller;
   private final Subscriber<?> subscriber; 
   private final String topic;
   private final Executor executor;
   private final AtomicLong messages = new AtomicLong();
   
   Subscription(SubscriptionController<?> controller, Subscriber<?> subscriber, String topic,
         Executor executor) {
      this.controller = controller;
      this.subscriber = subscriber;
      this.topic = topic;
      this.executor = executor;
   }

   public void unsubscribe() {
      controller.unsubscribe(topic, subscriber);
   }
   
   public boolean isUnsubscribed() {
      return controller.isSubscribed(topic, subscriber);
   }
   
   public long messagesDelivered() {
      return messages.get();
   }

   public String topic() {
      return topic;
   }
   
   public Subscriber<?> subscriber() {
      return subscriber;
   }
   
   void newMessage() {
      messages.incrementAndGet();
   }
   
   void execute(Runnable r) {
      executor.execute(r);
   }
}
