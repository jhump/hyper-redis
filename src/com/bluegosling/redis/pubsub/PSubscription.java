package com.bluegosling.redis.pubsub;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class PSubscription {
   private final SubscriptionController<?> controller;
   private final PSubscriber<?> subscriber;
   private final String pattern;
   private final Executor executor;
   private final AtomicLong messages = new AtomicLong();
   private final ConcurrentMap<String, LongAdder> topics = new ConcurrentHashMap<>(); 
   
   PSubscription(SubscriptionController<?> controller, PSubscriber<?> subscriber, String pattern,
         Executor executor) {
      this.controller = controller;
      this.subscriber = subscriber;
      this.pattern = pattern;
      this.executor = executor;
   }
   
   public void unsubscribe() {
      controller.unsubscribe(pattern, subscriber);
   }
   
   public boolean isUnsubscribed() {
      return controller.isSubscribed(pattern, subscriber);
   }
   
   public long messagesDelivered() {
      return messages.get();
   }

   public String pattern() {
      return pattern;
   }
   
   public PSubscriber<?> subscriber() {
      return subscriber;
   }

   public Set<String> topicsObserved() {
      return Collections.unmodifiableSet(topics.keySet());
   }

   public long messagesDeliveredForTopic(String topic) {
      LongAdder a = topics.get(topic);
      return a == null ? 0 : a.sum();
   }
   
   void newMessage(String topic) {
      messages.incrementAndGet();
      topics.computeIfAbsent(topic, t -> new LongAdder()).increment();
   }
   
   void execute(Runnable r) {
      executor.execute(r);
   }
}
