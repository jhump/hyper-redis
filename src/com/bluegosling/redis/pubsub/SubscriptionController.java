package com.bluegosling.redis.pubsub;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.StampedLock;

import com.bluegosling.redis.Marshaller;
import com.bluegosling.redis.channel.RedisChannel;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.concurrent.SerialExecutor;
import com.bluegosling.redis.protocol.BaseReplyListener;
import com.bluegosling.redis.protocol.RedisResponseException;
import com.bluegosling.redis.protocol.ReplyListener;
import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;

public class SubscriptionController<M> {
   private final RedisChannel channel;
   private final Executor executor;
   private final ReplyListener replyListener;
   private final StampedLock lock = new StampedLock();
   private final Map<String, SubscriberSet<Subscriber<? super M>, Subscription>> subscribers =
         new HashMap<>();
   private final Map<String, SubscriberSet<PSubscriber<? super M>, PSubscription>> psubscribers =
         new HashMap<>();
   private RedisChannel.SubscriberChannel currentSubscribedChannel;
   
   SubscriptionController(RedisChannel channel, Marshaller<M> messageMarshaller,
         Executor executor) {
      this.channel = requireNonNull(channel);
      this.executor = requireNonNull(executor);
      this.replyListener =
            new EventListener<>(Callback.of(this::onEvent, this::onFailure), messageMarshaller);
   }
   
   public Subscription subscribe(String topic, Subscriber<? super M> subscriber) {
      long stamp = lock.writeLock();
      try {
         SubscriberSet<Subscriber<? super M>, Subscription> set = subscribers.get(topic);
         if (set == null) {
            set = new SubscriberSet<>();
            subscribers.put(topic, set);
            if (currentSubscribedChannel == null) {
               currentSubscribedChannel = channel.asSubscriberChannel(replyListener);
            }
            currentSubscribedChannel.newCommand()
                  .nextToken("SUBSCRIBE")
                  .nextToken(topic)
                  .finish();
         }
         Subscription subscription = set.get(subscriber);
         if (subscription == null) {
            subscription = new Subscription(this, subscriber, topic, new SerialExecutor(executor));
            set.put(subscriber, subscription);
            if (set.initialized) {
               subscription.execute(() -> subscriber.onSubscribe(topic));
            }
         }
         return subscription;
      } finally {
         lock.unlockWrite(stamp);
      }
   }

   public PSubscription psubscribe(String pattern, PSubscriber<? super M> subscriber) {
      long stamp = lock.writeLock();
      try {
         SubscriberSet<PSubscriber<? super M>, PSubscription> pset = psubscribers.get(pattern);
         if (pset == null) {
            pset = new SubscriberSet<>();
            psubscribers.put(pattern, pset);
            if (currentSubscribedChannel == null) {
               currentSubscribedChannel = channel.asSubscriberChannel(replyListener);
            }
            currentSubscribedChannel.newCommand()
                  .nextToken("PSUBSCRIBE")
                  .nextToken(pattern)
                  .finish();
         }
         PSubscription subscription = pset.get(subscriber);
         if (subscription == null) {
            subscription = new PSubscription(this, subscriber, pattern,
                  new SerialExecutor(executor));
            pset.put(subscriber, subscription);
            if (pset.initialized) {
               subscription.execute(() -> subscriber.onSubscribe(pattern));
            }
         }
         return subscription;
      } finally {
         lock.unlockWrite(stamp);
      }
   }
   
   public boolean unsubscribe(String topic) {
      long stamp = lock.writeLock();
      try {
         SubscriberSet<Subscriber<? super M>, Subscription> set = subscribers.remove(topic);
         if (set == null || set.isEmpty()) {
            return false;
         }
         assert currentSubscribedChannel != null;
         currentSubscribedChannel.newCommand()
               .nextToken("UNSUBSCRIBE")
               .nextToken(topic)
               .finish();
         for (Entry<Subscriber<? super M>, Subscription> entry : set.entrySet()) {
            Subscriber<? super M> subscriber = entry.getKey();
            Subscription subscription = entry.getValue();
            subscription.execute(() -> subscriber.onUnsubscribe(topic));
         }
         return true;
      } finally {
         lock.unlockWrite(stamp);
      }
   }
   
   public boolean unsubscribe() {
      // TODO
      return false;
   }

   public boolean punsubscribe(String pattern) {
      long stamp = lock.writeLock();
      try {
         SubscriberSet<PSubscriber<? super M>, PSubscription> pset = psubscribers.remove(pattern);
         if (pset == null || pset.isEmpty()) {
            return false;
         }
         assert currentSubscribedChannel != null;
         currentSubscribedChannel.newCommand()
               .nextToken("PUNSUBSCRIBE")
               .nextToken(pattern)
               .finish();
         for (Entry<PSubscriber<? super M>, PSubscription> entry : pset.entrySet()) {
            PSubscriber<? super M> subscriber = entry.getKey();
            PSubscription subscription = entry.getValue();
            subscription.execute(() -> subscriber.onUnsubscribe(pattern));
         }
         return true;
      } finally {
         lock.unlockWrite(stamp);
      }
   }

   public boolean punsubscribe() {
      // TODO
      return false;
   }
   
   public boolean unsubscribeAllTopicsAndPatterns() {
      boolean ret = unsubscribe();
      ret |= punsubscribe();
      return ret;
   }
   
   public void ping(Callback<String> callback) {
      // TODO
   }

   public void ping(String message, Callback<String> callback) {
      // TODO
   }

   public int subscribedTopics() {
      long stamp = lock.tryOptimisticRead();
      if (stamp != 0) {
         int ret = subscribers.size();
         if (lock.validate(stamp)) {
            return ret;
         }
      }
      stamp = lock.readLock();
      try {
         return subscribers.size();
      } finally {
         lock.unlockRead(stamp);
      }
   }

   public int subscribedPatterns() {
      long stamp = lock.tryOptimisticRead();
      if (stamp != 0) {
         int ret = psubscribers.size();
         if (lock.validate(stamp)) {
            return ret;
         }
      }
      stamp = lock.readLock();
      try {
         return psubscribers.size();
      } finally {
         lock.unlockRead(stamp);
      }
   }
   
   void unsubscribe(String topic, Subscriber<?> subscriber) {
      Subscription s;
      long stamp = lock.writeLock();
      try {
         SubscriberSet<Subscriber<? super M>, Subscription> set = subscribers.get(topic);
         if (set == null) {
            return;
         }
         s = set.remove(subscriber);
         if (set.isEmpty()) {
            subscribers.remove(topic);
            assert currentSubscribedChannel != null;
            currentSubscribedChannel.newCommand()
                  .nextToken("UNSUBSCRIBE")
                  .nextToken(topic)
                  .finish();
         }
      } finally {
         lock.unlockWrite(stamp);
      }
      if (s != null) {
         s.execute(() -> subscriber.onUnsubscribe(topic));
      }
   }
   
   boolean isSubscribed(String topic, Subscriber<?> subscriber) {
      long stamp = lock.tryOptimisticRead();
      if (stamp != 0) {
         Map<Subscriber<? super M>, Subscription> topicMap = subscribers.get(topic);
         boolean ret = topicMap != null && topicMap.containsKey(subscriber);
         if (lock.validate(stamp)) {
            return ret;
         }
      }
      stamp = lock.readLock();
      try {
         Map<Subscriber<? super M>, Subscription> topicMap = subscribers.get(topic);
         return topicMap != null && topicMap.containsKey(subscriber);
      } finally {
         lock.unlockRead(stamp);
      }
   }

   void unsubscribe(String pattern, PSubscriber<?> subscriber) {
      PSubscription ps;
      long stamp = lock.writeLock();
      try {
         SubscriberSet<PSubscriber<? super M>, PSubscription> pset = psubscribers.get(pattern);
         if (pset == null) {
            return;
         }
         ps = pset.remove(subscriber);
         if (pset.isEmpty()) {
            psubscribers.remove(pattern);
            assert currentSubscribedChannel != null;
            currentSubscribedChannel.newCommand()
                  .nextToken("PUNSUBSCRIBE")
                  .nextToken(pattern)
                  .finish();
         }
      } finally {
         lock.unlockWrite(stamp);
      }
      if (ps != null) {
         ps.execute(() -> subscriber.onUnsubscribe(pattern));
      }
   }

   boolean isSubscribed(String pattern, PSubscriber<?> subscriber) {
      long stamp = lock.tryOptimisticRead();
      if (stamp != 0) {
         Map<PSubscriber<? super M>, PSubscription> patternMap = psubscribers.get(pattern);
         boolean ret = patternMap != null && patternMap.containsKey(subscriber);
         if (lock.validate(stamp)) {
            return ret;
         }
      }
      stamp = lock.readLock();
      try {
         Map<PSubscriber<? super M>, PSubscription> patternMap = psubscribers.get(pattern);
         return patternMap != null && patternMap.containsKey(subscriber);
      } finally {
         lock.unlockRead(stamp);
      }
   }
   
   private void onEvent(Event e) {
      long stamp = lock.readLock();
      try {
         if (e.kind.isState) {
            StateChangeEvent sce = (StateChangeEvent) e;
            switch (e.kind) {
            case SUBSCRIBE:
               SubscriberSet<Subscriber<? super M>, Subscription> set =
                     subscribers.get(sce.topicOrPattern);
               if (!set.initialized) {
                  long newStamp = lock.tryConvertToWriteLock(stamp);
                  if (newStamp == 0) {
                     lock.unlockRead(stamp);
                     stamp = lock.writeLock();
                     if (set.initialized) {
                        break;
                     }
                  } else {
                     stamp = newStamp;
                  }
                  for (Entry<Subscriber<? super M>, Subscription> entry : set.entrySet()) {
                     Subscriber<? super M> subscriber = entry.getKey();
                     Subscription subscription = entry.getValue();
                     subscription.execute(() -> subscriber.onSubscribe(sce.topicOrPattern));
                  }
                  set.initialized = true;
               }
               break;
            case PSUBSCRIBE:
               SubscriberSet<PSubscriber<? super M>, PSubscription> pset =
                     psubscribers.get(sce.topicOrPattern);
               if (!pset.initialized) {
                  long newStamp = lock.tryConvertToWriteLock(stamp);
                  if (newStamp == 0) {
                     lock.unlockRead(stamp);
                     stamp = lock.writeLock();
                     if (pset.initialized) {
                        break;
                     }
                  } else {
                     stamp = newStamp;
                  }
                  for (Entry<PSubscriber<? super M>, PSubscription> entry : pset.entrySet()) {
                     PSubscriber<? super M> subscriber = entry.getKey();
                     PSubscription subscription = entry.getValue();
                     subscription.execute(() -> subscriber.onSubscribe(sce.topicOrPattern));
                  }
                  pset.initialized = true;
               }
               break;
            case UNSUBSCRIBE: case PUNSUBSCRIBE:
               if (e.kind == Event.Kind.UNSUBSCRIBE) {
                  // when we sent 'unsubscribe' message, we would have already cleaned this up
                  assert !subscribers.containsKey(sce.topicOrPattern);
               } else {
                  // when we sent 'punsubscribe' message, we would have already cleaned this up
                  assert !psubscribers.containsKey(sce.topicOrPattern);
               }
               if (subscribers.isEmpty() && psubscribers.isEmpty()
                     && currentSubscribedChannel != null) {
                  // no more subscriptions, so subscribed channel can be closed
                  long newStamp = lock.tryConvertToWriteLock(stamp);
                  if (newStamp == 0) {
                     lock.unlockRead(stamp);
                     stamp = lock.writeLock();
                     if (!subscribers.isEmpty() || !psubscribers.isEmpty()
                           || currentSubscribedChannel == null) {
                        break;
                     }
                  } else {
                     stamp = newStamp;
                  }
                  currentSubscribedChannel.close();
                  currentSubscribedChannel = null;
               }
               break;
            default:
               throw new AssertionError();
            }
         } else if (e.kind == Event.Kind.MESSAGE) {
            @SuppressWarnings("unchecked") // we only send messages of the right type
            MessageEvent<M> msg = (MessageEvent<M>) e;
            SubscriberSet<Subscriber<? super M>, Subscription> set = subscribers.get(msg.topic);
            if (set != null) {
               assert set.initialized;
               for (Entry<Subscriber<? super M>, Subscription> entry : set.entrySet()) {
                  Subscriber<? super M> subscriber = entry.getKey();
                  Subscription subscription = entry.getValue();
                  subscription.execute(() -> subscriber.onMessage(msg.topic, msg.message));
               }
            }
         } else if (e.kind == Event.Kind.PMESSAGE) {
            @SuppressWarnings("unchecked") // we only send messages of the right type
            MessageEvent<M> msg = (MessageEvent<M>) e;
            SubscriberSet<PSubscriber<? super M>, PSubscription> pset =
                  psubscribers.get(msg.pattern);
            if (pset != null) {
               assert pset.initialized;
               for (Entry<PSubscriber<? super M>, PSubscription> entry : pset.entrySet()) {
                  PSubscriber<? super M> subscriber = entry.getKey();
                  PSubscription subscription = entry.getValue();
                  subscription.execute(() ->
                        subscriber.onMessage(msg.pattern, msg.topic, msg.message));
               }
            }
         } else {
            throw new AssertionError();
         }
      } finally {
         lock.unlock(stamp);
      }
   }

   private void onFailure(Throwable th) {
      long stamp = lock.writeLock();
      try {
         for (Entry<String, SubscriberSet<Subscriber<? super M>, Subscription>> entry
               : subscribers.entrySet()) {
            String topic = entry.getKey();
            for (Entry<Subscriber<? super M>, Subscription> e : entry.getValue().entrySet()) {
               Subscriber<? super M> subscriber = e.getKey();
               Subscription subscription = e.getValue();
               subscription.execute(() -> subscriber.onSubscriptionFailure(topic, th));
            }
            if (currentSubscribedChannel != null) {
               currentSubscribedChannel.newCommand().nextToken("UNSUBSCRIBE").finish();
            }
         }
         subscribers.clear();
         for (Entry<String, SubscriberSet<PSubscriber<? super M>, PSubscription>> entry
               : psubscribers.entrySet()) {
            String pattern = entry.getKey();
            for (Entry<PSubscriber<? super M>, PSubscription> e : entry.getValue().entrySet()) {
               PSubscriber<? super M> subscriber = e.getKey();
               PSubscription subscription = e.getValue();
               subscription.execute(() -> subscriber.onSubscriptionFailure(pattern, th));
            }
            if (currentSubscribedChannel != null) {
               currentSubscribedChannel.newCommand().nextToken("PUNSUBSCRIBE").finish();
            }
         }
         psubscribers.clear();
         if (currentSubscribedChannel != null) {
            currentSubscribedChannel.close();
            currentSubscribedChannel = null;
         }
      } finally {
         lock.unlockWrite(stamp);
      }
   }
   
   @SuppressWarnings("serial")
   private static class SubscriberSet<S, T> extends LinkedHashMap<S, T> {
      boolean initialized;
   }
   
   private abstract static class Event {
      enum Kind {
         SUBSCRIBE(true),
         PSUBSCRIBE(true),
         MESSAGE(false),
         PMESSAGE(false),
         UNSUBSCRIBE(true),
         PUNSUBSCRIBE(true);
         
         final boolean isState;
         
         Kind(boolean isState) {
            this.isState = isState;
         }
      }
      
      final Kind kind;
      
      Event(Kind kind) {
         this.kind = kind;
      }
   }
   
   private static class StateChangeEvent extends Event {
      final String topicOrPattern;
      
      private StateChangeEvent(Kind kind, String topicOrPattern) {
         super(kind);
         assert kind.isState;
         this.topicOrPattern = topicOrPattern;
      }
   }
   
   private static class MessageEvent<M> extends Event {
      static <M> MessageEvent<M> topicMessage(String topic, M message) {
         return new MessageEvent<>(Kind.MESSAGE, null, topic, message);
      }

      static <M> MessageEvent<M> patternMessage(String pattern, String topic, M message) {
         return new MessageEvent<>(Kind.PMESSAGE, pattern, topic, message);
      }
      
      final String pattern;
      final String topic;
      final M message;
      
      private MessageEvent(Kind kind, String pattern, String topic, M message) {
         super(kind);
         this.pattern = pattern;
         this.topic = topic;
         this.message = message;
      }
   }
   
   private static class EventListener<M> extends BaseReplyListener<Event> {
      private final Marshaller<M> messageMarshaller;
      private int tokensExpected = -1;
      private int tokensReceived;
      private Event.Kind currentKind;
      private String currentString1, currentString2;
      private M currentMessage;
      private boolean skip;
      
      protected EventListener(Callback<Event> callback, Marshaller<M> messageMarshaller) {
         super(callback);
         this.messageMarshaller = messageMarshaller;
      }
      
      private void receivedToken() {
         if (++tokensReceived == tokensExpected) {
            if (!skip) {
               Event e;
               if (currentKind.isState) {
                  e = new StateChangeEvent(currentKind, currentString1);
               } else if (currentKind == Event.Kind.MESSAGE) {
                  e = MessageEvent.topicMessage(currentString1, currentMessage);
               } else if (currentKind == Event.Kind.PMESSAGE) {
                  e = MessageEvent.patternMessage(currentString1, currentString2, currentMessage);
               } else {
                  throw new AssertionError("unknown kind: " + currentKind);
               }
               callbackSuccess(e);
            }
            currentKind = null;
            currentString1 = currentString2 = null;
            currentMessage = null;
            tokensExpected = -1;
            tokensReceived = 0;
            skip = false;
         }
      }
      
      private boolean receivedBadToken() {
         boolean alreadySkipped = skip;
         skip = true;
         receivedToken();
         return !alreadySkipped;
      }

      @Override
      public void onErrorReply(String errorMsg) {
         if (tokensExpected >= 0) {
            // we're processing an event so we don't expect to encounter an error message
            if (receivedBadToken()) {
               super.onErrorReply(errorMsg);
            }
         } else {
            super.onErrorReply(errorMsg);
         }
      }

      @Override
      public ReplyListener onArrayReply(int numberOfElements) {
         if (tokensExpected >= 0) {
            // we're processing an event so we don't expect to encounter a nested array
            if (receivedBadToken()) {
               return super.onArrayReply(numberOfElements);
            } else {
               return ReplyListener.NO_OP;
            }
         }
         tokensExpected = numberOfElements;
         return this;
      }

      @Override
      public void onSimpleReply(String string) {
         if (tokensExpected >= 0) {
            // we're processing an event so we don't expect to encounter a simple string
            if (receivedBadToken()) {
               super.onSimpleReply(string);
            }
         } else {
            super.onSimpleReply(string);
         }
      }

      @Override
      public void onBulkReply(ByteBuf bytes) {
         if (tokensExpected < 0) {
            super.onBulkReply(bytes);
            return;
         }

         if (skip) {
            receivedToken();
            return;
         }
         
         switch (tokensReceived) {
         case 0:
            // first field is kind; use it to validate the number of expected array elements
            try {
               currentKind = Event.Kind.valueOf(bytes.toString(Charsets.UTF_8).toUpperCase());
            } catch (IllegalArgumentException e) {
               if (receivedBadToken()) {
                  super.onBulkReply(bytes);
               }
               return;
            }
            switch (currentKind) {
            case MESSAGE:
               if (tokensExpected != 3) {
                  if (receivedBadToken()) {
                     callbackFailure(new RedisResponseException(
                           "'message' events should have 3 components, not " + tokensExpected));
                  }
                  return;
               }
               break;
            case PMESSAGE:
               if (tokensExpected != 4) {
                  if (receivedBadToken()) {
                     callbackFailure(new RedisResponseException(
                           "'pmessage' events should have 4 components, not " + tokensExpected));
                  }
                  return;
               }
               break;
            default:
               assert currentKind.isState;
               if (tokensExpected != 3) {
                  if (receivedBadToken()) {
                     callbackFailure(
                           new RedisResponseException("'" + currentKind.name().toLowerCase()
                                 + "' events should have 3 components, not " + tokensExpected));
                  }
                  return;
               }
            }
            receivedToken();
            return;
         case 1:
            // second field is either a topic or pattern
            currentString1 = bytes.toString(Charsets.UTF_8);
            receivedToken();
            break;
         case 2:
            // third field is the message for a 'message' event or the topic for a 'pmessage' event
            if (currentKind == Event.Kind.PMESSAGE) {
               currentString2 = bytes.toString(Charsets.UTF_8);
               receivedToken();
            } else if (currentKind == Event.Kind.MESSAGE) {
               currentMessage = messageMarshaller.fromBytes(bytes);
               receivedToken();
            } else {
               // other event types have a numeric third field!
               if (receivedBadToken()) {
                  super.onBulkReply(bytes);
               }
            }
            break;
         case 3:
            // fourth field is the message for a 'pmessage' event
            assert currentKind == Event.Kind.PMESSAGE;
            currentMessage = messageMarshaller.fromBytes(bytes);
            receivedToken();
            break;
         default:
            throw new AssertionError(); // should not be possible
         }
      }

      @Override
      public void onIntegerReply(long value) {
         if (tokensExpected < 0) {
            super.onIntegerReply(value);
            return;
         }

         if (skip) {
            receivedToken();
            return;
         }
         
         if (!currentKind.isState || tokensReceived != 2) {
            if (receivedBadToken()) {
               super.onIntegerReply(value);
               return;
            }
         }
         
         // we don't actually need to do anything with the value, which is the total number of
         // subscribed patterns and topics, since we don't expose it directly and can't really
         // use it internally
         receivedToken();
      }
   }
}
