package com.bluegosling.redis.concurrent;

import java.util.concurrent.CancellationException;

/**
 * Provides details about and ability to interact with a subscription. This is returned when an
 * observer is {@linkplain Observable#subscribe(Observer) subscribed} to an observable.
 * 
 * <p>This interface is used with asynchronous streaming. For interfaces and classes related to
 * Redis pub/sub, see {@link com.bluegosling.redis.pubsub}.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public interface Subscription {
   /**
    * Returns the number of missed messages. These are messages that were emitted by an
    * observable with a fixed-size cache before the observer was subscribed to it. If the
    * observable emits more elements than fit in the fixed size cache, later subscribers will
    * miss the earlier elements.
    * 
    * <p>Note that even an observable with a fixed-size cache will fully cache items until the
    * first {@linkplain Observable#subscribe(Observer) subscription}. If it is critical that no
    * messages be missed and you need multiple observers subscribed to the same observable, use
    * {@link Observable#cacheFully()} and subscribe observers to that.
    * 
    * @return the number of missed messages
    */
   int numberOfMissedMessages();
   
   /**
    * Returns true if this subscription is finished. The subscription ends when any of the
    * following events occurs:
    * <ul>
    * <li>The observable finishes (successfully or unsuccessfully).
    * <li>The observer is {@linkplain #unsubscribe() unsubscribed}.
    * <li>This subscription is {@linkplain #cancel() cancelled}.
    * </ul>
    * 
    * @return true if this subscription is finished
    */
   boolean isFinished();
   
   /**
    * Unsubscribes the observer associated with this subscription. The observer will receive an
    * {@link Observer#onFinish()} notification.
    * 
    * @return true if the observer is unsubscribed or false if it could not be unsubscribed
    *       because the subscription is already finished
    */
   boolean unsubscribe();
   
   /**
    * Returns true if this subscription was finished via a call to {@link #unsubscribe()}.
    * 
    * @return true if this subscription was finished via a call to {@link #unsubscribe()}
    */
   boolean isUnsubscribed();

   /**
    * Cancels this subscription. The observer will receive an
    * {@link Observer#onFailure(Throwable)} notification with a {@link CancellationException}.
    * 
    * @return true if the observer is cancelled or false if it could not be cancelled
    *       because the subscription is already finished
    */
   boolean cancel();

   /**
    * Returns true if this subscription was finished via a call to {@link #cancel()}.
    * 
    * @return true if this subscription was finished via a call to {@link #cancel()}
    */
   boolean isCancelled();
}