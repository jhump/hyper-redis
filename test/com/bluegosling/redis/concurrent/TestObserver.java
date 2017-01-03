package com.bluegosling.redis.concurrent;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.bluegosling.redis.concurrent.SizedObserver;

/**
 * A test observer that tracks all method calls to verify that it is used correctly. It also
 * accumulates all emissions in a list that can be queried via {@link #emittedItems()}.
 * 
 * <p>All test observers created are tracked so that tests can easily verify that they didn't break
 * the observer contract using {@link TestObserver#verifyUsage()}. (Calling this method also resets
 * state for the observers used so far, so that subsequent misuse can be attributed to the correct
 * test.) This uses static state, so tests that use this facility should not be run concurrently in
 * the same JVM (unless using multiple class loaders to isolate this static state).
 * 
 * <p>If a test observer is unsubscribed before a stream finished, it could get a finish notice
 * before all elements were emitted. This could look like improper use in that an observer could
 * have been initialized with some count of elements but then sent fewer due to the subscription
 * being interrupted. To avoid exceptions from {@link TestObserver#verifyUsage()} in this scenario,
 * tests should call {@link TestObserver#unsubscribed()}, so verification can be relaxed
 * accordingly.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of elements observed
 */
class TestObserver<T> implements SizedObserver<T> {
   private static final List<TestObserver<?>> observers = new ArrayList<>();
   
   static void verifyUsage() {
      List<TestObserver<?>> list;
      synchronized (observers) {
         list = new ArrayList<>(observers);
         observers.clear();
      }
      for (TestObserver<?> o : list) {
         o.verify();
      }
   }
   
   private static void register(TestObserver<?> observer) {
      synchronized (observers) {
         observers.add(observer);
      }
   }
   
   private final List<T> items = new ArrayList<>();
   private int itemsExpected = -1;
   private boolean finished;
   private Throwable failure;
   private boolean unsubscribed;
   private boolean usedAfterFinishOrFailure;
   private boolean initializedAfterStartOrInit;
   
   TestObserver() {
      register(this);
   }
   
   private void verify() {
      assertFalse("Methods called after onFinish() or onFailure()", usedAfterFinishOrFailure);
      assertFalse("initialize() called more than one or after items emitted",
            initializedAfterStartOrInit);
      if (itemsExpected >= 0) {
         if (finished && !unsubscribed) {
            assertTrue(
                  "Wrong number of items received: expected " + itemsExpected + " but got "
                        + items.size(),
                  itemsExpected == items.size());
         } else {
            assertTrue(
                  "Too many items received: expected up to " + itemsExpected + " but got "
                        + items.size(),
                  itemsExpected >= items.size());
         }
      }
   }
   
   synchronized boolean unsubscribed() {
      if (finished || unsubscribed) {
         return false;
      }
      unsubscribed = true;
      return true;
   }
   
   synchronized boolean isUnsubscribed() {
      return unsubscribed;
   }
   
   synchronized boolean isFinished() {
      return finished;
   }
   
   synchronized boolean isSuccessful() {
      return finished && failure == null;
   }

   synchronized boolean isFailure() {
      return finished && failure != null;
   }

   synchronized Throwable failure() {
      return failure;
   }
   
   synchronized List<T> emittedItems() {
      return Collections.unmodifiableList(items);
   }
   
   @Override
   public synchronized void initialize(int numberOfElements) {
      if (itemsExpected != -1 || !items.isEmpty()) {
         initializedAfterStartOrInit = true;
      }
      if (finished) {
         usedAfterFinishOrFailure = true;
      }
      itemsExpected = numberOfElements;
   }

   @Override
   public synchronized void onNext(T element) {
      if (finished) {
         usedAfterFinishOrFailure = true;
      }
      items.add(element);
   }
   
   @Override
   public synchronized void onFinish() {
      if (finished) {
         usedAfterFinishOrFailure = true;
      }
      finished = true;
   }

   @Override
   public synchronized void onFailure(Throwable failure) {
      if (finished) {
         usedAfterFinishOrFailure = true;
      }
      finished = true;
      this.failure = failure;
   }
}