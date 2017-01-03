package com.bluegosling.redis.concurrent;

import static com.bluegosling.redis.MoreAsserts.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.google.common.util.concurrent.MoreExecutors;

public class ObservableTest {
   
   @After public void tearDown() {
      TestObserver.verifyUsage();
   }
   
   @Test public void elementsCachedUntilFirstSubscription() {
      Observable<Integer> observable = new Observable<>(MoreExecutors.directExecutor());
      TestObserver<Integer> observer = new TestObserver<>();
      
      SizedObserver<Integer> driver = observable.getDriver();
      List<Integer> expected = new ArrayList<>();
      driver.initialize(1000);
      for (int i = 0; i < 1000; i++) {
         driver.onNext(i);
         expected.add(i);
      }
      driver.onFinish();
      
      Subscription sub = observable.subscribe(observer);
      assertEquals(0, sub.numberOfMissedMessages());
      assertEquals(expected, observer.emittedItems());
      
      // subscription is already finished
      assertTrue(observer.isFinished());
      assertTrue(sub.isFinished());
      assertFalse(sub.cancel());
      assertFalse(sub.unsubscribe());
      assertFalse(sub.isCancelled());
      assertFalse(sub.isUnsubscribed());
      
      // default cache size is zero, so adding a subscriber now will get no elements
      observer = new TestObserver<Integer>();
      sub = observable.subscribe(observer);
      assertTrue(observer.isFinished());
      assertTrue(observer.emittedItems().isEmpty());
      assertEquals(1000, sub.numberOfMissedMessages());
      assertTrue(sub.isFinished());
      assertFalse(sub.cancel());
      assertFalse(sub.unsubscribe());
      assertFalse(sub.isCancelled());
      assertFalse(sub.isUnsubscribed());
   }
   
   @Test public void cacheFully() {
      Observable<Integer> observable = new Observable<Integer>(MoreExecutors.directExecutor())
            .cacheFully();
      TestObserver<Integer> observer = new TestObserver<>();
      
      SizedObserver<Integer> driver = observable.getDriver();
      List<Integer> expected = new ArrayList<>();
      driver.initialize(1000);
      for (int i = 0; i < 1000; i++) {
         driver.onNext(i);
         expected.add(i);
      }
      driver.onFinish();
      
      Subscription sub = observable.subscribe(observer);
      assertEquals(0, sub.numberOfMissedMessages());
      assertEquals(expected, observer.emittedItems());
      
      // subscription is already finished
      assertTrue(observer.isFinished());
      assertTrue(sub.isFinished());
      assertFalse(sub.cancel());
      assertFalse(sub.unsubscribe());
      assertFalse(sub.isCancelled());
      assertFalse(sub.isUnsubscribed());
      
      // since we cache fully, new subscriber misses nothing
      observer = new TestObserver<Integer>();
      sub = observable.subscribe(observer);
      assertTrue(observer.isFinished());
      assertEquals(expected, observer.emittedItems());
      assertEquals(0, sub.numberOfMissedMessages());
      assertTrue(sub.isFinished());
      assertFalse(sub.cancel());
      assertFalse(sub.unsubscribe());
      assertFalse(sub.isCancelled());
      assertFalse(sub.isUnsubscribed());
   }
   
   @Test public void asObserver_outOfOrder_initialize() {
      SizedObserver<Integer> o1 =
            new Observable<Integer>(MoreExecutors.directExecutor()).getDriver();
      o1.onNext(0);
      assertThrows(IllegalStateException.class, () -> o1.initialize(100));

      SizedObserver<Integer> o2 =
            new Observable<Integer>(MoreExecutors.directExecutor()).getDriver();
      o2.onFinish();
      assertThrows(IllegalStateException.class, () -> o2.initialize(100));

      SizedObserver<Integer> o3 =
            new Observable<Integer>(MoreExecutors.directExecutor()).getDriver();
      o3.onFailure(new RuntimeException());
      assertThrows(IllegalStateException.class, () -> o3.initialize(100));
   }
   
   @Test public void asObserver_outOfOrder_onNext() {
      SizedObserver<Integer> o2 =
            new Observable<Integer>(MoreExecutors.directExecutor()).getDriver();
      o2.onFinish();
      assertThrows(IllegalStateException.class, () -> o2.onNext(0));

      SizedObserver<Integer> o3 =
            new Observable<Integer>(MoreExecutors.directExecutor()).getDriver();
      o3.onFailure(new RuntimeException());
      assertThrows(IllegalStateException.class, () -> o3.onNext(0));
   }
   
   // TODO: moar tests
}
