package com.bluegosling.redis.concurrent;

import static com.bluegosling.redis.MoreAsserts.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.bluegosling.redis.MoreAsserts.CheckedRunnable;
import com.bluegosling.redis.concurrent.SizedIterator;
import com.bluegosling.redis.concurrent.SizedObserver;
import com.bluegosling.redis.concurrent.StreamingIterator;
import com.google.common.util.concurrent.Uninterruptibles;

public class StreamingIteratorTest {
   
   SizedIterator<String> iter;
   SizedObserver<String> observer;
   
   @Before public void setup() {
      StreamingIterator<String> iter = new StreamingIterator<>();
      this.observer = iter.asObserver();
      this.iter = iter;
   }
   
   /*
    * Many of these tests are extremely similar permutations. Each Iterator method may be in a
    * state where it has the information it needs to return immediately *OR* it must block for
    * emission of elements or notices via the observer. So we test each scenario multiple times with
    * a different method invoked first. The first method must block for an action from the observer,
    * and the subsequent ones do not.
    */
   
   @Test public void hasNext_empty_withInitialize() {
      delay(() -> observer.initialize(0)); // don't even need onFinish() since we see size = 0
      assertFalse(iter.hasNext());
      
      assertThrows(NoSuchElementException.class, () -> iter.next());
      assertEquals(0, iter.remainingItems().getAsInt());
   }

   @Test public void hasNext_empty_withoutInitialize() {
      delay(() -> observer.onFinish());
      assertFalse(iter.hasNext());

      assertThrows(NoSuchElementException.class, () -> iter.next());
      assertEquals(0, iter.remainingItems().getAsInt());
   }

   @Test public void hasNext_notEmpty_withInitialize() {
      delay(() -> observer.initialize(5));
      assertTrue(iter.hasNext());
      
      assertEquals(5, iter.remainingItems().getAsInt());
   }

   @Test public void hasNext_notEmpty_withoutInitialize() {
      delay(() -> observer.onNext("foo"));
      assertTrue(iter.hasNext());
      
      assertFalse(iter.remainingItems().isPresent());
   }

   @Test public void remainingItems_empty_withInitialize() {
      delay(() -> observer.initialize(0)); // don't even need onFinish() to get size
      assertEquals(0, iter.remainingItems().getAsInt());
   }

   @Test public void remainingItems_empty_withoutInitialize() {
      delay(() -> observer.onFinish());
      assertEquals(0, iter.remainingItems().getAsInt());
   }

   @Test public void remainingItems_notEmpty_withInitialize() {
      delay(() -> observer.initialize(5));
      assertEquals(5, iter.remainingItems().getAsInt());

      assertTrue(iter.hasNext());
   }

   @Test public void remainingItems_notEmpty_withoutInitialize() {
      delay(() -> observer.onNext("foo"));
      assertFalse(iter.remainingItems().isPresent());

      assertTrue(iter.hasNext());
   }
   
   @Test public void next_empty_withInitialize() {
      delay(() -> observer.initialize(0)); // don't even need onFinish() since we see size = 0
      assertThrows(NoSuchElementException.class, () -> iter.next());

      assertFalse(iter.hasNext());
      assertEquals(0, iter.remainingItems().getAsInt());
   }

   @Test public void next_empty_withoutInitialize() {
      delay(() -> observer.onFinish());
      assertThrows(NoSuchElementException.class, () -> iter.next());

      assertFalse(iter.hasNext());
      assertEquals(0, iter.remainingItems().getAsInt());
   }

   @Test public void next_notEmpty_withInitialize() {
      delay(() -> {
         observer.initialize(5);
         observer.onNext("abc");
         observer.onNext("def");
         observer.onNext("ghi");
         observer.onNext("jkl");
         observer.onNext("mno");
         // don't actually require onFinish() since we know how many items to expect
      });
      assertEquals("abc", iter.next());
      assertTrue(iter.hasNext());
      assertEquals(4, iter.remainingItems().getAsInt());

      assertEquals("def", iter.next());
      assertTrue(iter.hasNext());
      assertEquals(3, iter.remainingItems().getAsInt());

      assertEquals("ghi", iter.next());
      assertTrue(iter.hasNext());
      assertEquals(2, iter.remainingItems().getAsInt());
   
      assertEquals("jkl", iter.next());
      assertTrue(iter.hasNext());
      assertEquals(1, iter.remainingItems().getAsInt());

      assertEquals("mno", iter.next());
      assertFalse(iter.hasNext());
      assertEquals(0, iter.remainingItems().getAsInt());

      assertThrows(NoSuchElementException.class, () -> iter.next());
   }

   @Test public void next_notEmpty_withoutInitialize() throws Exception {
      CountDownLatch doneEmitting = new CountDownLatch(1);
      CountDownLatch waitingToFinish = new CountDownLatch(1);
      CountDownLatch finished = new CountDownLatch(1);
      delay(() -> {
         observer.onNext("abc");
         observer.onNext("def");
         observer.onNext("ghi");
         observer.onNext("jkl");
         observer.onNext("mno");
         
         // we want more deterministic control over when onFinish() is
         // called to test some assertions properly
         doneEmitting.countDown();
         Uninterruptibles.awaitUninterruptibly(waitingToFinish);
         observer.onFinish();
         finished.countDown();
      });
      assertEquals("abc", iter.next());
      assertTrue(iter.hasNext());
      assertFalse(iter.remainingItems().isPresent());

      assertEquals("def", iter.next());
      assertTrue(iter.hasNext());
      assertFalse(iter.remainingItems().isPresent());

      assertEquals("ghi", iter.next());
      assertTrue(iter.hasNext());
      assertFalse(iter.remainingItems().isPresent());
      
      // let observer call onFinish(), at which point we know how many
      // items remain (based on items emitted and still in queue)
      doneEmitting.await();
      waitingToFinish.countDown();
      finished.await();
   
      assertEquals("jkl", iter.next());
      assertTrue(iter.hasNext());
      assertEquals(1, iter.remainingItems().getAsInt());

      assertEquals("mno", iter.next());
      assertFalse(iter.hasNext());
      assertEquals(0, iter.remainingItems().getAsInt());

      assertThrows(NoSuchElementException.class, () -> iter.next());
   }
   
   @Test public void hasNext_onFailure() {
      RuntimeException ex = new RuntimeException();
      delay(() -> observer.onFailure(ex));
      RuntimeException thrown = assertThrows(RuntimeException.class, () -> iter.hasNext());
      assertSame(ex, thrown);
      
      thrown = assertThrows(RuntimeException.class, () -> iter.next());
      assertSame(ex, thrown);
      assertFalse(iter.remainingItems().isPresent());
   }

   @Test public void next_onFailure() {
      RuntimeException ex = new RuntimeException();
      delay(() -> observer.onFailure(ex));
      RuntimeException thrown = assertThrows(RuntimeException.class, () -> iter.next());
      assertSame(ex, thrown);
      
      thrown = assertThrows(RuntimeException.class, () -> iter.hasNext());
      assertSame(ex, thrown);
      assertFalse(iter.remainingItems().isPresent());
   }

   @Test public void remainingItems_onFailure_withInitialize() {
      RuntimeException ex = new RuntimeException();
      delay(() -> {
         observer.initialize(10);
         observer.onFailure(ex);
      });
      
      // we don't propagate failures from remainingItems, only from next and hasNext
      assertEquals(10, iter.remainingItems().getAsInt());
   }

   @Test public void remainingItems_onFailure_withoutInitialize() {
      RuntimeException ex = new RuntimeException();
      delay(() -> observer.onFailure(ex));
      
      // we don't propagate failures from remainingItems, only from next and hasNext
      assertFalse(iter.remainingItems().isPresent());
   }

   @Test public void failureIgnoredIfAfterLastElement() throws Exception {
      RuntimeException ex = new RuntimeException();
      CountDownLatch latch = new CountDownLatch(1);
      delay(() -> {
         observer.initialize(1);
         observer.onNext("foo");
         observer.onFailure(ex);
         latch.countDown();
      });
      assertEquals("foo", iter.next());
      latch.await();
      // failure is not propagated because iterator got all expected elements prior to exception
      assertFalse(iter.hasNext());
      assertThrows(NoSuchElementException.class, () -> iter.next());
   }

   @Test public void asObserver_cannotInitializeTwice() {
      observer.initialize(1);
      assertThrows(IllegalStateException.class, () -> observer.initialize(1));
   }

   @Test public void asObserver_cannotInitializeAfterStart() {
      observer.onNext("foo");
      assertThrows(IllegalStateException.class, () -> observer.initialize(1));
   }

   @Test public void asObserver_noMethodsValidAfterFinish() {
      List<CheckedRunnable> methodInvocations = Arrays.asList(
            () -> observer.initialize(1),
            () -> observer.onNext("foo"),
            () -> observer.onFinish(),
            () -> observer.onFailure(new Throwable()));
      for (CheckedRunnable r : methodInvocations) {
         setup();
         observer.onFinish();
         assertThrows(IllegalStateException.class, r);
      }
   }

   @Test public void asObserver_noMethodsValidAfterFailure() {
      List<CheckedRunnable> methodInvocations = Arrays.asList(
            () -> observer.initialize(1),
            () -> observer.onNext("foo"),
            () -> observer.onFinish(),
            () -> observer.onFailure(new Throwable()));
      for (CheckedRunnable r : methodInvocations) {
         setup();
         observer.onFailure(new Throwable());
         assertThrows(IllegalStateException.class, r);
      }
   }

   private void delay(Runnable r) {
      new Thread(() -> {
         // delay execution
         Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
         r.run();
      }).start();
   }
}
