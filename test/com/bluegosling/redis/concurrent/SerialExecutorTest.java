package com.bluegosling.redis.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bluegosling.redis.concurrent.SerialExecutor;
import com.google.common.util.concurrent.Uninterruptibles;

public class SerialExecutorTest {
   
   private ExecutorService service;
   private SerialExecutor executor;
   
   @Before public void setup() {
      service = Executors.newFixedThreadPool(5);
      executor = new SerialExecutor(service);
   }
   
   @After public void tearDown() {
      service.shutdown();
   }

   @Test public void executesSequentially() throws Exception {
      List<Integer> results = Collections.synchronizedList(new ArrayList<>(100));
      for (int i = 0; i < 100; i++) {
         final int index = i;
         executor.execute(() -> results.add(index));
      }
      // we expect them to execute serially, so results are all in order
      List<Integer> expected = IntStream.range(0, 100).boxed().collect(Collectors.toList());
      assertEquals(expected, results);
   }
   
   @Test public void oneTaskBlocksSuccessors() throws Exception {
      CountDownLatch finish1 = new CountDownLatch(1);
      CountDownLatch started1 = new CountDownLatch(1);
      CountDownLatch started2 = new CountDownLatch(1);
      executor.execute(() -> {
         started1.countDown();
         Uninterruptibles.awaitUninterruptibly(finish1);
      });
      executor.execute(() -> {
         started2.countDown();
      });
      assertTrue(started1.await(100, TimeUnit.MILLISECONDS));
      // can't start until first task completes
      assertFalse(started2.await(500, TimeUnit.MILLISECONDS));
      finish1.countDown();      
      assertTrue(started2.await(100, TimeUnit.MILLISECONDS));
   }
   
   @Test public void taskFailureDoesNotBreakSequentialBehavior() throws Exception {
      List<Throwable> threadFailures = new ArrayList<>();
      
      CountDownLatch finish1 = new CountDownLatch(1);
      CountDownLatch finish2 = new CountDownLatch(1);
      CountDownLatch started2 = new CountDownLatch(1);
      CountDownLatch started3 = new CountDownLatch(1);
      RuntimeException ex = new RuntimeException();
      executor.execute(() -> {
         Thread.currentThread().setUncaughtExceptionHandler(
               (thread, throwable) -> threadFailures.add(throwable));
         Uninterruptibles.awaitUninterruptibly(finish1);
         throw ex;
      });
      executor.execute(() -> {
         started2.countDown();
         Uninterruptibles.awaitUninterruptibly(finish2);
      });
      finish1.countDown();
      assertTrue(started2.await(100, TimeUnit.MILLISECONDS));
      // enqueue after exception -- making sure single running task still
      // accepting items and that both tasks submitted before (second task) and after
      // (third task) run correctly and still in sequential order
      executor.execute(() -> {
         started3.countDown();
      });
      // can't start until second task completes
      assertFalse(started3.await(500, TimeUnit.MILLISECONDS));
      finish2.countDown();      
      assertTrue(started3.await(100, TimeUnit.MILLISECONDS));
      // verify that we let exception bubble up to uncaught exception handler (so we're not
      // trapping and hiding exceptions and an app w/ a custom handler can still be notified of
      // misbehaving tasks, just like with a normal ThreadPoolExecutor)
      assertEquals(Arrays.asList(ex), threadFailures);
   }
}
