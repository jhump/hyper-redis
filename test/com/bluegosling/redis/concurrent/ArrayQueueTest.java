package com.bluegosling.redis.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.bluegosling.redis.concurrent.ArrayQueue;

public class ArrayQueueTest {
   
   private static final Random rnd =  new Random();
   
   private ArrayQueue<String> queue;
   
   @Before public void setup() {
      queue = new ArrayQueue<>(5);
   }
   
   @Test public void offer() {
      assertTrue(queue.offer("foo"));
      checkContents("foo");
      assertTrue(queue.offer("bar"));
      checkContents("foo", "bar");
      assertTrue(queue.offer("baz"));
      checkContents("foo", "bar", "baz");
      assertTrue(queue.offer("snafu"));
      checkContents("foo", "bar", "baz", "snafu");
      assertTrue(queue.offer("frob"));
      checkContents("foo", "bar", "baz", "snafu", "frob");
      assertFalse(queue.offer("nitz"));
      checkContents("foo", "bar", "baz", "snafu", "frob");
   }
   
   @Test public void peekAndPoll() {
      queue.addAll(Arrays.asList("foo", "bar", "baz", "snafu", "frob"));
      assertEquals("foo", queue.peek());
      assertEquals("foo", queue.poll());
      assertEquals("bar", queue.peek());
      assertEquals("bar", queue.poll());
      assertEquals("baz", queue.peek());
      assertEquals("baz", queue.poll());
      assertEquals("snafu", queue.peek());
      assertEquals("snafu", queue.poll());
      assertEquals("frob", queue.peek());
      assertEquals("frob", queue.poll());
      assertTrue(queue.isEmpty());
      assertEquals(null, queue.peek());
      assertEquals(null, queue.poll());
   }
   
   @Test public void clear() {
      queue.addAll(Arrays.asList("foo", "bar", "baz", "snafu", "frob"));
      queue.clear();
      assertTrue(queue.isEmpty());
      assertEquals(null, queue.peek());
      assertEquals(null, queue.poll());
   }
   
   @Test public void circularBuffer() {
      LinkedList<String> bench = new LinkedList<>();
      for (int i = 0; i < 10_000; i++) {
         assert queue.size() < 5;
         for (int j = 0, r = rnd.nextInt(5 - queue.size()) + 1; j < r; j++) {
            String s = nextValue();
            queue.add(s);
            bench.add(s);
            checkContents(bench);
         }
         for (int j = 0, r = rnd.nextInt(queue.size()) + 1; j < r; j++) {
            assertEquals(bench.poll(), queue.poll());
            checkContents(bench);
         }
      }
   }
   
   private String nextValue() {
      int len = rnd.nextInt(5) + 3;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < len; i++) {
         sb.append((char)(rnd.nextInt(26) + 'a'));
      }
      return sb.toString();
   }

   private void checkContents(String... items) {
      checkContents(Arrays.asList(items));
   }

   private void checkContents(List<String> items) {
      assertEquals(items.size(), queue.size());
      int i = 0;
      for (String s : queue) {
         assertEquals(s, items.get(i++));
      }
   }
}
