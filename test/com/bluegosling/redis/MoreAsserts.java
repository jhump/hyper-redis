package com.bluegosling.redis;

import static org.junit.Assert.fail;

import com.google.common.base.Throwables;

public final class MoreAsserts {
   private MoreAsserts() { throw new AssertionError(); }
   
   public interface CheckedRunnable {
      void run() throws Throwable;
   }
   
   public static <T extends Throwable> T assertThrows(Class<T> type, CheckedRunnable task) {
      try {
         task.run();
         fail("Expecting " + type.getName() + " but nothing thrown");
         throw new AssertionError(); // make compiler happy that we always throw or return
      } catch (Throwable th) {
         if (!type.isInstance(th)) {
            throw Throwables.propagate(th);
         }
         return type.cast(th);
      }
   }
}
