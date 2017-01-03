package com.bluegosling.redis.concurrent;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executor;

/**
 * Wraps another executor, ensuring that tasks are run serially. The underlying executor, for
 * example, could be a thread pool and tasks submitted to it could run in parallel. This wrapper
 * ensures that all tasks submitted to it are instead run sequentially. Multiple such wrappers could
 * be created on top of the same thread pool to achieve parallelism while still retaining this
 * serial constraint per wrapper.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 */
public class SerialExecutor implements Executor {
   private final Executor underlying;
   private Queue<Runnable> tasks;
   
   public SerialExecutor(Executor underlying) {
      this.underlying = underlying;
   }

   @Override
   public synchronized void execute(Runnable command) {
      if (tasks != null) {
         tasks.add(command);
      } else {
         tasks = new LinkedList<>();
         tasks.add(command);
         underlying.execute(this::processTasks);
      }
   }
   
   private void processTasks() {
      while (true) {
         Runnable r;
         synchronized (this) {
            if (tasks == null) {
               return;
            }
            if (tasks.isEmpty()) {
               tasks = null;
               return;
            }
            r = tasks.poll();
         }
         try {
            r.run();
            synchronized (this) {
               if (tasks.isEmpty()) {
                  tasks = null;
                  return;
               }
            }
         } catch (RuntimeException | Error e) {
            synchronized (this) {
               if (tasks != null) {
                  underlying.execute(this::processTasks);
               }
            }
            throw e;
         }
      }
   }
}
