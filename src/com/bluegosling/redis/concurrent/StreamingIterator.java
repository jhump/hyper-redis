package com.bluegosling.redis.concurrent;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.OptionalInt;
import java.util.Queue;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * An iterator for fetching items from a streaming response. Unlike a normal iterator, the
 * {@link #hasNext()} and {@link #next()} methods may throw arbitrary (unchecked) exceptions. This
 * happens if the underlying stream fails before all elements are consumed (notified via
 * {@link Observer#onFailure(Throwable)}). If the observer is initialized with the number of
 * elements and the failure happens <em>after</em> all elements have been emitted, then it will
 * <em>not</em> get propagated to callers of {@code hasNext} and {@code next}.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of values emitted by the iterator
 */
class StreamingIterator<T> implements SizedIterator<T> {
   private final Object lock = new Object();
   private final Queue<T> queue = new LinkedList<>();
   private int expectedElements = -1;
   private boolean started;
   private boolean finished;
   private Throwable failure;

   /**
    * Constructs a new iterator. Calls to its methods will block until the iterator's
    * {@link #asObserver() observer} is connected to a source of data.
    */
   public StreamingIterator() {
   }
   
   /**
    * Returns an observer that can be used to feed elements into the iterator. The iterator will
    * stop yielding elements after {@link Observer#onFinish()} or
    * {@link Observer#onFailure(Throwable)} is called. If the underlying source reports the number
    * of expected elements, the iterator will stop yielding after it has emitted that number of
    * elements (even before a notice of end-of-stream is received).
    * 
    * <p>Calls to the iterator's methods will throw an exception if the stream reports a failure
    * via {@link Observer#onFailure(Throwable)}. The cause of failure will be thrown to the caller
    * of the iterator (wrapped in a {@link RuntimeException} if the cause of failure is a checked
    * exception).
    * 
    * @return an observer that connects a stream of data to this iterator
    */
   public SizedObserver<T> asObserver() {
      return new SizedObserver<T>() {
         @Override
         public void initialize(int numberOfElements) {
            synchronized (lock) {
               Preconditions.checkState(expectedElements == -1, "Stream already initialized");
               Preconditions.checkState(!started, "Stream already emitted item(s)");
               Preconditions.checkState(!finished, "Stream already finished");
               expectedElements = numberOfElements;
               // signal possible waiters that were querying for element count
               lock.notifyAll();
            }
         }
         
         @Override
         public void onNext(T element) {
            synchronized (lock) {
               Preconditions.checkState(!finished, "Stream already finished");
               started = true;
               queue.add(element);
               if (expectedElements == -1) {
                  // size not known, so we need to signal possible waiters querying for the count
                  lock.notifyAll();
               } else {
                  assert expectedElements >= queue.size();
                  if (queue.size() == 1) {
                     // became not empty, so signal possible waiter
                     lock.notify();
                  }
               }
            }
         }

         @Override
         public void onFinish() {
            synchronized (lock) {
               Preconditions.checkState(!finished, "Stream already finished");
               finished = true;
               // if we finish short (like on unsubscribe), make sure the iterator methods won't
               // block waiting for elements that aren't available in the queue (since no more
               // are coming)
               expectedElements = queue.size();
               // notify any waiters that stream is at end
               lock.notifyAll();
            }
         }

         @Override
         public void onFailure(Throwable cause) {
            synchronized (lock) {
               Preconditions.checkState(!finished, "Stream already finished");
               failure = cause;
               finished = true;
               // notify any waiters that stream is at end
               lock.notifyAll();
            }
         }
      };
   }

   @Override
   public OptionalInt remainingItems() {
      boolean interrupted = false;
      try {
         synchronized (lock) {
            while (true) {
               if (expectedElements >= 0) {
                  return OptionalInt.of(expectedElements);
               }
               if (started || finished) {
                  return OptionalInt.empty();
               }
               try {
                  lock.wait();
               } catch (InterruptedException e) {
                  interrupted = true;
               }
            }
         }
      } finally {
         if (interrupted) {
            Thread.currentThread().interrupt();
         }
      }
   }

   @Override
   public boolean hasNext() {
      boolean interrupted = false;
      try {
         synchronized (lock) {
            while (true) {
               if (expectedElements >= 0) {
                  return expectedElements > 0;
               }
               if (!queue.isEmpty()) {
                  return true;
               }
               if (failure != null) {
                  throw Throwables.propagate(failure);
               }
               assert !finished;
               try {
                  lock.wait();
               } catch (InterruptedException e) {
                  interrupted = true;
               }
            }
         }
      } finally {
         if (interrupted) {
            Thread.currentThread().interrupt();
         }
      }
   }

   @Override
   public T next() {
      boolean interrupted = false;
      try {
         synchronized (lock) {
            while (true) {
               if (expectedElements == 0) {
                  throw new NoSuchElementException();
               }
               if (!queue.isEmpty()) {
                  assert expectedElements != 0;
                  if (expectedElements > 0) {
                     expectedElements--;
                  }
                  return queue.remove();
               }
               if (failure != null) {
                  throw Throwables.propagate(failure);
               }
               assert !finished;
               
               try {
                  lock.wait();
                  if (queue.size() > 1) {
                     // if multiple items were added, also propagate signal to next waiter to get
                     // the next added element
                     lock.notify();
                  }
               } catch (InterruptedException e) {
                  interrupted = true;
               }
            }
         }
      } finally {
         if (interrupted) {
            Thread.currentThread().interrupt();
         }
      }
   }
}
