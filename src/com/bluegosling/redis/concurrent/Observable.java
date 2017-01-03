package com.bluegosling.redis.concurrent;

import static java.util.Objects.requireNonNull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * An observable, representing the asynchronous emission of zero or more values. An observable is to
 * a {@link Future} as an {@link Iterator} is to a singular value.
 * 
 * <p>This is a light-weight version, not as fully-featured as (for example) the API of the same
 * name in <a href="https://github.com/ReactiveX/RxJava">RxJava</a>. This class provides some
 * similar albeit more basic functionality for streaming operations, omitting many of the numerous
 * ways to compose and interact with streams that RxJava provides. Note that an RxJava
 * {@code Observable} could easily be substituted by adapting a {@code Subject} to the
 * {@link Observer} interface in this package (which is nearly identical to the interface of the
 * same name in RxJava). This class only supports the "hot observable" paradigm (where the results
 * of a Redis operation are the hot source of data).
 * 
 * <p>The API here is strongly influenced by and thus closely follows the API of {@link Stream}.
 * 
 * <p>General use involves creating a subclass that uses the protected {@linkplain #getDriver()
 * driver} to emit items and notices to subscribers. An alternative to creating a custom subclass is
 * to use a {@link Subject}, which provides the same methods for emitting items and notices and has
 * an accessor for an {@link Subject#asObservable() Observable} that represents the resulting
 * stream that is produced.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of elements emitted
 */
public class Observable<T> {
   /** Cache size threshold, below which a fixed size array is used. */
   private static final int ARRAY_QUEUE_THRESHOLD = 8 * 1024;
   
   /**
    * A constant observable that is empty. Any observer subscribed to it will immediately receive
    * an {@link Observer#onFinish()} notice.
    */
   private static final Observable<?> EMPTY = new Observable<Object>() {
      {
         getDriver().onFinish();
      }
   };
   
   private final int cacheLimit;
   private final Executor executor;
   private final Object lock = new Object();
   private final Map<Observer<? super T>, SubscriptionImpl> observers = new LinkedHashMap<>();
   private Queue<T> cache;
   private boolean started;
   private int expectedNumberOfMessages = -1;
   private int discardedMessages;
   private boolean finished;
   private Throwable failure;
   
   /**
    * Constructs a new observable that does not cache any elements and dispatches events to
    * observers using the {@link ForkJoinPool#commonPool()}.
    */
   public Observable() {
      this(0, ForkJoinPool.commonPool());
   }
   
   /**
    * Constructs a new observable that does not cache any elements and dispatches events to
    * observers using the given executor.
    * 
    * @param executor an executor
    */
   public Observable(Executor executor) {
      this(0, executor);
   }
   
   /**
    * Constructs a new observable that caches up to the given number of elements emitted and
    * dispatches events to observers using the {@link ForkJoinPool#commonPool()}.
    * 
    * @param cacheLimit the number of elements that will be buffered, for servicing subscriptions
    *       that begin after the observable has started emitting elements
    * @throws IllegalArgumentException if the given cache size is negative
    */
   public Observable(int cacheSize) {
      this(cacheSize, ForkJoinPool.commonPool());
   }

   /**
    * Constructs a new observable that caches up to the given number of elements emitted and
    * dispatches events to observers using the given executor.
    * 
    * <p>Note: Use of a {@link MoreExecutors#directExecutor()} could block threads that are expected
    * to be reserved for handling async I/O.
    * 
    * @param cacheSize the number of elements that will be buffered, for servicing subscriptions
    *       that begin after the observable has started emitting elements
    * @param executor an executor
    * @throws IllegalArgumentException if the given cache size is negative
    */
   public Observable(int cacheSize, Executor executor) {
      Preconditions.checkArgument(cacheSize >= 0,
               "cache size must be non-negative, instead got %s", cacheSize);
      this.cacheLimit = cacheSize;
      this.executor = executor;
   }
   
   /**
    * Returns an observer that, when invoked, will "drive" this observable. It will broadcast
    * elements and notices to this observable's subscribed observers and emitted elements will be
    * stored in this observable's cache (if present) to be possibly replayed to later subscribers.
    * 
    * @return an observer that can be used to drive emissions and notices for this observable
    */
   protected SizedObserver<T> getDriver() {
      return new SizedObserver<T>() {
         @Override
         public void initialize(int numberOfElements) {
            synchronized (lock) {
               Preconditions.checkState(expectedNumberOfMessages == -1,
                     "Stream already initialized");
               Preconditions.checkState(
                     (cache == null || cache.isEmpty()) && discardedMessages == 0,
                     "Stream already emitted item(s)");
               Preconditions.checkState(!finished, "Stream already finished");
               expectedNumberOfMessages = numberOfElements;
               for (Entry<Observer<? super T>, SubscriptionImpl> entry : observers.entrySet()) {
                  Observer<? super T> o = entry.getKey();
                  if (o instanceof SizedObserver) {
                     SubscriptionImpl s = entry.getValue();
                     s.execute(() -> ((SizedObserver<?>) o).initialize(numberOfElements));
                  }
               }
            }
         }
         
         @Override
         public void onNext(T element) {
            synchronized (lock) {
               Preconditions.checkState(!finished, "Stream already finished");
               if (!started) {
                  // cache all emissions that occur before start
                  if (cache == null) {
                     cache = new LinkedList<>();
                  }
                  cache.add(element);
               } else if (cacheLimit == 0) {
                  // cannot cache, must immediately discard
                  discardedMessages++;
               } else {
                  // discard oldest message in cache if we have reached limit
                  if (cache.size() == cacheLimit) {
                     cache.remove();
                     discardedMessages++;
                  }
                  cache.add(element);
               }
               for (Entry<Observer<? super T>, SubscriptionImpl> entry : observers.entrySet()) {
                  Observer<? super T> o = entry.getKey();
                  SubscriptionImpl s = entry.getValue();
                  s.execute(() -> o.onNext(element));
               }
            }
         }

         @Override
         public void onFinish() {
            synchronized (lock) {
               Preconditions.checkState(!finished, "Stream already finished");
               finished = true;
               for (Entry<Observer<? super T>, SubscriptionImpl> entry : observers.entrySet()) {
                  Observer<? super T> o = entry.getKey();
                  SubscriptionImpl s = entry.getValue();
                  s.execute(() -> o.onFinish());
               }
            }
         }

         @Override
         public void onFailure(Throwable cause) {
            synchronized (lock) {
               Preconditions.checkState(!finished, "Stream already finished");
               failure = cause;
               finished = true;
               for (Entry<Observer<? super T>, SubscriptionImpl> entry : observers.entrySet()) {
                  Observer<? super T> o = entry.getKey();
                  SubscriptionImpl s = entry.getValue();
                  s.execute(() -> o.onFailure(cause));
               }
            }
         }
      };
   }
   
   /**
    * Subscribes the given observer to this observable. If this is the first subscriber then it is
    * guaranteed to receive every element emitted by this observable. But subscribers registered
    * <em>after</em> the first one could miss elements if this observable has emitted more elements
    * than fit in the cache (and the default cache size is zero). You can create a copy of this
    * observable that has a larger cache using {@link #cache(int)} or {@link #cacheFully()}.
    * 
    * @param observer the new subscriber
    * @return the subscription between the given observer and this observable
    */
   public Subscription subscribe(Observer<? super T> observer) {
      return register(observer, true);
   }
   
   private Subscription register(Observer<? super T> observer, boolean start) {
      // Child observables want to start their parent. We ensure lock acquisition is always in the
      // same order (parent then child -- since that would happen when delivering notifications if
      // a direct executor were used) to avoid deadlock. That means we need to not be holding the
      // lock when we call start() so it can acquire any locks in a safe order.
      SubscriptionImpl s;
      synchronized (lock) {
         assert started || discardedMessages == 0;
         s = observers.computeIfAbsent(observer, o -> new SubscriptionImpl(o, discardedMessages));
         catchUpObserver(observer, s);
         if (started || !start) {
            return s;
         }
      }
      // Release lock before calling start().
      start();
      return s;
   }
   
   boolean start() {
      assert !Thread.holdsLock(lock);
      
      synchronized (lock) {
         if (started) {
            // nothing to do...
            return false;
         }
         
         assert discardedMessages == 0;
         // We cache the stream fully until we have "started" to ensure that an observer doesn't
         // miss any messages in the event that it doesn't subscribe until after messages have
         // already been received. After starting, we forward the messages to all subscribed
         // observers, optionally storing in a cache (for benefit of late subscribers). So, now that
         // we're starting, we want to swap out the unbounded cache for a limited one (possibly
         // backed by a fixed-sized array, or even simply null if the cache limit is zero).
         started = true;
         if (cacheLimit == 0) {
            discardedMessages = cache == null ? 0 : cache.size();
            cache = null;
         } else if (cacheLimit <= ARRAY_QUEUE_THRESHOLD) {
            Queue<T> newCache = new ArrayQueue<>(cacheLimit);
            if (cache != null) {
               if (cache.size() > cacheLimit) {
                  Iterator<T> iter = cache.iterator();
                  for (int skip = discardedMessages = cache.size() - cacheLimit; skip > 0; skip--) {
                     iter.next();
                  }
                  while (iter.hasNext()) {
                     boolean added = newCache.offer(iter.next());
                     assert added;
                  }
               } else {
                  newCache.addAll(cache);
               }
            }
            cache = newCache;
         } else if (cache != null && cache.size() > cacheLimit) {
            // We are continuing to use a LinkedList for the cache, but the current cache
            // accumulated too many elements. So we just trim it to the limit.
            for (int skip = discardedMessages = cache.size() - cacheLimit; skip > 0; skip--) {
               cache.remove();
            }
         }
         
         return true;
      }
   }
   
   private void catchUpObserver(Observer<? super T> observer, SubscriptionImpl s) {
      assert Thread.holdsLock(lock);
      
      if (expectedNumberOfMessages != -1 && observer instanceof SizedObserver) {
         int subscriberMessages = expectedNumberOfMessages - discardedMessages;
         s.execute(() -> ((SizedObserver<?>) observer).initialize(subscriberMessages));
      }
      if (cache != null) {
         for (T t : cache) {
            s.execute(() -> observer.onNext(t));
         }
      }
      if (failure != null) {
         s.execute(() -> observer.onFailure(failure));
      } else if (finished) {
         s.execute(() -> observer.onFinish());
      }
   }

   /**
    * Returns an observable that emits the same stream as this observable, but with an unbounded
    * cache of emitted elements. With this, subscribers need not worry about missing elements.
    * 
    * <p>Caution should be used with unbounded caching as it could cause an
    * {@link OutOfMemoryError} if an observable emits a very large number of elements.
    *  
    * @return an observable with an unbounded cache of emitted elements
    */
   public Observable<T> cacheFully() {
      return cache(Integer.MAX_VALUE);
   }
   
   /**
    * Returns an observable that emits the same stream as this observable, but that caches up to the
    * given number of emitted elements.
    * 
    * <p>Caution should be used with extremely high cache limit values as it could cause an
    * {@link OutOfMemoryError} if an observable emits a very large number of elements.
    *  
    * @return an observable with an unbounded cache of emitted elements
    */
   public Observable<T> cache(int cacheLimit) {
      Preconditions.checkArgument(cacheLimit >= 0,
            "cache size must be non-negative, instead got %s", cacheLimit);
      if (this.cacheLimit == cacheLimit) {
         // this observable already has the right cache limit
         return this;
      }
      Observable<T> ret = childObservable(cacheLimit);
      register(ret.getDriver(), false);
      return ret;
   }

   /**
    * Returns a view of the elements emitted by this observable as an {@link Iterator}. Unlike
    * typical iterators, arbitrary (unchecked) exceptions may be thrown by calls to
    * {@link Iterator#hasNext()} and {@link Iterator#next()} if the stream fails before all elements
    * are emitted/consumed.
    * 
    * <p>Note: this implicitly starts this observable, if not already started, by subscribing an
    * observer that feeds the elements to the returned iterator. Any subsequent subscribers to this
    * observable can miss emitted elements.
    * 
    * @return a view of the elements emitted by this observable as an {@link Iterator}
    * 
    * @see StreamingIterator
    */
   public SizedIterator<T> asIterator() {
      StreamingIterator<T> iter = new StreamingIterator<>();
      subscribe(iter.asObserver());
      return iter;
   }
   
   /**
    * Invokes the given action for each element emitted by this observable.
    * 
    * <p>Note: this implicitly starts this observable, if not already started, by subscribing an
    * observer that invokes the given action. Any subsequent subscribers to this observable can miss
    * emitted elements.
    * 
    * @param action the action invoked for each element emitted by this observable
    */
   public void forEach(Consumer<? super T> action) {
      subscribe(Observer.of(action, () -> {}, th -> {}));
   }
   
   /**
    * Transforms this observable to one of a different type. The given function is used to transform
    * elements emitted by this observable into elements emitted by the returned one.
    * 
    * @param function the function that maps elements to the new type
    * @return an observable of elements transformed by the given function
    */
   public <U> Observable<U> map(Function<? super T, ? extends U> function) {
      Observable<U> ret = childObservable();
      SizedObserver<U> asObserver = ret.getDriver();
      register(new SizedObserver<T>() {
         @Override
         public void initialize(int numberOfElements) {
            asObserver.initialize(numberOfElements);
         }

         @Override
         public void onNext(T element) {
            asObserver.onNext(function.apply(element));
         }

         @Override
         public void onFinish() {
            asObserver.onFinish();
         }

         @Override
         public void onFailure(Throwable failure) {
            asObserver.onFailure(failure);
         }
      }, false);
      return ret;
   }

   /**
    * Filters this observable by omitting some elements. The given predicate is used to filter
    * elements emitted by this observable.
    * 
    * @param filter the predicate that filters elements
    * @return an observable of elements filtered by the given predicate
    */
   public Observable<T> filter(Predicate<? super T> filter) {
      Observable<T> ret = childObservable();
      Observer<T> asObserver = ret.getDriver();
      register(new Observer<T>() {
         @Override
         public void onNext(T element) {
            if (filter.test(element)) {
               asObserver.onNext(element);
            }
         }

         @Override
         public void onFinish() {
            asObserver.onFinish();
         }

         @Override
         public void onFailure(Throwable failure) {
            asObserver.onFailure(failure);
         }
      }, false);
      return ret;
   }
   
   /**
    * Filters this observable by only emitting the first element. The returned observable will
    * either emit exactly one element or it will emit none (if this observable is empty). The
    * returned observable may finish before this observable since it will complete immediately after
    * this observable emits the first element.
    * 
    * @return an observable that emits only the first element
    */
   public Observable<T> first() {
      Observable<T> ret = childObservable(1);
      SizedObserver<T> asObserver = ret.getDriver();
      register(new SizedObserver<T>() {
         boolean done = false;
         
         @Override
         public void initialize(int numberOfElements) {
            asObserver.initialize(numberOfElements > 0 ? 1 : 0);
         }
         
         @Override
         public void onNext(T element) {
            if (!done) {
               asObserver.onNext(element);
               asObserver.onFinish();
               done = true;
            }
         }

         @Override
         public void onFinish() {
            if (!done) {
               asObserver.onFinish();
               done = true;
            }
         }

         @Override
         public void onFailure(Throwable failure) {
            if (!done) {
               asObserver.onFailure(failure);
               done = true;
            }
         }
      }, false);
      return ret;
   }

   /**
    * Gets the first element emitted by this observable. If this observable is empty then the given
    * default value is used. The returned future will complete as soon as this observable emits the
    * first element and may thus finish before this observable finishes.
    * 
    * @return a future that completes with the first value from this observable or the given default
    *       if this observable is empty
    */
   public CompletionStageFuture<T> firstOr(T defaultIfEmpty) {
      CompletableFuture<T> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         boolean done = false;
         
         @Override
         public void onNext(T element) {
            if (!done) {
               ret.complete(element);
               done = true;
            }
         }

         @Override
         public void onFinish() {
            if (!done) {
               ret.complete(defaultIfEmpty);
               done = true;
            }
         }

         @Override
         public void onFailure(Throwable failure) {
            if (!done) {
               ret.completeExceptionally(failure);
               done = true;
            }
         }
      }, false);
      return ret;
   }

   /**
    * Filters this observable by only emitting the last element. The returned observable will
    * either emit exactly one element or it will emit none (if this observable is empty).
    * 
    * @return an observable that emits only the last element
    */
   public Observable<T> last() {
      Observable<T> ret = childObservable(1);
      SizedObserver<T> asObserver = ret.getDriver();
      register(new SizedObserver<T>() {
         boolean empty = true;
         T last;
         
         @Override
         public void initialize(int numberOfElements) {
            asObserver.initialize(numberOfElements > 0 ? 1 : 0);
         }
         
         @Override
         public void onNext(T element) {
            empty = false;
            last = element;
         }

         @Override
         public void onFinish() {
            if (!empty) {
               asObserver.onNext(last);
            }
            asObserver.onFinish();
         }

         @Override
         public void onFailure(Throwable failure) {
            asObserver.onFailure(failure);
         }
      }, false);
      return ret;
   }

   /**
    * Gets the last element emitted by this observable. If this observable is empty then the given
    * default value is used.
    * 
    * @return a future that completes with the last value from this observable or the given default
    *       if this observable is empty
    */
   public CompletionStageFuture<T> lastOr(T defaultIfEmpty) {
      CompletableFuture<T> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         boolean empty = true;
         T last;
         
         @Override
         public void onNext(T element) {
            empty = false;
            last = element;
         }

         @Override
         public void onFinish() {
            ret.complete(empty ? defaultIfEmpty : last);
         }

         @Override
         public void onFailure(Throwable failure) {
            ret.completeExceptionally(failure);
         }
      }, false);
      return ret;
   }
   
   /**
    * Returns true if this observable is presently finished (either normally or due to a failure).
    * 
    * @return true if this observable is presently finished
    */
   public boolean isFinished() {
      synchronized (lock) {
         return finished;
      }
   }
   
   /**
    * Counts the number of elements emitted by this observable.
    * 
    * @return a future that completes with the count of elements emitted by this observable
    */
   public CompletionStageFuture<Integer> count() {
      CompletableFuture<Integer> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         @Override
         public void onNext(T element) {
         }

         @Override
         public void onFinish() {
            int count;
            synchronized (lock) {
               count = discardedMessages + (cache == null ? 0 : cache.size());
            }
            ret.complete(count);
         }

         @Override
         public void onFailure(Throwable failure) {
            onFinish();
         }
      }, false);
      return ret;
   }

   /**
    * Determines if this observable is empty or not. It is empty if it finishes without emitting any
    * elements (e.g. the count of elements is zero).
    * 
    * @return a future that completes with a value of true if this observable is empty
    */
   public CompletionStageFuture<Boolean> isEmpty() {
      CompletableFuture<Boolean> ret = new CompletableFuture<>();
      Subscription s = register(new Observer<T>() {
         @Override
         public void onNext(T element) {
            ret.complete(false);
         }

         @Override
         public void onFinish() {
            int count;
            synchronized (lock) {
               count = discardedMessages + (cache == null ? 0 : cache.size());
            }
            ret.complete(count == 0);
         }

         @Override
         public void onFailure(Throwable failure) {
            onFinish();
         }
      }, false);
      if (s.numberOfMissedMessages() > 0) {
         ret.complete(false);
      }
      return ret;
   }
   
   /**
    * Determines if this observable is successful or not. A successful observable is one that
    * finishes normally, without a failure.
    * 
    * @return a future that completes with a value of true if this observable is successful
    */
   public CompletionStageFuture<Boolean> isSuccessful() {
      CompletableFuture<Boolean> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         @Override
         public void onNext(T element) {
         }

         @Override
         public void onFinish() {
            ret.complete(true);
         }

         @Override
         public void onFailure(Throwable failure) {
            ret.complete(false);
         }
      }, false);
      return ret;
   }
   
   /**
    * Determines if this observable is failed or not. A failed observable is one that finishes with
    * a failure.
    * 
    * @return a future that completes with a value of true if this observable is a failure
    */
   public CompletionStageFuture<Boolean> isFailure() {
      CompletableFuture<Boolean> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         @Override
         public void onNext(T element) {
         }

         @Override
         public void onFinish() {
            ret.complete(false);
         }

         @Override
         public void onFailure(Throwable failure) {
            ret.complete(true);
         }
      }, false);
      return ret;
   }

   /**
    * Produces the cause of failure for this observable. If this observable finishes successfully,
    * this produces {@code null}.
    * 
    * @return a future that completes with the cause of failure of this observable or {@code null}
    *       if this observable is successful
    */
   public CompletionStageFuture<Throwable> getFailure() {
      CompletableFuture<Throwable> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         @Override
         public void onNext(T element) {
         }

         @Override
         public void onFinish() {
            ret.complete(null);
         }

         @Override
         public void onFailure(Throwable failure) {
            ret.complete(failure);
         }
      }, false);
      return ret;
   }
   
   /**
    * Determines if any elements emitted by the observable match the given predicate. If this
    * observable is empty, the result will be false. The returned future may finish before this
    * observable finishes as it can complete as soon as the first match is emitted.
    * 
    * @param filter the predicate used to test elements
    * @return a future that completes with a value of true if any of the elements emitted by this
    *       observable match the given predicate
    */
   public CompletionStageFuture<Boolean> anyMatch(Predicate<? super T> filter) {
      CompletableFuture<Boolean> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         boolean done = false;
         
         @Override
         public void onNext(T element) {
            if (filter.test(element)) {
               ret.complete(true);
               done = true;
            }
         }

         @Override
         public void onFinish() {
            if (!done) {
               ret.complete(false);
            }
         }

         @Override
         public void onFailure(Throwable failure) {
            if (!done) {
               ret.completeExceptionally(failure);
            }
         }
      }, false);
      return ret;
   }

   /**
    * Determines if all elements emitted by the observable match the given predicate. If this
    * observable is empty, the result will be true. The returned future may finish before this
    * observable finishes as it can complete as soon as the first element that does not match is
    * emitted.
    * 
    * @param filter the predicate used to test elements
    * @return a future that completes with a value of true if every element emitted by this
    *       observable matches the given predicate
    */
   public CompletionStageFuture<Boolean> allMatch(Predicate<? super T> filter) {
      CompletableFuture<Boolean> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         boolean done = false;
         
         @Override
         public void onNext(T element) {
            if (!filter.test(element)) {
               ret.complete(false);
               done = true;
            }
         }

         @Override
         public void onFinish() {
            if (!done) {
               ret.complete(true);
            }
         }

         @Override
         public void onFailure(Throwable failure) {
            if (!done) {
               ret.completeExceptionally(failure);
            }
         }
      }, false);
      return ret;
   }

   /**
    * Determines if none of the elements emitted by the observable match the given predicate. If
    * this observable is empty, the result will be true. The returned future may finish before this
    * observable finishes as it can complete as soon as the first matching element is emitted.
    * 
    * @param filter the predicate used to test elements
    * @return a future that completes with a value of true if none of the elements emitted by this
    *       observable match the given predicate
    */
   public CompletionStageFuture<Boolean> noneMatch(Predicate<? super T> filter) {
      CompletableFuture<Boolean> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         boolean done = false;
         
         @Override
         public void onNext(T element) {
            if (filter.test(element)) {
               ret.complete(false);
               done = true;
            }
         }

         @Override
         public void onFinish() {
            if (!done) {
               ret.complete(true);
            }
         }

         @Override
         public void onFailure(Throwable failure) {
            if (!done) {
               ret.completeExceptionally(failure);
            }
         }
      }, false);
      return ret;
   }
   
   /**
    * Reduces this observable to at most one result using the given reduce function. If this
    * observable is empty, the returned observable is also empty. If this observable emits exactly
    * one element, the returned observable will emit the same element. But if this observable emits
    * multiple elements, the first element is combined with the second using the given function. The
    * third element, if present is combined with that combination using the given function, and so
    * on. The result of the final invocation of the function, combining the last element with the
    * reduced value of its predecessor elements, is the one value emitted by the returned
    * observable.
    * 
    * @param reduceFunction the function used to reduce elements
    * @return an observable that emits no items if this observable is empty, otherwise emitting a
    *       single item that is the reduction of all items emitted by this observable
    */
   public Observable<T> reduce(BinaryOperator<T> reduceFunction) {
      Observable<T> ret = childObservable(1);
      SizedObserver<T> asObserver = ret.getDriver();
      register(new SizedObserver<T>() {
         boolean empty = true;
         T last;
         
         @Override
         public void initialize(int numberOfElements) {
            asObserver.initialize(numberOfElements > 0 ? 1 : 0);
         }
         
         @Override
         public void onNext(T element) {
            if (empty) {
               empty = false;
               last = element;
            } else {
               last = reduceFunction.apply(last, element);
            }
         }

         @Override
         public void onFinish() {
            if (!empty) {
               asObserver.onNext(last);
            }
            asObserver.onFinish();
         }

         @Override
         public void onFailure(Throwable failure) {
            asObserver.onFailure(failure);
         }
      }, false);
      return ret;
   }
   
   /**
    * Reduces this observable to exactly one result using the given reduce function. The reduction
    * is seeded with the given identity value. The first element is combined with this seed value
    * using the given function. Subsequent values are combined with the reduction of the preceding
    * values using the function. The returned future will complete when the observable finishes with
    * the final result of reducing the last emitted element.
    * 
    * @param identity the initial value used to seed the reduction
    * @param reduceFunction the function used to reduce elements
    * @return a future that completes with the given identity if this observable is empty, otherwise
    *       completing with the result of reducing all items emitted by this observable
    */
   public CompletionStageFuture<T> reduce(T identity, BinaryOperator<T> reduceFunction) {
      CompletableFuture<T> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         T last = identity;
         
         @Override
         public void onNext(T element) {
            last = reduceFunction.apply(last, element);
         }

         @Override
         public void onFinish() {
            ret.complete(last);
         }

         @Override
         public void onFailure(Throwable failure) {
            ret.completeExceptionally(failure);
         }
      }, false);
      return ret;
   }
   
   /**
    * Returns a view of the initial elements of this observable. If this observable emits more than
    * the given number of elements, they will not be emitted by the returned observable. The
    * returned observable can complete successfully, after the specified number of items have been
    * emitted, even if this observable fails after that point.
    * 
    * @param limit the limit on the initial number of elements
    * @return an observable that emits up to the specified number of elements and then completes
    */
   public Observable<T> limit(int limit) {
      if (limit < 0) {
         throw new IllegalArgumentException("limit cannot be negative");
      }
      Observable<T> ret = childObservable(Math.min(limit, cacheLimit));
      SizedObserver<T> asObserver = ret.getDriver();
      if (limit == 0) {
         asObserver.onFinish();
         return ret;
      }
      register(new SizedObserver<T>() {
         int remaining = limit;
         
         @Override
         public void initialize(int numberOfElements) {
            asObserver.initialize(Math.min(numberOfElements, limit));
         }
         
         @Override
         public void onNext(T element) {
            if (remaining > 0) {
               remaining--;
               asObserver.onNext(element);
               if (remaining == 0) {
                  asObserver.onFinish();
               }
            }
         }

         @Override
         public void onFinish() {
            if (remaining > 0) {
               asObserver.onFinish();
            }
         }

         @Override
         public void onFailure(Throwable failure) {
            if (remaining > 0) {
               asObserver.onFailure(failure);
            }
         }
      }, false);
      return ret;
   }
   
   /**
    * Returns a view that skips the initial elements of this observable. If this observable emits
    * fewer than the given number of elements, the returned observable will be empty. The returned
    * observable only emits elements <em>after</em> the given number have been emitted by this
    * observable.
    * 
    * @param skip the number of initial elements to skip
    * @return an observable that skips the given number of initial elements from this observable
    *       before it emits elements
    */
   public Observable<T> skip(int skip) {
      if (skip == 0) {
         return this;
      } else if (skip < 0) {
         throw new IllegalArgumentException("skip count cannot be negative");
      }
      Observable<T> ret = childObservable();
      SizedObserver<T> asObserver = ret.getDriver();
      register(new SizedObserver<T>() {
         int toSkip = skip;
         
         @Override
         public void initialize(int numberOfElements) {
            asObserver.initialize(Math.max(0, numberOfElements - skip));
         }
         
         @Override
         public void onNext(T element) {
            if (toSkip > 0) {
               toSkip--;
            } else {
               asObserver.onNext(element);
            }
         }

         @Override
         public void onFinish() {
            asObserver.onFinish();
         }

         @Override
         public void onFailure(Throwable failure) {
            asObserver.onFailure(failure);
         }
      }, false);
      return ret;
   }
   
   /**
    * Filters this observable by only emitting the minimum value. If this observable is empty, the
    * returned observable is also empty. If at least one item is emitted by this observable, the
    * returned observable emits only the smallest element according to the given comparator.
    * 
    * @param comparator a comparator
    * @return an observable that emits no elements if this observable is empty or only the smallest
    *       element if this observable is not empty
    */
   public Observable<T> min(Comparator<? super T> comparator) {
      requireNonNull(comparator);
      Observable<T> ret = childObservable();
      SizedObserver<T> asObserver = ret.getDriver();
      register(new Observer<T>() {
         T min;
         int count;
         boolean finished;
         
         @Override
         public void onNext(T element) {
            if (finished) {
               return;
            }
            
            if (count++ == 0) {
               min = element;
               return;
            }
            try {
               if (comparator.compare(min, element) > 0) {
                  min = element;
               }
            } catch (Exception e) {
               asObserver.onFailure(e);
               finished = true;
               
            }
         }

         @Override
         public void onFinish() {
            if (finished) {
               return;
            }
            
            if (count > 0) {
               asObserver.onNext(min);
            }
            asObserver.onFinish();
            finished = true;
         }

         @Override
         public void onFailure(Throwable failure) {
            if (finished) {
               return;
            }
            asObserver.onFailure(failure);
            finished = true;
         }
      }, false);
      return ret;
   }

   /**
    * Computes the minimum value emitted by this observable. If this observable is empty, the
    * returned future completes with the default given value.  If at least one item is emitted by
    * this observable, the returned future completes with the smallest element according to the
    * given comparator.
    * 
    * @param comparator a comparator
    * @param defaultValue a default value to use if this observable is empty
    * @return a future that completes with the smallest element emitted by this observable or with
    *       the given default
    */
   public CompletionStageFuture<T> minOr(Comparator<? super T> comparator, T defaultValue) {
      requireNonNull(comparator);
      CompletableFuture<T> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         T min;
         int count;
         boolean finished;
         
         @Override
         public void onNext(T element) {
            if (finished) {
               return;
            }
            
            if (count++ == 0) {
               min = element;
               return;
            }
            try {
               if (comparator.compare(min, element) > 0) {
                  min = element;
               }
            } catch (Exception e) {
               ret.completeExceptionally(e);
               finished = true;
               
            }
         }

         @Override
         public void onFinish() {
            if (finished) {
               return;
            }
            
            if (count > 0) {
               ret.complete(min);
            } else {
               ret.complete(defaultValue);
            }
            finished = true;
         }

         @Override
         public void onFailure(Throwable failure) {
            if (finished) {
               return;
            }
            ret.completeExceptionally(failure);
            finished = true;
         }
      }, false);
      return ret;
   }
   
   /**
    * Filters this observable by only emitting the maximum value. If this observable is empty, the
    * returned observable is also empty. If at least one item is emitted by this observable, the
    * returned observable emits only the largest element according to the given comparator.
    * 
    * @param comparator a comparator
    * @return an observable that emits no elements if this observable is empty or only the largest
    *       element if this observable is not empty
    */
   public Observable<T> max(Comparator<? super T> comparator) {
      requireNonNull(comparator);
      Observable<T> ret = childObservable();
      SizedObserver<T> asObserver = ret.getDriver();
      register(new Observer<T>() {
         T max;
         int count;
         boolean finished;
         
         @Override
         public void onNext(T element) {
            if (finished) {
               return;
            }
            
            if (count++ == 0) {
               max = element;
               return;
            }
            try {
               if (comparator.compare(max, element) < 0) {
                  max = element;
               }
            } catch (Exception e) {
               asObserver.onFailure(e);
               finished = true;
               
            }
         }

         @Override
         public void onFinish() {
            if (finished) {
               return;
            }
            
            if (count > 0) {
               asObserver.onNext(max);
            }
            asObserver.onFinish();
            finished = true;
         }

         @Override
         public void onFailure(Throwable failure) {
            if (finished) {
               return;
            }
            asObserver.onFailure(failure);
            finished = true;
         }
      }, false);
      return ret;
   }

   /**
    * Computes the maximum value emitted by this observable. If this observable is empty, the
    * returned future completes with the default given value.  If at least one item is emitted by
    * this observable, the returned future completes with the largest element according to the
    * given comparator.
    * 
    * @param comparator a comparator
    * @param defaultValue a default value to use if this observable is empty
    * @return a future that completes with the largest element emitted by this observable or with
    *       the given default
    */
   public CompletionStageFuture<T> maxOr(Comparator<? super T> comparator, T defaultValue) {
      requireNonNull(comparator);
      CompletableFuture<T> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         T max;
         int count;
         boolean finished;
         
         @Override
         public void onNext(T element) {
            if (finished) {
               return;
            }
            
            if (count++ == 0) {
               max = element;
               return;
            }
            try {
               if (comparator.compare(max, element) < 0) {
                  max = element;
               }
            } catch (Exception e) {
               ret.completeExceptionally(e);
               finished = true;
               
            }
         }

         @Override
         public void onFinish() {
            if (finished) {
               return;
            }
            
            if (count > 0) {
               ret.complete(max);
            } else {
               ret.complete(defaultValue);
            }
            finished = true;
         }

         @Override
         public void onFailure(Throwable failure) {
            if (finished) {
               return;
            }
            ret.completeExceptionally(failure);
            finished = true;
         }
      }, false);
      return ret;
   }
   
   /**
    * Collects the elements emitted by this observable. The value returned is created via the given
    * supplier. Emitted elements are added to this value via the given accumulator. When the
    * observable finishes, the returned future completes with this value.
    * 
    * @param supplier creates the result value
    * @param accumulator accumulates elements into the result value
    * @return a future that finishes with the collected result when this observable finishes
    */
   public <R> CompletionStageFuture<R> collect(Supplier<R> supplier, BiConsumer<R, T> accumulator) {
      return collect(supplier, accumulator, Function.identity());
   }

   /**
    * Collects the elements emitted by this observable. An intermediate object is created via the
    * given supplier. Emitted elements are added to this intermediate value via the given
    * accumulator. The intermediate value is then transformed into the result value via the given
    * finisher function. When the observable finishes, the returned future completes with this 
    * finished value.
    * 
    * @param supplier creates the intermediate value
    * @param accumulator accumulates elements into the intermediate value
    * @param finisher computes the finished result from the intermediate value
    * @return a future that finishes with the collected result when this observable finishes
    */
   public <R, A> CompletionStageFuture<R> collect(Supplier<A> supplier,
         BiConsumer<A, ? super T> accumulator, Function<A, R> finisher) {
      requireNonNull(supplier);
      requireNonNull(accumulator);
      requireNonNull(finisher);
      CompletableFuture<R> ret = new CompletableFuture<>();
      register(new Observer<T>() {
         boolean finished;
         int count;
         A a;
         
         @Override
         public void onNext(T element) {
            if (finished) {
               return;
            }
            
            try {
               if (count++ == 0) {
                  a = supplier.get();
               } else {
                  accumulator.accept(a, element);
               }
            } catch (Exception e) {
               ret.completeExceptionally(e);
               finished = true;
            }
         }

         @Override
         public void onFinish() {
            if (finished) {
               return;
            }
            
            try {
               if (count == 0) {
                  ret.complete(finisher.apply(supplier.get()));
               } else {
                  ret.complete(finisher.apply(a));
               }
            } catch (Exception e) {
               ret.completeExceptionally(e);
            }
            finished = true;
         }

         @Override
         public void onFailure(Throwable failure) {
            if (finished) {
               return;
            }
            ret.completeExceptionally(failure);
            finished = true;
         }
      }, false);
      return ret;
   }

   /**
    * Collects the elements emitted by this observable using the given collector.
    * 
    * @param collector a collector
    * @return a future that finishes with the collected result when this observable finishes
    */
   public <R, A> CompletionStageFuture<R> collect(Collector<? super T, A, R> collector) {
      return collect(collector.supplier(), collector.accumulator(), collector.finisher());
   }
   

   /**
    * Collects the elements emitted by this observable into an array.
    * 
    * @return a future that finishes with an array that contains the elements emitted by this
    *       observable, in the order they were emitted
    */
   public CompletionStageFuture<Object[]> toArray() {
      return collect(ArrayList::new, ArrayList::add, ArrayList::toArray);
   }

   /**
    * Collects the elements emitted by this observable into an array. The given function is invoked
    * with the final array size and must produce the final array value (though empty). 
    * 
    * @return a future that finishes with an array that contains the elements emitted by this
    *       observable, in the order they were emitted
    */
   public <A> CompletionStageFuture<A[]> toArray(IntFunction<A[]> arrayFactory) {
      return collect(ArrayList::new, ArrayList::add, 
            list -> list.toArray(arrayFactory.apply(list.size())));
   }
   
   /**
    * Sorts the elements emitted by this observable. The returned observable does not emit any 
    * elements until this observable finishes. It accumulates (and thus buffers) all of the emitted
    * values and, on completion, sorts them and emits them in sorted order.
    * 
    * @param comparator the comparator used to order the emitted elements
    * @return an observable that emits the same elements as this observable, but in sorted order
    */
   public Observable<T> sorted(Comparator<? super T> comparator) {
      requireNonNull(comparator);
      Observable<T> ret = childObservable();
      SizedObserver<T> asObserver = ret.getDriver();
      register(new SizedObserver<T>() {
         ArrayList<T> list;
         
         @Override
         public void initialize(int numberOfElements) {
            list = new ArrayList<>(numberOfElements);
            asObserver.initialize(numberOfElements);
         }
         
         @Override
         public void onNext(T element) {
            if (list == null) {
               list = new ArrayList<>();
            }
            list.add(element);
         }

         @Override
         public void onFinish() {
            if (list != null) {
               try {
                  list.sort(comparator);
               } catch (Exception e) {
                  asObserver.onFailure(e);
               }
               for (T t : list) {
                  asObserver.onNext(t);
               }
            }
            asObserver.onFinish();
         }

         @Override
         public void onFailure(Throwable failure) {
            // sort and emit the elements collected so far before failing
            if (list != null) {
               boolean failedToSort = false;
               try {
                  list.sort(comparator);
               } catch (Exception e) {
                  failure.addSuppressed(e);
                  failedToSort = true;
               }
               if (!failedToSort) {
                  for (T t : list) {
                     asObserver.onNext(t);
                  }
               }
            }
            asObserver.onFailure(failure);
         }
      }, false);
      return ret;
   }
   
   /**
    * Filters the elements emitted by this observable so that only distinct values are emitted. The
    * returned observable does not emit any duplicates. It accumulates (and thus buffers) all of the
    * distinct emitted values so it can decide whether or not to emit the next element. The
    * {@link Object#hashCode()} and {@link Object#equals(Object)} methods are used to determine if
    * two values are equal.
    * 
    * @return an observable that emits the same elements as this observable, but without any
    *       repeated values
    */
   public Observable<T> distinct() {
      Observable<T> ret = childObservable();
      SizedObserver<T> asObserver = ret.getDriver();
      register(new Observer<T>() {
         HashSet<T> set;
         
         @Override
         public void onNext(T element) {
            if (set.add(element)) {
               asObserver.onNext(element);
            }
         }

         @Override
         public void onFinish() {
            asObserver.onFinish();
         }

         @Override
         public void onFailure(Throwable failure) {
            asObserver.onFailure(failure);
         }
      }, false);
      return ret;
   }
   
   public Observable<T> concat(Observable<? extends T> other) {
      Observable<T> ret = childObservable();
      SizedObserver<T> asObserver = ret.getDriver();
      register(new Observer<T>() {
         boolean usingOther;
         
         @Override
         public void onNext(T element) {
            asObserver.onNext(element);
         }

         @Override
         public void onFinish() {
            if (usingOther) {
               asObserver.onFinish();
            } else {
               usingOther = true;
               other.subscribe(this);
            }
         }

         @Override
         public void onFailure(Throwable failure) {
            asObserver.onFailure(failure);
         }
      }, false);
      return ret;
   }
   
   public static <T> Observable<T> concat(Iterable<? extends Observable<? extends T>> observables) {
      Iterator<? extends Observable<? extends T>> iter = observables.iterator();
      if (!iter.hasNext()) {
         return empty();
      }
      Observable<? extends T> first = iter.next();
      Observable<T> ret = first.childObservable();
      SizedObserver<T> asObserver = ret.getDriver();
      first.register(new Observer<T>() {
         @Override
         public void onNext(T element) {
            asObserver.onNext(element);
         }

         @Override
         public void onFinish() {
            if (iter.hasNext()) {
               iter.next().subscribe(this);
            } else {
               asObserver.onFinish();
            }
         }

         @Override
         public void onFailure(Throwable failure) {
            asObserver.onFailure(failure);
         }
      }, false);
      return ret;      
   }

   @SuppressWarnings("unchecked") // no values actually used, so cast is safe
   public static <T> Observable<T> empty() {
      return (Observable<T>) EMPTY;
   }

   private <U> Observable<U> childObservable() {
      return childObservable(cacheLimit, executor);
   }
   
   private <U> Observable<U> childObservable(int childCacheLimit) {
      return childObservable(childCacheLimit, executor);
   }
   
   private <U> Observable<U> childObservable(int childCacheLimit, Executor childExecutor) {
      Observable<T> parent = this;
      return new Observable<U>(childCacheLimit, childExecutor) {
         @Override
         boolean start() {
            // make sure parent is started first
            parent.start();
            return super.start();
         }
      };
   }
   
   private class SubscriptionImpl implements Subscription {
      /*
       * NB: Synchronization is used in this class and also used in the enclosing class, using two
       * different lock objects. Lock acquisition should always be ordered so that this
       * subscription's monitor is acquired first, and the observable's lock is acquired second.
       * Reversing this order could lead to deadlock.
       */
      private final int missedMessages;
      private final Observer<? super T> observer;
      private final Executor executor;
      private boolean cancelled;
      private boolean unsubscribed;
      
      SubscriptionImpl(Observer<? super T> observer, int missedMessages) {
         this.observer = observer;
         this.missedMessages = missedMessages;
         this.executor = new SerialExecutor(Observable.this.executor);
      }
      
      void execute(Runnable r) {
         executor.execute(r);
      }
      
      @Override
      public int numberOfMissedMessages() {
         return missedMessages;
      }
      
      @Override
      public synchronized boolean isFinished() {
         if (cancelled || unsubscribed) {
            return true;
         }
         synchronized (lock) {
            return finished;
         }
      }
      
      @Override
      public synchronized boolean isCancelled() {
         return cancelled;
      }
      
      @Override
      public synchronized boolean cancel() {
         synchronized (lock) {
            if (finished) {
               return false;
            }
            if (!observers.remove(observer, this)) {
               return false;
            }
         }
         cancelled = true;
         execute(() -> observer.onFailure(new CancellationException()));
         return true;
      }
      
      @Override
      public synchronized boolean isUnsubscribed() {
         return unsubscribed;
      }
      
      @Override
      public synchronized boolean unsubscribe() {
         synchronized (lock) {
            if (finished) {
               return false;
            }
            if (!observers.remove(observer, this)) {
               return false;
            }
         }
         unsubscribed = true;
         execute(() -> observer.onFinish());
         return true;
      }
   }
}
