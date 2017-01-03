package com.bluegosling.redis.protocol;

import java.nio.CharBuffer;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import com.bluegosling.redis.Marshaller;
import com.bluegosling.redis.RedisException;
import com.bluegosling.redis.channel.RedisChannel;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.concurrent.CompletableFutureCallback;
import com.bluegosling.redis.concurrent.Observer;
import com.bluegosling.redis.values.Response;
import com.bluegosling.redis.values.ScanCursor;
import com.bluegosling.redis.values.ScanResult;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.internal.MessagePassingQueue.Consumer;

/**
 * Implementations for the SCAN operations (SCAN, SSCAN, HSCAN, and ZSCAN) introduced in Redis 2.8.
 * These implementations issue a scan command and stream the responses back to an observer. When the
 * results are exhausted, they automatically issue another scan command with the latest cursor,
 * until the server replies with a cursor of "0". This allows clients to seamlessly stream the
 * entire set of results.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public final class Scanning {
   private Scanning() { throw new AssertionError(); }
   
   private static final Marshaller<Double> DOUBLE_MARSHALLER = new Marshaller<Double>() {
      @Override
      public ByteBuf toBytes(Double d, ByteBufAllocator alloc) {
         return ByteBufUtil.encodeString(alloc, CharBuffer.wrap(d.toString()), Charsets.UTF_8);
      }

      @Override
      public Double fromBytes(ByteBuf buffer) {
         String s = buffer.toString(Charsets.UTF_8);
         return Double.parseDouble(s);
      }
   };
   
   public static <K> void scanAsync(RedisChannel channel, String pattern, Marshaller<K> marshaller,
         Observer<K> observer) {
      performScanAsync(channel, req -> req.nextToken("SCAN"), pattern,
            new MarshallingObserver<>(marshaller, observer));
   }
   
   public static <K> Iterator<K> scanSync(RedisChannel channel, String pattern,
         Marshaller<K> marshaller) {
      return performScanSync(channel, req -> req.nextToken("SCAN"), pattern,
            cb -> new ScanResultListener<>(cb, marshaller));
   }
   
   public static <K, V> void sscanAsync(RedisChannel channel, K key, String pattern,
         Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller, Observer<V> observer) {
      performScanAsync(channel,
            req -> req
                  .nextToken("SSCAN")
                  .nextToken(keyMarshaller.toBytes(key, channel.allocator())),
            pattern,
            new MarshallingObserver<>(valueMarshaller, observer));
   }

   public static <K, V> Iterator<V> sscanSync(RedisChannel channel, K key, String pattern,
         Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
      return performScanSync(channel,
            req -> req
                  .nextToken("SSCAN")
                  .nextToken(keyMarshaller.toBytes(key, channel.allocator())),
            pattern,
            cb -> new ScanResultListener<>(cb, valueMarshaller));
   }

   public static <K, V> void hscanAsync(RedisChannel channel, K key, String pattern,
         Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller,
         Observer<Entry<K, V>> observer) {
      performScanAsync(channel,
            req -> req
                  .nextToken("HSCAN")
                  .nextToken(keyMarshaller.toBytes(key, channel.allocator())),
            pattern,
            new EntryMarshallingObserver<>(keyMarshaller, valueMarshaller, observer));
   }
   
   public static <K, V> Iterator<Entry<K, V>> hscanSync(RedisChannel channel, K key, String pattern,
         Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
      return performScanSync(channel,
            req -> req
                  .nextToken("HSCAN")
                  .nextToken(keyMarshaller.toBytes(key, channel.allocator())),
            pattern,
            cb -> new EntryScanResultListener<>(cb, keyMarshaller, valueMarshaller));
   }

   public static <K, V> void zscanAsync(RedisChannel channel, K key, String pattern,
         Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller,
         Observer<Entry<V, Double>> observer) {
      performScanAsync(channel,
            req -> req
                  .nextToken("ZSCAN")
                  .nextToken(keyMarshaller.toBytes(key, channel.allocator())),
            pattern,
            new EntryMarshallingObserver<>(valueMarshaller, DOUBLE_MARSHALLER, observer));
   }

   public static <K, V> Iterator<Entry<V, Double>> zscanSync(RedisChannel channel, K key,
         String pattern, Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
      return performScanSync(channel,
            req -> req
                  .nextToken("ZSCAN")
                  .nextToken(keyMarshaller.toBytes(key, channel.allocator())),
            pattern,
            cb -> new EntryScanResultListener<>(cb, valueMarshaller, DOUBLE_MARSHALLER));
   }

   private static void performScanAsync(RedisChannel channel,
         Consumer<RequestStreamer> prefaceWriter, String pattern, Observer<ByteBuf> observer) {
      new ScanReplyListener(channel, prefaceWriter, pattern, observer).start();
   }

   private static <T> Iterator<T> performScanSync(RedisChannel channel,
         Consumer<RequestStreamer> prefaceWriter, String pattern,
         Function<Callback<ScanResult<T>>, ReplyListener> listenerFactory) {
      return new ScanIterator<>(channel, prefaceWriter, pattern, listenerFactory);
   }
   
   /*
    * Async implementation -- streams everything to observer, no backpressure
    */
   // TODO: basic backpressure so we only buffer a single chunk at a time and don't fetch
   // subsequent chunks until there is demand

   private static class MarshallingObserver<T> implements Observer<ByteBuf> {
      private final Marshaller<T> marshaller;
      private final Observer<T> observer;
      private boolean failed;
      
      MarshallingObserver(Marshaller<T> marshaller, Observer<T> observer) {
         this.marshaller = marshaller;
         this.observer = observer;
      }

      @Override
      public void onNext(ByteBuf element) {
         if (failed) {
            return;
         }
         T value;
         try {
            value = marshaller.fromBytes(element);
         } catch (Throwable th) {
            observer.onFailure(th);
            failed = true;
            return;
         }
         observer.onNext(value);
      }

      @Override
      public void onFinish() {
         if (!failed) {
            observer.onFinish();
         }
      }

      @Override
      public void onFailure(Throwable failure) {
         if (!failed) {
            observer.onFailure(failure);
         }
      }
   }

   private static class EntryMarshallingObserver<K, V> implements Observer<ByteBuf> {
      private final Marshaller<K> keyMarshaller;
      private final Marshaller<V> valueMarshaller;
      private final Observer<Entry<K, V>> observer;
      private boolean awaitingValue;
      private K latestKey;
      private boolean failed;

      EntryMarshallingObserver(Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller,
            Observer<Entry<K, V>> observer) {
         this.keyMarshaller = keyMarshaller;
         this.valueMarshaller = valueMarshaller;
         this.observer = observer;
      }

      @Override
      public void onNext(ByteBuf element) {
         if (failed) {
            return;
         }
         if (awaitingValue) {
            V value;
            try {
               value = valueMarshaller.fromBytes(element);
            } catch (Throwable th) {
               observer.onFailure(th);
               failed = true;
               return;
            }
            observer.onNext(new AbstractMap.SimpleImmutableEntry<>(latestKey, value));
            latestKey = null;
         } else {
            try {
               latestKey = keyMarshaller.fromBytes(element);
            } catch (Throwable th) {
               observer.onFailure(th);
               failed = true;
               return;
            }
         }
         awaitingValue = !awaitingValue;
      }

      @Override
      public void onFinish() {
         if (failed) {
            return;
         } else if (awaitingValue) {
            observer.onFailure(new RedisResponseException(
                  "Reply had odd number of elements: missing value for final entry"));
            latestKey = null;
         } else {
            observer.onFinish();
         }
      }

      @Override
      public void onFailure(Throwable failure) {
         if (!failed) {
            observer.onFailure(failure);
         }
      }
   }
   
   private static class ScanReplyListener implements ReplyListener {
      private enum ReplyPhase {
         ENVELOPE,
         ARRAY_OF_CURSOR_AND_DATA,
         STREAM
      }
      private final RedisChannel channel;
      private final Consumer<RequestStreamer> prefaceWriter;
      private final String pattern;
      private final Observer<ByteBuf> observer;
      private String nextCursor = "";
      private ReplyPhase phase = ReplyPhase.ENVELOPE;
      private int elementsRemaining;
      private boolean done;
      
      ScanReplyListener(RedisChannel channel, Consumer<RequestStreamer> prefaceWriter,
            String pattern, Observer<ByteBuf> observer) {
         this.channel = channel;
         this.prefaceWriter = prefaceWriter;
         this.pattern = pattern;
         this.observer = observer;
      }
      
      void start() {
         doScan("0");
      }
      
      private void doScan(String cursor) {
         RequestStreamer req = channel.newCommand(this);
         prefaceWriter.accept(req);
         req.nextToken(cursor);
         if (pattern != null) {
            req.nextToken("MATCH").nextToken(pattern);
         }
         req.finish();
      }
      
      @Override
      public void onSimpleReply(String reply) {
         if (!done) {
            done = true;
            observer.onFailure(
                  new RedisUnexpectedResponseException(Response.simpleResponse(reply)));
         }
      }

      @Override
      public void onErrorReply(String errorMsg) {
         if (!done) {
            done = true;
            observer.onFailure(new RedisException(errorMsg));
         }
      }

      @Override
      public void onIntegerReply(long value) {
         if (!done) {
            done = true;
            observer.onFailure(
                  new RedisUnexpectedResponseException(Response.intResponse(value)));
         }
      }

      @Override
      public void onBulkReply(ByteBuf bytes) {
         if (done) {
            return;
         } else if (phase == ReplyPhase.ARRAY_OF_CURSOR_AND_DATA && elementsRemaining == 2) {
            elementsRemaining--;
            nextCursor = bytes.toString(Charsets.UTF_8);
         } else if (phase == ReplyPhase.STREAM) {
            observer.onNext(bytes);
            if (--elementsRemaining == 0) {
               scanNext();
            }
         } else {
            done = true;
            observer.onFailure(
                  new RedisUnexpectedResponseException(Response.bulkResponse(bytes)));
         }
      }

      @Override
      public ReplyListener onArrayReply(int numberOfElements) {
         if (done) {
            return NO_OP;
         } else if (phase == ReplyPhase.ENVELOPE) {
            if (numberOfElements != 2) {
               done = true;
               observer.onFailure(new RedisResponseException(
                     "Scan response should contain two elements (next cursor and data) but instead"
                     + " has " + Math.max(0, numberOfElements) + " elements"));
               return NO_OP;
            } else {
               phase = ReplyPhase.ARRAY_OF_CURSOR_AND_DATA;
               elementsRemaining = 2;
               return this;
            }
         } else if (phase == ReplyPhase.ARRAY_OF_CURSOR_AND_DATA) {
            if (--elementsRemaining != 0) {
               done = true;
               observer.onFailure(
                     new RedisResponseException("Expecting cursor but got array value"));
               return NO_OP;
            } else if (numberOfElements < 0) {
               done = true;
               observer.onFailure(
                     new RedisResponseException("Expecting stream of results but got nil array"));
               return NO_OP;
            } else if (numberOfElements == 0) {
               // scan, especially with a pattern to match, can return chunks with zero
               // elements. caller is expected to repeat, but with new cursor
               scanNext();
               return NO_OP;
            } else {
               phase = ReplyPhase.STREAM;
               elementsRemaining = numberOfElements;
               return this;
            }
         } else {
            // accumulate array elements in order to report the actual response in the exception
            done = true;
            if (numberOfElements < 0) {
               observer.onFailure(
                     new RedisUnexpectedResponseException(Response.nilArrayResponse()));
               return NO_OP;
            } else if (numberOfElements == 0) {
               observer.onFailure(
                     new RedisUnexpectedResponseException(Response.emptyArrayResponse()));
               return NO_OP;
            }
            Response[] responses = new Response[numberOfElements];
            return new ResponseListener(new Callback<Response>() {
               int index = 0;
               
               @Override
               public void onSuccess(Response t) {
                  responses[index++] = t;
                  if (index == numberOfElements) {
                     observer.onFailure(
                           new RedisUnexpectedResponseException(Response.arrayResponse(responses)));
                  }
               }

               @Override
               public void onFailure(Throwable th) {
                  observer.onFailure(th);
               }
            });
         }
      }
      
      private void scanNext() {
         if (nextCursor.equals("0")) {
            done = true;
            observer.onFinish();
         } else {
            phase = ReplyPhase.ENVELOPE; // reset
            doScan(nextCursor);
         }
      }

      @Override
      public void onFailure(Throwable th) {
         if (!done) {
            done = true;
            observer.onFailure(th);
         }
      }
   }
   
   /*
    * Sync implementation -- fetches in chunks, supports back-pressure in that it will only
    * buffer a single chunk of results at a time until consumer drains the chunk. Fetches
    * subsequent chunks only on demand.
    */
   
   private static class ScanIterator<T> implements Iterator<T> {
      private final RedisChannel channel;
      private final Consumer<RequestStreamer> prefaceWriter;
      private final String pattern;
      private final Function<Callback<ScanResult<T>>, ReplyListener> listenerFactory;
      private ScanCursor nextCursor;
      private Iterator<T> currentIterator;

      ScanIterator(RedisChannel channel, Consumer<RequestStreamer> prefaceWriter, String pattern,
            Function<Callback<ScanResult<T>>, ReplyListener> listenerFactory) {
         this.channel = channel;
         this.prefaceWriter = prefaceWriter;
         this.pattern = pattern;
         this.listenerFactory = listenerFactory;
      }
      
      private void fetchMore(ScanCursor cursor) {
         CompletableFutureCallback<ScanResult<T>> f = new CompletableFutureCallback<>();
         RequestStreamer req = channel.newCommand(listenerFactory.apply(f));
         prefaceWriter.accept(req);
         req.nextToken(cursor.toString());
         if (pattern != null) {
            req.nextToken("MATCH").nextToken(pattern);
         }
         req.finish();
         ScanResult<T> result;
         try {
            result = Uninterruptibles.getUninterruptibly(f);
         } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
         }
         nextCursor = result.nextCursor();
         currentIterator = result.data().iterator();
      }
      
      private void maybeFetchMore() {
         if (nextCursor == null) {
            // initial fetch
            fetchMore(ScanCursor.START);
         }
         while (currentIterator != null && !currentIterator.hasNext()) {
            if (nextCursor.equals(ScanCursor.STOP)) {
               // done
               currentIterator = null;
               return;
            }
            fetchMore(nextCursor);
         }
      }
      
      @Override
      public boolean hasNext() {
         maybeFetchMore();
         return currentIterator != null;
      }

      @Override
      public T next() {
         maybeFetchMore();
         if (currentIterator == null) {
            throw new NoSuchElementException();
         }
         return currentIterator.next();
      }
   }
}
