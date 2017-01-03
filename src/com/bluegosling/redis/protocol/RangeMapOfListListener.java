package com.bluegosling.redis.protocol;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import com.bluegosling.redis.concurrent.Callback;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import io.netty.buffer.ByteBuf;

public class RangeMapOfListListener<K extends Comparable<K>, V>
extends BaseReplyListener<RangeMap<K, List<V>>> {
   private final Function<Callback<K>, ReplyListener> keyListener;
   private final Function<Callback<V>, ReplyListener> valueListener;
   private RangeMap<K, List<V>> map;
   private int pendingEntries;

   public RangeMapOfListListener(Callback<RangeMap<K, List<V>>> callback,
         Function<Callback<K>, ReplyListener> keyListener,
         Function<Callback<V>, ReplyListener> valueListener) {
      super(callback);
      this.keyListener = keyListener;
      this.valueListener = valueListener;
   }

   @Override
   public ReplyListener onArrayReply(int numberOfElements) {
      if (numberOfElements <= 0) {
         callbackSuccess(ImmutableRangeMap.of());
         return NO_OP;
      }
      pendingEntries = numberOfElements >> 1;
      map = TreeRangeMap.create();
      return new EntryListener<K, V>(new Callback<Entry<Range<K>, List<V>>>() {
         @Override
         public void onSuccess(Entry<Range<K>, List<V>> e) {
            map.put(e.getKey(), Collections.unmodifiableList(e.getValue()));
            pendingEntries--;
            if (pendingEntries == 0) {
               callbackSuccess(map);
            }
         }

         @Override
         public void onFailure(Throwable th) {
            callbackFailure(th);
         }
      }, keyListener, valueListener);
   }
   
   static class EntryListener<K extends Comparable<K>, V>
   extends BaseReplyListener<Entry<Range<K>, List<V>>> {
      private final Function<Callback<K>, ReplyListener> keyListener;
      private final Function<Callback<V>, ReplyListener> valueListener;

      EntryListener(Callback<Entry<Range<K>, List<V>>> callback,
            Function<Callback<K>, ReplyListener> keyListener,
            Function<Callback<V>, ReplyListener> valueListener) {
         super(callback);
         this.keyListener = keyListener;
         this.valueListener = valueListener;
      }
      
      @Override
      public ReplyListener onArrayReply(int numberOfElements) {
         if (numberOfElements < 0) {
            callbackSuccess(null);
            return NO_OP;
         }
         if (numberOfElements < 2) {
            callbackFailure(new RedisResponseException(
                  "Expecting an array of exactly two elements, got " + numberOfElements));
            return NO_OP;
         }
         return new ArrayElementListener<>(getCallback(), numberOfElements - 2, keyListener,
               valueListener);
      }
   }
   
   static class ArrayElementListener<K extends Comparable<K>, V>
   extends BaseReplyListener<Entry<Range<K>, List<V>>> {
      private final ReplyListener keyListener;
      private final ReplyListener valueListener;
      private K lowerBound;
      private K upperBound;
      private List<V> values = new ArrayList<>();
      private int receivedBounds;
      
      ArrayElementListener(Callback<Entry<Range<K>, List<V>>> callback, int expectedListSize,
            Function<Callback<K>, ReplyListener> keyListener,
            Function<Callback<V>, ReplyListener> valueListener) {
         super(callback);
         this.keyListener = keyListener.apply(new Callback<K>() {
            @Override
            public void onSuccess(K k) {
               if (receivedBounds == 0) {
                  lowerBound = k;
               } else {
                  upperBound = k;
               }
               receivedBounds++;
            }

            @Override
            public void onFailure(Throwable th) {
               callbackFailure(th);
            }
         });
         this.valueListener = valueListener.apply(new Callback<V>() {
            @Override
            public void onSuccess(V v) {
               values.add(v);
               if (values.size() == expectedListSize) {
                  callbackSuccess(new AbstractMap.SimpleImmutableEntry<>(
                        Range.closed(lowerBound, upperBound), values));
               }
            }

            @Override
            public void onFailure(Throwable th) {
               callbackFailure(th);
            }
         });
      }
      
      private ReplyListener current() {
         return receivedBounds < 2 ? keyListener : valueListener;
      }

      @Override
      public void onSimpleReply(String string) {
         current().onSimpleReply(string);
      }

      @Override
      public void onBulkReply(ByteBuf bytes) {
         current().onBulkReply(bytes);
      }

      @Override
      public void onErrorReply(String errorMsg) {
         current().onErrorReply(errorMsg);
      }

      @Override
      public void onIntegerReply(long value) {
         current().onIntegerReply(value);
      }

      @Override
      public ReplyListener onArrayReply(int numberOfElements) {
         return current().onArrayReply(numberOfElements);
      }
   }
}
