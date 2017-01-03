package com.bluegosling.redis.values;

import static java.util.Objects.requireNonNull;

import java.nio.CharBuffer;

import com.bluegosling.redis.Marshaller;
import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

/**
 * Represents a bound for range queries over the members of sorted sets.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of the bound
 */
public class Bound<T> {
   private static final ByteBuf PLUS =
         Unpooled.unreleasableBuffer(ByteBufUtil.encodeString(
               UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap("+"), Charsets.US_ASCII))
         .asReadOnly();
   private static final ByteBuf MINUS =
         Unpooled.unreleasableBuffer(ByteBufUtil.encodeString(
               UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap("-"), Charsets.US_ASCII))
         .asReadOnly();
   private static final ByteBuf BRACKET =
         Unpooled.unreleasableBuffer(ByteBufUtil.encodeString(
               UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap("["), Charsets.US_ASCII))
         .asReadOnly();
   private static final ByteBuf PAREN =
         Unpooled.unreleasableBuffer(ByteBufUtil.encodeString(
               UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap("("), Charsets.US_ASCII))
         .asReadOnly();
   private static final ByteBuf PLUS_INF =
         Unpooled.unreleasableBuffer(ByteBufUtil.encodeString(
               UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap("+inf"), Charsets.US_ASCII))
         .asReadOnly();
   private static final ByteBuf MINUS_INF =
         Unpooled.unreleasableBuffer(ByteBufUtil.encodeString(
               UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap("-inf"), Charsets.US_ASCII))
         .asReadOnly();

   /**
    * A marshaller for converting bounds into bytes for communicating with Redis over a socket. This
    * delegates to another marshaller for the actual bound value.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    *
    * @param <T> the type of the bound
    */
   public static class BoundMarshaller<T> implements Marshaller<Bound<T>> {
      private final Marshaller<T> delegate;
      
      public BoundMarshaller(Marshaller<T> delegate) {
         this.delegate = delegate;
      }
      
      @Override
      public ByteBuf toBytes(Bound<T> t, ByteBufAllocator alloc) {
         if (t == POSITIVE_INFINITY) {
            return PLUS;
         } else if (t == NEGATIVE_INFINITY) {
            return MINUS;
         } else {
            return alloc.compositeBuffer(2)
                  .addComponent(true, t.closed ? BRACKET : PAREN)
                  .addComponent(true, delegate.toBytes(t.element, alloc));
         }
      }

      @Override
      public Bound<T> fromBytes(ByteBuf buffer) {
         char first = (char) buffer.readByte();
         boolean more = buffer.isReadable();
         if (first == '+' && !more) {
            return positiveInfinity();
         } else if (first == '-' && !more) {
            return negativeInfinity();
         } else if (first == '[' && more) {
            return closed(delegate.fromBytes(buffer));
         } else if (first == ')' && more) {
            return open(delegate.fromBytes(buffer));
         }
         buffer.readerIndex(buffer.readerIndex() - 1); // "unread" the first byte
         throw new IllegalStateException(
               "Bound cannot be parsed from \"" + debugString(buffer) + "\"");
      }
   }
   
   private static final Bound<?> POSITIVE_INFINITY = new Bound<Object>(null, false) {
      @Override
      public boolean equals(Object o) {
         return o == POSITIVE_INFINITY;
      }
      
      @Override
      public int hashCode() {
         return Double.hashCode(Double.POSITIVE_INFINITY);
      }
      
      @Override
      public String toString() {
         return "+";
      }
   };
   
   private static final Bound<?> NEGATIVE_INFINITY = new Bound<Object>(null, false) {
      @Override
      public boolean equals(Object o) {
         return o == NEGATIVE_INFINITY;
      }
      
      @Override
      public int hashCode() {
         return Double.hashCode(Double.NEGATIVE_INFINITY);
      }
      
      @Override
      public String toString() {
         return "-";
      }
   };
   
   /**
    * The maximum possible bound. This bound is always greater than every value. When used as the
    * max or stop of a range, it means that all values from the min/start up to the end of the set
    * are to be included.
    * 
    * @return the maximum possible bound
    */
   @SuppressWarnings("unchecked")
   public static <T> Bound<T> positiveInfinity() {
      return (Bound<T>) POSITIVE_INFINITY;
   }

   /**
    * The minimum possible bound. This bound is always less than every value. When used as the
    * min or start of a range, it means that all values from the very first up to the max/stop
    * are to be included.
    * 
    * @return the maximum possible bound
    */
   @SuppressWarnings("unchecked")
   public static <T> Bound<T> negativeInfinity() {
      return (Bound<T>) NEGATIVE_INFINITY;
   }

   /**
    * Creates an open bound. An open bound is one that excludes the given value. It means that only
    * values greater than (for a min/start bound) or less than (for a max/stop bound) the given
    * value are included in the range.
    *  
    * @param value the value
    * @return an open bound for the given value
    */
   public static <T> Bound<T> open(T value) {
      return new Bound<>(requireNonNull(value), false);
   }

   /**
    * Creates a closed bound. A closed bound is one that includes the given value. So values that
    * are greater than or equal to (for a min/start bound) or less than or equal to (for a max/stop
    * bound) the given value are included in the range.
    *  
    * @param value the value
    * @return a closed bound for the given value
    */
   public static <T> Bound<T> closed(T value) {
      return new Bound<>(requireNonNull(value), true);
   }

   private final T element;
   private final boolean closed;
   
   private Bound(T element, boolean closed) {
      this.element = element;
      this.closed = closed;
   }
   
   /**
    * Returns the value associated with this bound. This will return {@code null} for
    * {@linkplain #positiveInfinity() infinite} {@linkplain #negativeInfinity() bounds}.
    * 
    * @return the value associated with this bound
    */
   public T get() {
      return element;
   }
   
   /**
    * Returns true if this is a {@linkplain #closed(Object) closed} bound.
    * 
    * @return true if this is a closed bound
    */
   public boolean isClosed() {
      return closed;
   }
   
   /**
    * Returns true if this is an {@linkplain #open(Object) open} bound.
    * 
    * @return true if this is an open bound
    */
   public boolean isOpen() {
      return !closed;
   }
   
   @Override
   public boolean equals(Object o) {
      if (o instanceof Bound) {
         Bound<?> other = (Bound<?>) o;
         return element.equals(other.element) && closed == other.closed;
      }
      return false;
   }
   
   @Override
   public int hashCode() {
      int code = element.hashCode();
      return closed ? ~code : code;
   }
   
   @Override
   public String toString() {
      return (closed ? "[" : "(") + element.toString();
   }
   
   private static String debugString(ByteBuf buffer) {
      byte[] bytes = new byte[buffer.readableBytes()];
      buffer.readBytes(bytes);
      StringBuilder sb = new StringBuilder();
      Response.debugString(sb, bytes);
      return sb.toString();
   }
   
   /**
    * A primitive specialization of {@link Bound} for {@code double} values. This is used for
    * defining ranges over the scores in a sorted set.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    */
   public static class OfDouble extends Bound<Double> {
      /**
       * A marshaller for converting double bounds into bytes and back.
       * 
       * @author Joshua Humphries (jhumphries131@gmail.com)
       */
      public static final Marshaller<OfDouble> MARSHALLER = new Marshaller<OfDouble>() {
         @Override
         public ByteBuf toBytes(OfDouble t, ByteBufAllocator alloc) {
            if (t == POSITIVE_INFINITY) {
               return PLUS_INF;
            } else if (t == NEGATIVE_INFINITY) {
               return MINUS_INF;
            } else {
               ByteBuf val = ByteBufUtil.encodeString(alloc,
                     CharBuffer.wrap(Double.toString(t.value)), Charsets.US_ASCII);
               return t.isClosed()
                     ? val
                     : alloc.compositeBuffer(2)
                      .addComponent(true, PAREN)
                      .addComponent(true, val);
            }
         }

         @Override
         public OfDouble fromBytes(ByteBuf buffer) {
            String str = buffer.toString(Charsets.US_ASCII);
            if (str.equalsIgnoreCase("+inf")) {
               return positiveInfinity();
            } else if (str.equalsIgnoreCase("-inf")) {
               return negativeInfinity();
            } else if (str.charAt(0) == '(') {
               return open(Double.parseDouble(str.substring(1)));
            } else {
               return closed(Double.parseDouble(str));
            }
         }
      };
      
      private static final OfDouble POSITIVE_INFINITY =
            new OfDouble(Double.POSITIVE_INFINITY, true) {
               @Override
               public boolean equals(Object o) {
                  return o == POSITIVE_INFINITY;
               }
               
               @Override
               public int hashCode() {
                  return Double.hashCode(Double.POSITIVE_INFINITY);
               }
               
               @Override
               public String toString() {
                  return "+inf";
               }
            };
      
      private static final OfDouble NEGATIVE_INFINITY =
            new OfDouble(Double.NEGATIVE_INFINITY, true) {
               @Override
               public boolean equals(Object o) {
                  return o == NEGATIVE_INFINITY;
               }
               
               @Override
               public int hashCode() {
                  return Double.hashCode(Double.NEGATIVE_INFINITY);
               }
               
               @Override
               public String toString() {
                  return "-inf";
               }
            };
            
      /**
       * The maximum possible bound, with a value of {@linkplain Double#POSITIVE_INFINITY positive
       * infinity}.
       * 
       * @return the maximum possible bound
       */
      public static OfDouble positiveInfinity() {
         return POSITIVE_INFINITY;
      }

      /**
       * The minimum possible bound, with a value of {@linkplain Double#NEGATIVE_INFINITY negative
       * infinity}.
       * 
       * @return the minimum possible bound
       */
      public static OfDouble negativeInfinity() {
         return NEGATIVE_INFINITY;
      }

      /**
       * Creates an open bound. An open bound is one that excludes the given value. It means that
       * only values greater than (for a min/start bound) or less than (for a max/stop bound) the
       * given value are included in the range.
       *  
       * @param value the value
       * @return an open bound for the given value
       * @throws IllegalArgumentException if the given value is infinite or NaN
       */
      public static OfDouble open(double value) {
         if (Double.isInfinite(value) || Double.isNaN(value)) {
            throw new IllegalArgumentException(
                  "bound value should be finite, instead got " + value);
         }
         return new OfDouble(value, false);
      }

      /**
       * Creates a closed bound. A closed bound is one that includes the given value. So values that
       * are greater than or equal to (for a min/start bound) or less than or equal to (for a
       * max/stop bound) the given value are included in the range.
       *  
       * @param value the value
       * @return a closed bound for the given value
       * @throws IllegalArgumentException if the given value is NaN
       */
      public static OfDouble closed(double value) {
         if (Double.isNaN(value)) {
            throw new IllegalArgumentException(
                  "bound value should be a number, instead got " + value);
         }
         if (Double.isInfinite(value)) {
            return value > 0 ? positiveInfinity() : negativeInfinity();
         }
         return new OfDouble(value, true);
      }
      
      private final double value;
      
      private OfDouble(double value, boolean closed) {
         super(null, closed);
         this.value = value;
      }
      
      @Override public Double get() {
         return value;
      }
      
      /**
       * Returns the value associated with this bound as a primitive value, without boxing.
       * 
       * @return the value associated with this bound
       */
      public double getAsDouble() {
         return value;
      }

      @Override
      public boolean equals(Object o) {
         if (o instanceof OfDouble) {
            OfDouble other = (OfDouble) o;
            return value == other.value && isClosed() == other.isClosed();
         }
         return false;
      }
      
      @Override
      public int hashCode() {
         int code = Double.hashCode(value);
         return isClosed() ? ~code : code;
      }
      
      @Override
      public String toString() {
         return (isClosed() ? "" : "(") + Double.toString(value);
      }
   }
}
