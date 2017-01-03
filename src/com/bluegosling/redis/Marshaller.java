package com.bluegosling.redis;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

/**
 * Marshals data, keys and com.bluegosling.redis.values, from a typed representation to an array of bytes and back.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type that represents the key or value
 */
public interface Marshaller<T> {
   /**
    * Marshals the given value to an array of bytes.
    * 
    * @param t a value
    * @param alloc an allocator, for instantiating buffers
    * @return an array of bytes that is the serialized form of the given value
    */
   ByteBuf toBytes(T t, ByteBufAllocator alloc);
   
   /**
    * Unmarshals the given array of bytes into an object.
    * 
    * @param buffer a buffer of readable bytes
    * @return the object de-serialized from the given bytes
    */
   T fromBytes(ByteBuf buffer);
   
   /**
    * Creates a marshaller from the given functions, useful for using lambdas to define marshalling
    * behavior.
    * 
    * @param marshal a function that marshals com.bluegosling.redis.values to bytes
    * @param unmarshal a function that unmarshals com.bluegosling.redis.values from bytes.
    * @return an object that uses the given functions to perform marshalling and unmarhsalling
    */
   static <T> Marshaller<T> of(BiFunction<T, ByteBufAllocator, ByteBuf> marshal,
         Function<ByteBuf, T> unmarshal) {
      return new Marshaller<T>() {
         @Override
         public ByteBuf toBytes(T t, ByteBufAllocator alloc) {
            return marshal.apply(t, alloc);
         }

         @Override
         public T fromBytes(ByteBuf bytes) {
            return unmarshal.apply(bytes);
         }
      };
   }
   
   static Marshaller<String> ofStrings(Charset charset) {
      return of(
            (s, alloc) -> ByteBufUtil.encodeString(alloc, CharBuffer.wrap(s), charset),
            b -> {
               String ret = b.toString(charset);
               b.readerIndex(b.writerIndex()); // consume the bytes
               return ret;
            });
   }

   static Marshaller<byte[]> ofBytes() {
      return of(
            (b, alloc) -> Unpooled.wrappedBuffer(b),
            b -> {
               byte[] bytes = new byte[b.readableBytes()];
               b.readBytes(bytes);
               return bytes;
            });
   }
}
