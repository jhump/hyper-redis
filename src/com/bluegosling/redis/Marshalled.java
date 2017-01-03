package com.bluegosling.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * An object that can be marshalled to bytes via a {@link Marshaller}. This is basically a tuple
 * that represents a value and its associated marshaller.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of underlying value
 */
class Marshalled<T> implements Marshallable {
   private final T value;
   private final Marshaller<? super T> marshaller;

   /**
    * Constructs a new object that wraps the given value and associated marshaller.
    * 
    * @param value a value
    * @param marshaller the marshaller for the given value
    */
   Marshalled(T value, Marshaller<? super T> marshaller) {
      this.value = value;
      this.marshaller = marshaller;
   }
   
   @Override
   public ByteBuf marshall(ByteBufAllocator alloc) {
      return marshaller.toBytes(value, alloc);
   }
}
