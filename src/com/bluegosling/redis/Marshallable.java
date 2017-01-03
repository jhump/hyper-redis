package com.bluegosling.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * An object that can marshal itself to bytes.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public interface Marshallable {
   /**
    * Marshals this object to bytes. Marshalling returns a {@link ByteBuf} and should use the given
    * {@link ByteBufAllocator} to construct buffers into which the data are written.
    * 
    * @param alloc an allocator of buffers
    * @return a buffer with the marshalled bytes that represent this object
    */
   ByteBuf marshall(ByteBufAllocator alloc);
}
