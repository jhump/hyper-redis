package com.bluegosling.redis.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Listens for com.bluegosling.redis.values in reply to Redis commands. As response data is read from the Redis server,
 * it is translated into the various RESP reply types and passed to the listener.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public interface ReplyListener {
   /**
    * Handles a simple reply, also known as a status reply.
    * 
    * @param reply the string reply value
    */
   void onSimpleReply(String reply);
   
   /**
    * Handles an error reply.
    * 
    * @param errorMsg the error message string
    */
   void onErrorReply(String errorMsg);
   
   /**
    * Handles an integer reply. Redis integer replies can be 64-bit com.bluegosling.redis.values, so a Java {@code long}
    * is used instead of {@code int}.
    * 
    * @param value the integer reply value
    */
   void onIntegerReply(long value);
   
   /**
    * Handles a bulk reply, used to encode binary-safe string data. The given object may be
    * {@code null}, indicating a "nil" bulk reply was received. If non-null, the given buffer's
    * readable bytes represent the contents of the bulk reply. The listener is allowed to modify the
    * given buffer, generally via calls to read data from it (which modifies its reader index).
    * 
    * <p>If a reference to the buffer is kept beyond the scope of this method,
    * {@link ByteBuf#retain()} must be used. And when that buffer is no longer needed,
    * {@link ByteBuf#release()} must be called to avoid resource leaks.
    * 
    * @param bytes a buffer with the bytes of the bulk reply
    */
   void onBulkReply(ByteBuf bytes);
   
   /**
    * Handles an array reply, also known as a multi-bulk reply. The returned listener is invoked
    * for each array element. This means that the returned listener must correctly handle being
    * invoked multiple times (once for each array element).
    * 
    * <p>If the given number of elements is zero or less, the returned listener is ignored and may
    * be null. A number of elements that is less than zero indicates a "nil" array reply, distinct
    * from an empty array reply which is indicated via a number of elements of zero.
    * 
    * <p>If an error occurs while reading an array element, only the returned listener will be
    * notified of the failure. If this listener also needs to be notified, a listener should be
    * returned that propagates the failure to this one.
    * 
    * @param numberOfElements the number of elements in the array value
    * @return a listener that will be invoked once for each element in the array
    */
   ReplyListener onArrayReply(int numberOfElements);
   
   /**
    * Handles a failure. This is generally indicative of an I/O exception or something wrong with
    * how responses are received. It could be a sign of using a newer version of Redis, which could
    * add a new RESP reply type that this library was not implemented to handle.
    * 
    * @param th the cause of failure
    */
   void onFailure(Throwable th);
   
   /**
    * A listener that does nothing when invoked. This can be used as a sink for reply com.bluegosling.redis.values when
    * the actual data can be ignored.
    */
   ReplyListener NO_OP = new ReplyListener() {
      @Override
      public void onSimpleReply(String reply) {
      }

      @Override
      public void onErrorReply(String errorMsg) {
      }

      @Override
      public void onIntegerReply(long value) {
      }

      @Override
      public void onBulkReply(ByteBuf bytes) {
      }

      @Override
      public ReplyListener onArrayReply(int numberOfElements) {
         return this;
      }

      @Override
      public void onFailure(Throwable th) {
      }
   };
}