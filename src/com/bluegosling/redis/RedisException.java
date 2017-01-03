package com.bluegosling.redis;

/**
 * An exception thrown from Redis operations.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 */
public class RedisException extends RuntimeException {
   private static final long serialVersionUID = 2144680810179175180L;

   public RedisException() {
   }
   
   public RedisException(String message) {
      super(message);
   }
   
   public RedisException(String message, Throwable cause) {
      super(message, cause);
   }

   public RedisException(Throwable cause) {
      super(cause);
   }
}
