package com.bluegosling.redis.transaction;

import com.bluegosling.redis.RedisException;

/**
 * An exception that is thrown when a transaction fails. This should not normally occur as it
 * generally indicates a syntax problem with a request. It most likely indicates that the wrong
 * version of the Redis API is being used (e.g. using a newer version API and issuing a command that
 * the actual Redis server doesn't understood).
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class TransactionFailedException extends RedisException {
   private static final long serialVersionUID = 4432787701410537399L;

   public TransactionFailedException(String message) {
      super(message);
   }
   
   public TransactionFailedException(String message, Throwable cause) {
      super(message, cause);
   }

   public TransactionFailedException(Throwable cause) {
      super(cause);
   }
}
