package com.bluegosling.redis.protocol;

import com.bluegosling.redis.RedisException;

/**
 * An exception that indicates a problem encountering while constructing a request message.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class RedisRequestException extends RedisException {
   private static final long serialVersionUID = 7246500954812519110L;

   public RedisRequestException(String message) {
      super(message);
   }
}
