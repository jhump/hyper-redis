package com.bluegosling.redis.protocol;

import com.bluegosling.redis.RedisException;

/**
 * An exception that indicates an error in the response contents from the Redis server.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class RedisResponseException extends RedisException {
   private static final long serialVersionUID = -2146704829595902757L;
   
   public RedisResponseException(String message) {
      super(message);
   }
}
