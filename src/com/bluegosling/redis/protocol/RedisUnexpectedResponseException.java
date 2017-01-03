package com.bluegosling.redis.protocol;

import com.bluegosling.redis.values.Response;

/**
 * An exception that indicates the wrong kind of response was received from a Redis server.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class RedisUnexpectedResponseException extends RedisResponseException {
   private static final long serialVersionUID = -4069618798315596437L;
   
   private final Response response;

   public RedisUnexpectedResponseException(Response response) {
      this("Not expecting " + response.getKind().name().toLowerCase() + " reply", response);
   }

   public RedisUnexpectedResponseException(String message, Response response) {
      super(message);
      this.response = response;
   }
   
   public Response getResponse() {
      return response;
      
   }
}
