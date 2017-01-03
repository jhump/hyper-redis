package com.bluegosling.redis.channel;

public class RedisChannelAcquisitionException extends RedisChannelException {

   private static final long serialVersionUID = -7714263272126630368L;

   public RedisChannelAcquisitionException(String message) {
      super(message);
   }

   public RedisChannelAcquisitionException(String message, Throwable cause) {
      super(message, cause);
   }

   public RedisChannelAcquisitionException(Throwable cause) {
      super(cause);
   }
}
