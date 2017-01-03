package com.bluegosling.redis.channel;

import com.bluegosling.redis.RedisException;

public class RedisChannelException extends RedisException {
   private static final long serialVersionUID = -3349940134769131268L;

   public RedisChannelException(String message) {
      super(message);
   }
   
   public RedisChannelException(String message, Throwable cause) {
      super(message, cause);
   }

   public RedisChannelException(Throwable cause) {
      super(cause);
   }
}
