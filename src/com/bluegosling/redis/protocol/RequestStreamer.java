package com.bluegosling.redis.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Used to create a request to a Redis server. Callers emit a stream of tokens that are assembled
 * into a properly formed request. The streamer consumes the stream of tokens, buffering the
 * request fully and then sending to the server once the stream is {@linkplain #finish() finished}.
 * 
 * <p>Callers must either call {@link #finish} or {@link #error} to avoid resource leaks.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public interface RequestStreamer {
   /**
    * Indicates a token in the request stream. This token is appended to any tokens already
    * provided. The given value is encoded via UTF-8.
    * 
    * @param token a token in the request
    * @return {@code this}, for method chaining
    */
   RequestStreamer nextToken(String token);

   /**
    * Indicates a token in the request stream. This token is appended to any tokens already
    * provided.
    * 
    * @param token a binary token in the request
    * @return {@code this}, for method chaining
    */
   RequestStreamer nextToken(ByteBuf token);
   
   /**
    * Indicates that all tokens in the request stream have been provided. The request will be
    * sent to the server.
    */
   void finish();
   
   /**
    * Indicates that an error occurred in building the request. This releases any buffers already
    * created to construct the request message.
    * 
    * @param th the cause of failure
    */
   void error(Throwable th);
}
