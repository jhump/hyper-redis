package com.bluegosling.redis.protocol;

import com.bluegosling.redis.values.Response;
import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Implements the Redis response protocol by parsing received data and sending to a
 * {@link ReplyListener}. A single response protocol object handles exactly one response message
 * from a Redis server.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class ResponseProtocol {
   /**
    * Parses response data and invokes the given reply listener as needed. Implicitly forms a stack
    * of parsers, for properly parsing nested array replies.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    */
   private static class RespParser {
      private final ReplyListener listener;
      private final RespParser previous;
      private Response.Kind current;
      private boolean expectingCrLf;
      private ReplyListener elementListener;
      private int awaitingElements;
      
      /**
       * Constructs a new parser that forwards reply data to the given listener. If this parser
       * is reading a nested array element, the previous parser given represents the enclosing
       * response. If the given previous parser is {@code null} then this is a top-level parser.
       * 
       * @param listener the listener that receives reply data as it is parsed
       * @param previous the previous parser in the stack
       */
      RespParser(ReplyListener listener, RespParser previous) {
         this.listener = listener;
         this.previous = previous;
      }
      
      void onFailure(Throwable th) {
         listener.onFailure(th);
      }
      
      /**
       * Parses the given buffer of bytes. If the given buffer represents an incomplete response,
       * either the current parser or a child parser (for parsing enclosed array elements) is
       * returned. If the buffer is a complete response, the parser only reads bytes up to the end
       * of the response and then returns the previous parser (which transfers control to a parent
       * parser in the event that the responses is itself an element of an array).
       * 
       * <p>The listener is invoked as sufficient data is read from the reply.
       * 
       * @param buf the buffer with readable bytes representing a response
       * @return {@code this} if more bytes are needed, a new (child) parser if subsequent bytes
       *    represent elements in a nested array, or the previous parser if the response is
       *    completely parsed
       */
      RespParser parseResp(ByteBuf buf) {
         while (buf.isReadable()) {
            if (expectingCrLf) {
               if (buf.readableBytes() < 2) {
                  return this;
               }
               assert buf.toString(buf.readerIndex(), 2, Charsets.US_ASCII).equals("\r\n");
               // skip over the CRLF
               buf.readerIndex(buf.readerIndex() + 2);
               expectingCrLf = false;
               // if awaitingElements == 0 and type is BULK, we fall through below and end up
               // needing to consume another "\r\n" tuple
               if (awaitingElements < 0
                     || (awaitingElements == 0 && current == Response.Kind.ARRAY)) {
                  return previous;
               }
            }
            if (current == null) {
               current = readRespType(buf);
               awaitingElements = -1;
            }
            if (awaitingElements < 0) {
               // length not yet known: scanning for "\r"
               int nextIndexToScan = buf.readerIndex() - awaitingElements - 1;
               int foundIndex = -1;
               for (int i = nextIndexToScan; i < buf.writerIndex(); i++) {
                  if (buf.getByte(i) == (byte) '\r') {
                     foundIndex = i;
                     break;
                  }
               }
               if (foundIndex == -1) {
                  // need more data
                  awaitingElements = -buf.readableBytes() - 1;
                  return this;
               }
               String str = buf.toString(buf.readerIndex(), foundIndex - buf.readerIndex(),
                     Charsets.UTF_8);
               buf.readerIndex(foundIndex);
               if (current == Response.Kind.SIMPLE) {
                  listener.onSimpleReply(str);
               } else if (current == Response.Kind.ERROR) {
                  listener.onErrorReply(str);
               } else if (current == Response.Kind.INT) {
                  listener.onIntegerReply(Long.parseLong(str));
               } else  {
                  awaitingElements = Integer.parseInt(str);
                  if (awaitingElements <= 0) {
                     if (current == Response.Kind.ARRAY) {
                        listener.onArrayReply(awaitingElements);
                     } else {
                        assert current == Response.Kind.BULK;
                        // count of -1 means a "nil" response
                        listener.onBulkReply(awaitingElements == 0 ? Unpooled.EMPTY_BUFFER : null);
                     }
                  } else if (current == Response.Kind.ARRAY) {
                     elementListener = listener.onArrayReply(awaitingElements);
                  }
               }
               expectingCrLf = true;
            } else if (current == Response.Kind.ARRAY) {
               if (awaitingElements == 0) {
                  return previous;
               }
               awaitingElements--;
               return new RespParser(elementListener, this);
            } else {
               assert current == Response.Kind.BULK;
               if (buf.readableBytes() < awaitingElements) {
                  // need more bytes!
                  return this;
               }
               listener.onBulkReply(buf.readSlice(awaitingElements));
               expectingCrLf = true;
               awaitingElements = -1;
            }
         }
         // hit end of buffer: unless we've read last element in an array, then we need more
         return current == Response.Kind.ARRAY && awaitingElements == 0 && !expectingCrLf
               ? previous
               : this;
      }
      
      /**
       * Reads a single character from the buffer to determine the type of RESP reply.
       * 
       * @param buf the buffer with response data
       * @return the kind of the response
       */
      private Response.Kind readRespType(ByteBuf buf) {
         char ch = (char) buf.readByte();
         switch (ch) {
         case '+':
            return Response.Kind.SIMPLE;
         case '-':
            return Response.Kind.ERROR;
         case ':':
            return Response.Kind.INT;
         case '$':
            return Response.Kind.BULK;
         case '*':
            return Response.Kind.ARRAY;
         default:
            RedisResponseException e = new RedisResponseException("Incorrect RESP encoding: " + ch);
            listener.onFailure(e);
            throw e;
         }
      }
   }
   
   private RespParser parser;
   
   /**
    * Creates a new response protocol that sends the response data to the given listener.
    * 
    * @param listener a listener that will consume the response data
    */
   public ResponseProtocol(ReplyListener listener) {
      this.parser = new RespParser(listener, null);
   }
   
   /**
    * Parses a response from the given buffer. The buffer may be read in whole or in part. If the
    * method returns false, more bytes must be read to parse a response. When the method returns
    * true, a complete response has been parsed and any remaining readable bytes in the buffer are
    * part of a subsequent response message.
    * 
    * <p>When false is returned, remaining readable data in the buffer must be provided again in
    * a subsequent invocation. The subsequent invocation must also include more bytes or no more
    * progress can be made.
    * 
    * @param buf a buffer of response data
    * @return true if a response is parsed or false if more data needs to be read.
    */
   public boolean parseReply(ByteBuf buf) {
      while (parser != null) {
         RespParser next = parser.parseResp(buf);
         if (next == parser) {
            return false;
         }
         parser = next;
      }
      return true;
   }
   
   /**
    * Indicates an error has occurred reading response bytes from the server.
    * 
    * @param th the cause of failure in reading a response
    */
   public void onFailure(Throwable th) {
      if (parser != null) {
         parser.onFailure(th);
      }
   }
}
