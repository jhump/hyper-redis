package com.bluegosling.redis.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import org.junit.Test;

import com.bluegosling.redis.RedisException;
import com.bluegosling.redis.concurrent.Callback;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;

public class ResponseProtocolTest {
   
   @Test public void parseResponse() {
      parse("OK", "+OK\r\n");
      parse(new Failure("Error message"), "-Error message\r\n");
      parse(Long.valueOf(1000), ":1000\r\n");
      parse(Long.valueOf(0), ":0\r\n");
      parse(new Bytes("foobar".getBytes()), "$6\r\nfoobar\r\n");
      parse(new Bytes(new byte[0]), "$0\r\n\r\n");
      parse(null, "$-1\r\n");
      parse(Collections.emptyList(), "*0\r\n");
      parse(Arrays.asList(new Bytes("foo".getBytes()), new Bytes("bar".getBytes())),
            "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
      parse(Arrays.asList(Long.valueOf(1), Long.valueOf(2), Long.valueOf(3)),
            "*3\r\n:1\r\n:2\r\n:3\r\n");
      parse(Arrays.asList(
            Long.valueOf(1), Long.valueOf(2), Long.valueOf(3), Long.valueOf(4),
                  new Bytes("foobar".getBytes())),
            "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n");
      parse(null, "*-1\r\n");
      parse(Arrays.asList(
            Arrays.asList(Long.valueOf(1), Long.valueOf(2), Long.valueOf(3)),
            Arrays.asList("Foo", new Failure("Bar"))),
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n");
      parse(Arrays.asList(new Bytes("foo".getBytes()), null, new Bytes("bar".getBytes())),
            "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n");
   }
   
   
   private void parse(Object expected, String replyContents) {
      ByteBuf buffer = ByteBufUtil.encodeString(
            UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap(replyContents), Charsets.UTF_8);
      
      // all at once
      doParse(expected, protocol -> assertTrue(protocol.parseReply(buffer.slice())));

      // one byte at a time
      doParse(expected, protocol -> {
         ByteBuf copy = buffer.slice();
         ByteBuf partial = UnpooledByteBufAllocator.DEFAULT.buffer();
         while (copy.isReadable()) {
            partial.writeBytes(copy, 1);
            assertEquals(!copy.isReadable(), protocol.parseReply(partial));
         }
      });

      // random partial chunks
      doParse(expected, protocol -> {
         Random rnd = new Random(0);
         ByteBuf copy = buffer.slice();
         ByteBuf partial = UnpooledByteBufAllocator.DEFAULT.buffer();
         while (copy.isReadable()) {
            partial.writeBytes(copy, rnd.nextInt(copy.readableBytes()) + 1);
            assertEquals(!copy.isReadable(), protocol.parseReply(partial));
         }
      });
   }
   
   private void doParse(Object expected, Consumer<ResponseProtocol> action) {
      Object[] response = new Object[1];
      ResponseBuilder builder = new ResponseBuilder(new Callback<Object>() {
         @Override
         public void onSuccess(Object t) {
            response[0] = t;
         }

         @Override
         public void onFailure(Throwable th) {
            response[0] = th;
         }
      });
      ResponseProtocol protocol = new ResponseProtocol(builder);
      
      action.accept(protocol);
      
      if (expected instanceof RedisException) {
         assertTrue(response[0] instanceof RedisException);
         assertEquals(((RedisException) expected).getMessage(),
               ((RedisException) response[0]).getMessage());
      } else {
         assertEquals(expected, response[0]);
      }
   }
   
   /**
    * A simple reply listener that constructs a response object. The object passed to the callback
    * on success will be a string, a long, an array of bytes, or a list when the RESP is a simple,
    * integer, bulk, or array response.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    */
   static class ResponseBuilder extends BaseReplyListener<Object> {
      List<Object> list;
      int expectedElements;

      ResponseBuilder(Callback<Object> callback) {
         super(callback);
      }
      
      private void addElement(Object element) {
         assert list.size() < expectedElements;
         list.add(element);
         if (list.size() == expectedElements) {
            callbackSuccess(list);
            list = null;
            expectedElements = 0;
         }
      }
      
      @Override
      public void onSimpleReply(String reply) {
         assert list == null;
         callbackSuccess(reply);
      }

      @Override
      public void onIntegerReply(long value) {
         assert list == null;
         callbackSuccess(value);
      }

      @Override
      public void onErrorReply(String message) {
         assert list == null;
         callbackSuccess(new Failure(message));
      }

      @Override
      public void onBulkReply(ByteBuf bytes) {
         assert list == null;
         if (bytes == null) {
            callbackSuccess(null);
         } else {
            byte[] byteArray = new byte[bytes.readableBytes()];
            bytes.readBytes(byteArray);
            callbackSuccess(new Bytes(byteArray));
         }
      }

      @Override
      public ReplyListener onArrayReply(int numberOfElements) {
         assert list == null;
         if (numberOfElements == -1) {
            callbackSuccess(null);
            return null;
         } else if (numberOfElements == 0) {
            callbackSuccess(Collections.emptyList());
            return null;
         } else {
            expectedElements = numberOfElements;
            list = new ArrayList<>(expectedElements);
            return new ResponseBuilder(new Callback<Object>() {
               @Override
               public void onSuccess(Object t) {
                  addElement(t);
               }

               @Override
               public void onFailure(Throwable th) {
                  ResponseBuilder.this.onFailure(th);
               }
            });
         }
      }
   }
   
   /**
    * A simple wrapper for a byte array that provides a sensible implementation of equals and
    * hashCode.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    */
   static class Bytes {
      private final byte[] bytes;
      
      Bytes(byte[] bytes) {
         this.bytes = bytes;
      }
      
      @Override public int hashCode() {
         return Arrays.hashCode(bytes);
      }
      
      @Override public boolean equals(Object o) {
         return o instanceof Bytes && Arrays.equals(bytes, ((Bytes) o).bytes);
      }
      
      @Override public String toString() {
         return Arrays.toString(bytes);
      }
   }
   
   static class Failure {
      private final String message;
      
      Failure(String message) {
         this.message = message;
      }
      
      @Override public int hashCode() {
         return message.hashCode();
      }
      
      @Override public boolean equals(Object o) {
         return o instanceof Failure && Objects.equal(message, ((Failure) o).message);
      }
      
      @Override public String toString() {
         return "Failure: " + message;
      }
   }
}
