package com.bluegosling.redis.values;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;

import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;

/**
 * A Redis response. Results of Redis operations are sent to clients using RESP, the REdis
 * Serialization Protocol, which allows for conveying com.bluegosling.redis.values that are strings, bytes, integers,
 * errors, or arrays thereof.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public final class Response {
   private static final Response[] EMPTY_RESPONSES = new Response[0];
   private static final Response EMPTY_ARRAY_RESPONSE = new Response(EMPTY_RESPONSES);
   private static final Response NIL_ARRAY_RESPONSE = new Response((Response[]) null);

   private static final byte[] EMPTY_BYTES = new byte[0];
   private static final Response EMPTY_BULK_RESPONSE = new Response(EMPTY_BYTES);
   private static final Response NIL_BULK_RESPONSE = new Response((byte[]) null);
   
   /**
    * A visitor of a response. This implements the visitor pattern, using double dispatch, for
    * interacting with the various kinds of responses.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    *
    * @param <P> the type of extra parameter that the visitor accepts
    * @param <R> the type of result of visiting
    */
   public interface Visitor<P, R> {
      /**
       * Visits a simple response, also called a status response. This is often just the string
       * "OK" as a token of a completed operation. But some operations return other string data via
       * a simple response.
       */
      R visitSimple(String resp, P param);

      /**
       * Visits an error response.
       */
      R visitError(String error, P param);
      
      /**
       * Visits an integer response. Despite the name's similarity to Java's primitive {@code int},
       * the value is a {@code long} because Redis integer replies support 64-bit com.bluegosling.redis.values.
       */
      R visitInt(long value, P param);

      /**
       * Visits a bulk response. A bulk response conveys string or binary data. The value is
       * provided as an array of bytes.
       */
      R visitBulk(byte[] bytes, P param);
      
      /**
       * Visits an array response, also called a multi-bulk response. This response is an array of
       * other response kinds, which can include nested arrays.
       */
      R visitArray(Response[] array, P param);
   }
   
   /**
    * The kind of a Redis response.
    * 
    * @author Joshua Humphries (jhumphries131@gmail.com)
    *
    */
   public enum Kind {
      /**
       * A simple response, also called a status response.
       */
      SIMPLE,
      /**
       * An error response.
       */
      ERROR,
      /**
       * An integer response.
       */
      INT,
      /**
       * A bulk response (for binary-safe strings).
       */
      BULK,
      /**
       * An array response, also called a multi-bulk response.
       */
      ARRAY
   }
   
   /**
    * Creates a simple response.
    * 
    * @param resp the value of the simple response
    * @return a simple response
    */
   public static Response simpleResponse(String resp) {
      return new Response(Kind.SIMPLE, requireNonNull(resp));
   }

   /**
    * Creates an error response.
    * 
    * @param errorMessage the error message com.bluegosling.redis.values for the error response
    * @return an error response
    */
   public static Response errorResponse(String errorMessage) {
      return new Response(Kind.ERROR, requireNonNull(errorMessage));
   }

   /**
    * Creates an integer response.
    * 
    * @param value the value of the integer response
    * @return an integer response
    */
   public static Response intResponse(long value) {
      return new Response(value);
   }

   /**
    * Creates a bulk response.
    * 
    * @param bytes a byte array for the value of the bulk response
    * @return a bulk response
    */
   public static Response bulkResponse(byte[] bytes) {
      if (bytes == null) {
         return NIL_BULK_RESPONSE;
      } else if (bytes.length == 0) {
         return EMPTY_BULK_RESPONSE;
      }
      return new Response(bytes);
   }

   /**
    * Creates a bulk response.
    * 
    * @param bytes a byte buffer for the value of the bulk response
    * @return a bulk response
    */
   public static Response bulkResponse(ByteBuf bytes) {
      if (bytes == null) {
         return NIL_BULK_RESPONSE;
      } else if (!bytes.isReadable()) {
         return EMPTY_BULK_RESPONSE;
      }
      byte[] byteArray = new byte[bytes.readableBytes()];
      bytes.getBytes(bytes.readerIndex(), byteArray);
      return new Response(byteArray);
   }

   /**
    * Creates a bulk response. The given string is converted to bytes via UTF 8 encoding.
    * 
    * @param string a string for the value of the bulk response
    * @return a bulk response
    */
   public static Response bulkResponse(String string) {
      return bulkResponse(string, Charsets.UTF_8);
   }

   /**
    * Creates a bulk response. The given string is converted to byte via the given character set.
    * 
    * @param string a string for the value of the bulk response
    * @param charset the character set used to encode the string to bytes
    * @return a bulk response
    */
   public static Response bulkResponse(String string, Charset charset) {
      if (string == null) {
         return NIL_BULK_RESPONSE;
      } else if (string.length() == 0) {
         return EMPTY_BULK_RESPONSE;
      }
      ByteBuffer buffer = charset.encode(CharBuffer.wrap(string));
      byte[] bytes = new byte[buffer.position()];
      buffer.get(bytes);
      return bulkResponse(bytes);
   }
   
   /**
    * Returns an empty bulk response.
    * 
    * @return an empty bulk response
    */
   public static Response emptyBulkResponse() {
      return EMPTY_BULK_RESPONSE;
   }

   /**
    * Returns a "nil" bulk response. When a Redis bulk response indicates a length of -1, the
    * response is considered a "nil" response. This is distinct from an empty response, where the
    * length will be zero.
    *  
    * @return a nil bulk response
    */
   public static Response nilBulkResponse() {
      return NIL_BULK_RESPONSE;
   }
   
   /**
    * Creates an array response.
    * 
    * @param responses the elements of the array response
    * @return an array response
    */
   public static Response arrayResponse(Response... responses) {
      if (responses == null) {
         return NIL_ARRAY_RESPONSE;
      } else if (responses.length == 0) {
         return EMPTY_ARRAY_RESPONSE;
      }
      return new Response(responses);
   }

   /**
    * Creates an array response.
    * 
    * @param responses the elements of the array response
    * @return an array response
    */
   public static Response arrayResponse(Collection<Response> responses) {
      if (responses == null) {
         return NIL_ARRAY_RESPONSE;
      } else if (responses.isEmpty()) {
         return EMPTY_ARRAY_RESPONSE;
      }
      return new Response(responses.toArray(EMPTY_RESPONSES));
   }

   /**
    * Returns an empty array response.
    * 
    * @return an empty array response
    */
   public static Response emptyArrayResponse() {
      return EMPTY_ARRAY_RESPONSE;
   }

   /**
    * Returns a "nil" array response. When a Redis array response indicates a size of -1, the
    * response is considered a "nil" response. This is distinct from an empty response, where the
    * size will be zero.
    *  
    * @return a nil array response
    */
   public static Response nilArrayResponse() {
      return NIL_ARRAY_RESPONSE;
   }

   /**
    * Converts a bulk response value from an array of bytes to a string. The bytes are decoded using
    * UTF 8.
    * 
    * @param bulkResponse an array of bytes
    * @return a string decoded from the given bytes
    */
   public static String bulkAsString(byte[] bulkResponse) {
      return bulkAsString(bulkResponse, Charsets.UTF_8);
   }

   /**
    * Converts a bulk response from an array of bytes to a string. The bytes are decoded using the
    * given character set.
    * 
    * @param bulkResponse an array of bytes
    * @param charset the character set used to decode a string from bytes
    * @return a string decoded from the given bytes
    */
   public static String bulkAsString(byte[] bulkResponse, Charset charset) {
      return charset.decode(ByteBuffer.wrap(bulkResponse)).toString();
   }

   private final Kind kind;
   private final long intValue;
   private final Object objValue;
   
   private Response(Kind kind, String value) {
      assert kind == Kind.SIMPLE || kind == Kind.ERROR;
      this.kind = kind;
      this.objValue = value;
      this.intValue = 0;
   }

   private Response(byte[] value) {
      this.kind = Kind.BULK;
      this.objValue = value;
      this.intValue = 0;
   }

   private Response(Response[] value) {
      this.kind = Kind.ARRAY;
      this.objValue = value;
      this.intValue = 0;
   }

   private Response(long value) {
      this.kind = Kind.INT;
      this.objValue = null;
      this.intValue = value;
   }

   /**
    * Gets the underlying value of this response. If this is a simple or error response, the value
    * is a string. If it is an integer response, the value is a {@code long}. If it is a bulk
    * response, the value is an array of bytes. If it is an array response, the value is an array of
    * other {@link Response} objects.
    * 
    * @return the underlying value of this response
    */
   public Object getValue() {
      if (kind == Kind.INT) {
         return intValue;
      } else if (kind == Kind.BULK) {
         return ((byte[]) objValue).clone();
      } else if (kind == Kind.ARRAY) {
         return ((Response[]) objValue).clone();
      } else {
         return objValue;
      }
   }
   
   /**
    * Returns the kind of this response.
    * 
    * @return the kind of this response
    */
   public Kind getKind() {
      return kind;
   }
   
   /**
    * Returns true if this response is either a "nil" bulk response or a "nil" array response.
    * 
    * @return true if this is a nil response
    */
   public boolean isNil() {
      return kind != Kind.INT && objValue == null;
   }
   
   /**
    * Accepts the given visitor, invoking the corresponding method on the visitor based on the kind
    * of this response.
    * 
    * @param visitor the visitor
    * @param param an extra parameter passed to the visitor
    * @return the result of invoking a method on the visitor
    */
   public <P, R> R accept(Visitor<? super P, ? extends R> visitor, P param) {
      switch (kind) {
      case SIMPLE:
         return visitor.visitSimple((String) objValue, param);
      case ERROR:
         return visitor.visitError((String) objValue, param);
      case INT:
         return visitor.visitInt(intValue, param);
      case BULK:
         return visitor.visitBulk(((byte[]) objValue).clone(), param);
      case ARRAY:
         return visitor.visitArray(((Response[]) objValue).clone(), param);
      default:
         throw new AssertionError("unknown response kind: " + kind);   
      }
   }
   
   @Override public boolean equals (Object o) {
      if (o instanceof Response) {
         Response other = (Response) o;
         return kind == other.kind && intValue == other.intValue && objValue.equals(other.objValue);
      }
      return false;
   }
   
   @Override public int hashCode() {
      return kind.hashCode() ^ (kind == Kind.INT ? Long.hashCode(intValue) : objValue.hashCode());
   }
   
   @Override public String toString() {
      StringBuilder sb = new StringBuilder().append(kind).append(" Response: ");
      switch (kind) {
      case INT:
         sb.append(intValue);
         break;
      case BULK:
         debugString(sb, (byte[]) objValue);
         break;
      case ARRAY:
         sb.append(Arrays.toString((Response[]) objValue));
         break;
      default:
         sb.append(objValue);
      }
      return sb.toString();
   }
   
   static void debugString(StringBuilder sb, byte[] bytes) {
      for (byte b : bytes) {
         if (b >= 32 && b <= 126) {
            // printable
            sb.append((char) b);
         } else {
            // not printable or part of non-ascii code point
            sb.append("\\x");
            if (b < 16) {
               sb.append("0"); // leading zero
            }
            sb.append(Integer.toHexString(b));
         }
      }
   }
}
