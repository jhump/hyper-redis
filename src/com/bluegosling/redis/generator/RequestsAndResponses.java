package com.bluegosling.redis.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.bluegosling.redis.generator.HyperRedisParser.CommandContext;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

public final class RequestsAndResponses {
   private RequestsAndResponses() { throw new AssertionError(); }
   
   public interface ExpressionWriter {
      void prepare(CodeBlock prepBlock);
      void finish(CodeBlock finalExpression);
   }

   /**
    * Generates code for building a request with the given piece of request data.
    * 
    * @param identifier an identifier that represents the request value to be written
    * @param type the type of the request value
    * @param w the writer, used to emit code blocks that will marshal the request value
    */
   public static void generateRequest(String identifier, TypeName type, ExpressionWriter w) {
      // TODO
   }
   
   /**
    * Generates code for building a reply listener that will handle the response.
    * 
    * @param streaming true if the response is a streaming response
    * @param type the type of the response value
    * @param w the writer, used to emit code blocks that will construct the reply listener
    */
   public static void generateResponse(boolean streaming, TypeName type, ExpressionWriter w) {
      // TODO
   }
   
   public static void generateImplementation(CommandContext cmd, Map<String, TypeName> args,
         CodeBlock.Builder methodBody) {
      List<String> identifiers = new ArrayList<>(args.keySet());
      
      // TODO
      
   }
   
   public static void generateImplementationWithBuilder(CommandContext cmd,
         Map<String, TypeName> args, CodeBlock.Builder methodBody) {
      // TODO
   }
   
   public static TypeSpec generateBuilder(CommandContext cmd, Map<String, TypeName> args) {
      // TODO
      return null;
   }
}
