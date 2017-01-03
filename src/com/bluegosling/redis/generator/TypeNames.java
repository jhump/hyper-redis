package com.bluegosling.redis.generator;

import static com.google.common.base.Preconditions.checkState;

import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.antlr.v4.runtime.Token;

import com.bluegosling.redis.generator.HyperRedisParser.DeclaredTypeContext;
import com.bluegosling.redis.generator.HyperRedisParser.ParamContext;
import com.bluegosling.redis.generator.HyperRedisParser.QualifiedIdentifierContext;
import com.bluegosling.redis.generator.HyperRedisParser.SyntheticParamContext;
import com.bluegosling.redis.generator.HyperRedisParser.TypeArgContext;
import com.bluegosling.redis.generator.HyperRedisParser.TypeContext;
import com.bluegosling.redis.generator.HyperRedisParser.WildcardTypeContext;
import com.bluegosling.redis.values.Bound;
import com.bluegosling.redis.values.RedisInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;

public final class TypeNames {
   private TypeNames() { throw new AssertionError(); }
   
   private static final Map<String, TypeName> PRIMITIVES =
         ImmutableMap.<String, TypeName>builder()
               .put("boolean", TypeName.BOOLEAN)
               .put("byte", TypeName.BYTE)
               .put("short", TypeName.SHORT)
               .put("char", TypeName.CHAR)
               .put("int", TypeName.INT)
               .put("long", TypeName.LONG)
               .put("double", TypeName.DOUBLE)
               .put("float", TypeName.FLOAT)
               .put("void", TypeName.VOID)
               .build();
   private static final Map<String, TypeVariableName> TYPE_VARS =
         ImmutableMap.<String, TypeVariableName>builder()
               .put("K", TypeVariableName.get("K"))
               .put("FK", TypeVariableName.get("K")) // FK is alias for K type, but for field keys in a hash
               .put("V", TypeVariableName.get("V"))
               .build();
   private static final Map<String, ClassName> SPECIAL_CASE_CLASSES =
         ImmutableMap.<String, ClassName>builder()
               .put("Entry", ClassName.get(Entry.class))
               .put("Instant", ClassName.get(Instant.class))
               .put("HostAndPort", ClassName.get(HostAndPort.class))
               .put("Bound.OfDouble", ClassName.get(Bound.OfDouble.class))
               .put("RedisInfo.Section", ClassName.get(RedisInfo.Section.class))
               .build();
   private static final List<String> STANDARD_PACKAGES = ImmutableList.of(
         "java.lang", "java.util", "com.google.common.collect", "com.bluegosling.redis.values",
         "com.bluegosling.redis.protocol");
   
   public static TypeName fromParseTree(ParamContext ctx) {
      TypeName type = fromParseTree(ctx.paramDef().type());
      return ctx.ArrayIndicator() != null
            ? ArrayTypeName.of(type)
            : type;
   }
   
   public static TypeName fromParseTree(SyntheticParamContext ctx) {
      return fromParseTree(ctx.synthetic().type());
   }
   
   public static TypeName fromParseTree(TypeContext ctx) {
      TypeName type = fromParseTree(ctx.declaredType());
      int arrayDepth = ctx.ArrayIndicator().size();
      for (int i = 0; i < arrayDepth; i++) {
         type = ArrayTypeName.of(type);
      }
      return type;
   }

   public static TypeName fromParseTree(DeclaredTypeContext ctx) {
      TypeName type = fromParseTree(ctx.qualifiedIdentifier());
      if (ctx.typeArgs() == null) {
         return type;
      }
      checkState(type instanceof ClassName, "only declared class types can have type args");
      ClassName classType = (ClassName) type;
      
      List<TypeArgContext> parseTreeArgs = ctx.typeArgs().typeArg();
      TypeName[] args = new TypeName[parseTreeArgs.size()];
      int i = 0;
      for (TypeArgContext t : parseTreeArgs) {
         if (t.type() != null) {
            args[i++] = fromParseTree(t.type());
         } else {
            args[i++] = fromParseTree(t.wildcardType());
         }
      }
      return ParameterizedTypeName.get(classType, args);
   }
   
   public static TypeName fromParseTree(QualifiedIdentifierContext ctx) {
      String name = ctx.getText();
      TypeName primitive = PRIMITIVES.get(name);
      if (primitive != null) {
         return primitive;
      }
      
      TypeVariableName typeVar = TYPE_VARS.get(name);
      if (typeVar != null) {
         return typeVar;
      }

      ClassName type = SPECIAL_CASE_CLASSES.get(name);
      if (type != null) {
         return type;
      }

      // If it's not a special-cased class name, try to resolve it
      int pos = name.lastIndexOf('.');
      if (pos >= 0) {
         // fully-qualified type name
         return ClassName.get(name.substring(0, pos), name.substring(pos + 1));
      } else {
         // look in standard packages
         for (String packageName : STANDARD_PACKAGES) {
            try {
               Class<?> clazz = Class.forName(packageName + "." + name);
               if (Modifier.isPublic(clazz.getModifiers())) {
                  return ClassName.get(clazz);
               }
            } catch (ClassNotFoundException e) {
               // fall-through
            }
         }
         Token start = ctx.start;
         throw new IllegalArgumentException("Failed to resolve class name: " + name
               + ", line " + start.getLine() + ", column " + start.getCharPositionInLine());
      }
   }
   
   public static TypeName fromParseTree(WildcardTypeContext ctx) {
      if (ctx.wildcardBound() == null) {
         return WildcardTypeName.subtypeOf(Object.class);
      }
      if ("extends".equals(ctx.wildcardBound().getChild(0).getText())) {
         return WildcardTypeName.subtypeOf(fromParseTree(ctx.wildcardBound().type()));
      } else {
         assert "super".equals(ctx.wildcardBound().getChild(0).getText());
         return WildcardTypeName.supertypeOf(fromParseTree(ctx.wildcardBound().type()));
      }
   }
}