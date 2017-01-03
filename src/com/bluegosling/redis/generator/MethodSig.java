package com.bluegosling.redis.generator;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;

/**
 * Represents a method signature.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
class MethodSig {
   final String name;
   final List<TypeName> args;

   MethodSig(MethodSpec.Builder method) {
      MethodSpec spec = method.build();
      this.name = spec.name;
      this.args = Lists.transform(spec.parameters, p -> p.type);
   }
   
   @Override
   public boolean equals(Object o) {
      if (o instanceof MethodSig) {
         MethodSig other = (MethodSig) o;
         return name.equals(other.name) && args.equals(other.args);
      }
      return false;
   }
   
   @Override
   public int hashCode() {
      return Objects.hash(name, args);
   }
   
   @Override
   public String toString() {
      return args.stream()
            .map(TypeName::toString)
            .collect(Collectors.joining(",", name + "(", ")"));
   }
}