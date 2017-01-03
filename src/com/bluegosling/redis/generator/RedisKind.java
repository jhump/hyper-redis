package com.bluegosling.redis.generator;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * The kinds of {@code Redis} interfaces generated. These correspond to the main block of
 * commands and represent a subset of commands in the block.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
enum RedisKind {
   /**
    * A subset of commands that are read-only and operate on keys in the Redis database. Other
    * kinds of read-only operations, like that operate on metadata, are not included. This subset
    * of commands is used for "watch" operations, for optimistic concurrency in transactions.
    */
   READONLY_KEY_COMMANDS("%sReadOnlyKeyCommands", false),

   /**
    * A subset of commands that can be performed against a read-only slave. This is a strict
    * superset of those in {@link #READONLY_KEY_COMMANDS}.
    */
   READONLY_COMMANDS("%sReadOnlyCommands", false, READONLY_KEY_COMMANDS),

   /**
    * All of the commands in the main {@code Redis} block.
    */
   COMMANDS("%sCommands", false, READONLY_COMMANDS),

   /**
    * A normal interface that implements all commands. This interface includes additional methods
    * for accessing a transactor (for Redis versions greater than or equal to 1.2, when
    * transaction support was introduced). 
    */
   STANDARD("%s", true, COMMANDS),

   /**
    * A watching interface, which only implements the read-only key commands. This interface also
    * includes methods for watching and unwatching keys.
    */
   WATCHING("Watching%s", true, READONLY_KEY_COMMANDS),

   /**
    * A transacting interface, which just queues operations and returns promises for them. This
    * interface also includes methods for executing and discarding the transaction.
    */
   TRANSACTING("Transacting%s", true);
   
   private final String classNameFormat;
   private final boolean includeImpl;
   private final Set<RedisKind> superKinds;
   
   RedisKind(String classNameFormat, boolean includeImpl, RedisKind... superKinds) {
      this.classNameFormat = classNameFormat;
      this.includeImpl = includeImpl;
      this.superKinds = ImmutableSet.copyOf(superKinds);
   }
   
   public String getGeneratedClassName(String blockName) {
      return String.format(classNameFormat, blockName);
   }
   
   public boolean includeImplementation() {
      return includeImpl;
   }
   
   public Set<RedisKind> superKinds() {
      return superKinds;
   }
}