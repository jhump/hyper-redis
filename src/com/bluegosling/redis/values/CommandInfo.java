package com.bluegosling.redis.values;

import static java.util.Objects.requireNonNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class CommandInfo {
   public enum CommandFlag {
      WRITE,
      READ_ONLY,
      DENY_OOM,
      ADMIN,
      PUBSUB,
      NOSCRIPT,
      RANDOM,
      SORT_FOR_SCRIPTS,
      LOADING,
      STALE,
      SKIP_MONITOR,
      ASKING,
      FAST,
      MOVABLE_KEYS;
      
      private static Map<String, CommandFlag> reverseIndex;
      static {
         CommandFlag[] flags = values();
         Map<String, CommandFlag> map = new HashMap<>(flags.length * 4 / 3);
         for (CommandFlag f : flags) {
            map.put(f.flagName(), f);
         }
         reverseIndex = Collections.unmodifiableMap(map);
      }
      
      private final String flagName;
      
      CommandFlag() {
         String name = this.name();
         StringBuilder sb = new StringBuilder(name.length());
         for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (ch == '_') {
               continue;
            }
            sb.append(Character.toLowerCase(ch));
         }
         flagName = sb.toString();
      }
      
      public String flagName() {
         return flagName;
      }
      
      public static CommandFlag lookup(String flagName) {
         return reverseIndex.get(flagName);
      }
      
   }
   
   private final String name;
   private final int arity;
   private final Set<CommandFlag> flags;
   private final Set<String> unknownFlags;
   private final int firstKeyPosition;
   private final int lastKeyPosition;
   private final int stepCountForRepeatingKeys;
   private final List<Response> unknownAttributes;
   
   CommandInfo(Builder builder) {
      this.name = builder.name;
      this.arity = builder.arity;
      this.flags = Collections.unmodifiableSet(builder.flags);
      this.unknownFlags = Collections.unmodifiableSet(builder.unknownFlags);
      this.firstKeyPosition = builder.firstKeyPosition;
      this.lastKeyPosition = builder.lastKeyPosition;
      this.stepCountForRepeatingKeys = builder.stepCountForRepeatingKeys;
      this.unknownAttributes = Collections.unmodifiableList(builder.unknownAttributes);
   }
   
   public String name() {
      return name;
   }
   
   public int arity() {
      return arity;
   }
   
   public Set<CommandFlag> flags() {
      return flags;
   }
   
   public Set<String> unknownFlags() {
      return unknownFlags;
   }
   
   public int firstKeyPosition() {
      return firstKeyPosition;
   }
   
   public int lastKeyPosition() {
      return lastKeyPosition;
   }
   
   public int stepCountForRepeatingKeys() {
      return stepCountForRepeatingKeys;
   }
   
   public List<Response> unknownAttributes() {
      return unknownAttributes;
   }
   
   // TODO: hashCode, equals, etc

   public static final class Builder {
      private String name;
      private int arity;
      private EnumSet<CommandFlag> flags = EnumSet.noneOf(CommandFlag.class);
      private LinkedHashSet<String> unknownFlags = new LinkedHashSet<>();
      private int firstKeyPosition;
      private int lastKeyPosition;
      private int stepCountForRepeatingKeys;
      private ArrayList<Response> unknownAttributes = new ArrayList<>();
      private boolean resetOnWrite;
      
      public Builder name(String name) {
         this.name = name;
         return this;
      }
      
      public Builder arity(int arity) {
         this.arity = arity;
         return this;
      }

      public Builder flag(String flag) {
         CommandFlag f = CommandFlag.lookup(flag);
         if (f == null) {
            return unknownFlag(flag);
         } else {
            return flag(f);
         }
      }

      public Builder flag(CommandFlag flag) {
         maybeReset();
         this.flags.add(requireNonNull(flag));
         return this;
      }

      public Builder unknownFlag(String flag) {
         maybeReset();
         this.unknownFlags.add(requireNonNull(flag));
         return this;
      }
      
      public Builder firstKeyPosition(int pos) {
         this.firstKeyPosition = pos;
         return this;
      }

      public Builder lastKeyPosition(int pos) {
         this.lastKeyPosition = pos;
         return this;
      }

      public Builder stepCountForRepeatingKeys(int step) {
         this.stepCountForRepeatingKeys = step;
         return this;
      }
      
      public Builder unknownAttribute(Response resp) {
         maybeReset();
         this.unknownAttributes.add(requireNonNull(resp));
         return this;
      }

      public CommandInfo build() {
         resetOnWrite = true;
         return new CommandInfo(this);
      }
      
      @SuppressWarnings("unchecked")
      private void maybeReset() {
         if (resetOnWrite) {
            // we do this lazily to avoid unnecessary allocations/copying while still protecting
            // built objects from subsequent modifications in the builder
            this.flags = this.flags.clone();
            this.unknownFlags = (LinkedHashSet<String>) unknownFlags.clone();
            this.unknownAttributes = (ArrayList<Response>) unknownAttributes.clone();
            resetOnWrite = false;
         }
      }
   }
}
